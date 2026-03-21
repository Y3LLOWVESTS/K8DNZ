use crate::error::{ApexError, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ApexMapCfg {
    pub class_count: u8,
    pub max_depth: u8,
    pub depth_shift: u8,
}

impl Default for ApexMapCfg {
    fn default() -> Self {
        Self {
            class_count: 4,
            max_depth: 0,
            depth_shift: 1,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ApexMapNode {
    pub start: usize,
    pub end: usize,
    pub depth: u8,
    pub hist: [u64; 4],
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BoundaryPair {
    pub left: u8,
    pub right: u8,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RefineStats {
    pub overrides: usize,
    pub boundary_count: usize,
    pub touched_positions: usize,
    pub applied_by_desired: [usize; 4],
    pub blocked_by_budget: [usize; 4],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RefineCfg {
    pub band: usize,
    pub delta: usize,
    pub base_margin: u64,
    pub desired_margin_add: [u64; 4],
    pub transition_margin_add: [[u64; 4]; 4],
    pub dominant_share_ppm_min: [u32; 4],
    pub desired_from_mask: [u8; 4],
    pub desired_apply_budget: [usize; 4],
}

impl Default for RefineCfg {
    fn default() -> Self {
        Self {
            band: 16,
            delta: 1,
            base_margin: 8,
            desired_margin_add: [0; 4],
            transition_margin_add: [[0; 4]; 4],
            dominant_share_ppm_min: [0; 4],
            desired_from_mask: [0x0F; 4],
            desired_apply_budget: [0; 4],
        }
    }
}

impl RefineCfg {
    pub fn new(band: usize, delta: usize, base_margin: u64) -> Self {
        Self {
            band,
            delta,
            base_margin,
            ..Self::default()
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OverrideDecision {
    Applied,
    SameClass,
    ClassOutOfRange,
    DisallowedFromMask,
    ShareBelowFloor,
    MarginTooSmall,
    BudgetExceeded,
}

impl OverrideDecision {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::SameClass => "same-class",
            Self::ClassOutOfRange => "class-out-of-range",
            Self::DisallowedFromMask => "disallowed-from-mask",
            Self::ShareBelowFloor => "share-below-floor",
            Self::MarginTooSmall => "margin-too-small",
            Self::BudgetExceeded => "budget-exceeded",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OverrideTrace {
    pub pos: usize,
    pub current: u8,
    pub desired: u8,
    pub allow_mask: u8,
    pub desired_score: u64,
    pub current_score: u64,
    pub needed_margin: u64,
    pub share_ppm: u32,
    pub share_floor: u32,
    pub decision: OverrideDecision,
}

impl OverrideTrace {
    pub fn applied(self) -> bool {
        self.decision == OverrideDecision::Applied
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApexMap {
    len: usize,
    class_count: u8,
    cfg: ApexMapCfg,
    nodes: Vec<ApexMapNode>,
}

impl ApexMap {
    pub fn from_symbols(symbols: &[u8], cfg: ApexMapCfg) -> Result<Self> {
        if cfg.class_count == 0 || cfg.class_count > 4 {
            return Err(ApexError::Validation(
                "apex-map class_count must be in 1..=4".into(),
            ));
        }
        for &sym in symbols {
            if sym >= cfg.class_count {
                return Err(ApexError::Validation(
                    "apex-map symbol exceeds class_count".into(),
                ));
            }
        }

        let mut nodes = Vec::new();
        Self::build_nodes(&mut nodes, symbols, 0, symbols.len(), 0, cfg);

        Ok(Self {
            len: symbols.len(),
            class_count: cfg.class_count,
            cfg,
            nodes,
        })
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn class_count(&self) -> u8 {
        self.class_count
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn max_depth_seen(&self) -> u8 {
        self.nodes.iter().map(|node| node.depth).max().unwrap_or(0)
    }

    pub fn nodes(&self) -> &[ApexMapNode] {
        &self.nodes
    }

    pub fn score_at(&self, pos: usize) -> Result<[u64; 4]> {
        if pos >= self.len {
            return Err(ApexError::Validation(
                "apex-map score_at position out of range".into(),
            ));
        }

        let mut scores = [0u64; 4];
        let x2 = (pos as i64) * 2 + 1;

        for node in &self.nodes {
            if pos < node.start || pos >= node.end {
                continue;
            }

            let span = node.end.saturating_sub(node.start);
            if span == 0 {
                continue;
            }

            let center2 = (node.start + node.end) as i64;
            let dist2 = (x2 - center2).unsigned_abs() as usize;
            let half_width2 = span;
            let raw = half_width2.saturating_sub(dist2).max(1) as u64;
            let shift = (node.depth as u32).saturating_mul(self.cfg.depth_shift as u32);
            let weight = if shift >= 63 { 1 } else { (raw >> shift).max(1) };

            for cls in 0..self.class_count as usize {
                scores[cls] = scores[cls].saturating_add(node.hist[cls].saturating_mul(weight));
            }
        }

        Ok(scores)
    }

    pub fn dominant_at(&self, pos: usize) -> Result<u8> {
        let (label, _) = self.dominant_and_margin_at(pos)?;
        Ok(label)
    }

    pub fn dominant_and_margin_at(&self, pos: usize) -> Result<(u8, u64)> {
        let scores = self.score_at(pos)?;
        let mut best_idx = 0usize;
        let mut best_score = 0u64;
        let mut second_score = 0u64;

        for cls in 0..self.class_count as usize {
            let s = scores[cls];
            if s > best_score {
                second_score = best_score;
                best_score = s;
                best_idx = cls;
            } else if s > second_score {
                second_score = s;
            }
        }

        Ok((best_idx as u8, best_score.saturating_sub(second_score)))
    }

    pub fn class_share_ppm_at(&self, pos: usize, class: u8) -> Result<u32> {
        if class >= self.class_count {
            return Err(ApexError::Validation(
                "apex-map class_share_ppm_at class out of range".into(),
            ));
        }

        let scores = self.score_at(pos)?;
        Ok(Self::score_share_ppm(&scores, class))
    }

    pub fn boundary_pair(&self, boundary: usize, delta: usize) -> Result<BoundaryPair> {
        if boundary == 0 || boundary >= self.len {
            return Err(ApexError::Validation(
                "apex-map boundary must be in 1..len-1".into(),
            ));
        }

        let delta = delta.max(1);
        let left_pos = boundary.saturating_sub(delta).min(self.len - 1);
        let right_pos = boundary
            .saturating_add(delta.saturating_sub(1))
            .min(self.len - 1);

        Ok(BoundaryPair {
            left: self.dominant_at(left_pos)?,
            right: self.dominant_at(right_pos)?,
        })
    }

    pub fn refine_boundaries(
        &self,
        base: &[u8],
        boundaries: &[usize],
        cfg: RefineCfg,
    ) -> Result<(Vec<u8>, RefineStats)> {
        if base.len() != self.len {
            return Err(ApexError::Validation(
                "apex-map base length mismatch".into(),
            ));
        }

        let mut out = base.to_vec();
        let mut stats = RefineStats::default();
        let mut applied_counts = [0usize; 4];

        for &boundary in boundaries {
            if boundary == 0 || boundary >= self.len {
                continue;
            }

            let pair = self.boundary_pair(boundary, cfg.delta)?;
            stats.boundary_count = stats.boundary_count.saturating_add(1);

            let left_from = boundary.saturating_sub(cfg.band);
            for pos in left_from..boundary {
                stats.touched_positions = stats.touched_positions.saturating_add(1);
                let trace = self.evaluate_override_with_budget(pos, out[pos], pair.left, cfg, &applied_counts)?;
                if trace.applied() {
                    out[pos] = pair.left;
                    stats.overrides = stats.overrides.saturating_add(1);
                    let slot = pair.left as usize;
                    applied_counts[slot] = applied_counts[slot].saturating_add(1);
                    stats.applied_by_desired[slot] = stats.applied_by_desired[slot].saturating_add(1);
                } else if trace.decision == OverrideDecision::BudgetExceeded {
                    let slot = pair.left as usize;
                    stats.blocked_by_budget[slot] = stats.blocked_by_budget[slot].saturating_add(1);
                }
            }

            let right_to = boundary.saturating_add(cfg.band).min(self.len);
            for pos in boundary..right_to {
                stats.touched_positions = stats.touched_positions.saturating_add(1);
                let trace = self.evaluate_override_with_budget(pos, out[pos], pair.right, cfg, &applied_counts)?;
                if trace.applied() {
                    out[pos] = pair.right;
                    stats.overrides = stats.overrides.saturating_add(1);
                    let slot = pair.right as usize;
                    applied_counts[slot] = applied_counts[slot].saturating_add(1);
                    stats.applied_by_desired[slot] = stats.applied_by_desired[slot].saturating_add(1);
                } else if trace.decision == OverrideDecision::BudgetExceeded {
                    let slot = pair.right as usize;
                    stats.blocked_by_budget[slot] = stats.blocked_by_budget[slot].saturating_add(1);
                }
            }
        }

        Ok((out, stats))
    }

    pub fn evaluate_override(
        &self,
        pos: usize,
        current: u8,
        desired: u8,
        cfg: RefineCfg,
    ) -> Result<OverrideTrace> {
        self.evaluate_override_with_budget(pos, current, desired, cfg, &[0usize; 4])
    }

    pub fn evaluate_override_with_budget(
        &self,
        pos: usize,
        current: u8,
        desired: u8,
        cfg: RefineCfg,
        applied_counts: &[usize; 4],
    ) -> Result<OverrideTrace> {
        let allow_mask = if (desired as usize) < cfg.desired_from_mask.len() {
            cfg.desired_from_mask[desired as usize]
        } else {
            0
        };

        let desired_margin_add = if (desired as usize) < cfg.desired_margin_add.len() {
            cfg.desired_margin_add[desired as usize]
        } else {
            0
        };
        let transition_margin_add = if (current as usize) < cfg.transition_margin_add.len()
            && (desired as usize) < cfg.transition_margin_add[current as usize].len()
        {
            cfg.transition_margin_add[current as usize][desired as usize]
        } else {
            0
        };
        let needed_margin = cfg
            .base_margin
            .saturating_add(desired_margin_add)
            .saturating_add(transition_margin_add);

        let share_floor = if (desired as usize) < cfg.dominant_share_ppm_min.len() {
            cfg.dominant_share_ppm_min[desired as usize]
        } else {
            0
        };

        if current == desired {
            return Ok(OverrideTrace {
                pos,
                current,
                desired,
                allow_mask,
                desired_score: 0,
                current_score: 0,
                needed_margin,
                share_ppm: 0,
                share_floor,
                decision: OverrideDecision::SameClass,
            });
        }

        if current >= self.class_count || desired >= self.class_count {
            return Ok(OverrideTrace {
                pos,
                current,
                desired,
                allow_mask,
                desired_score: 0,
                current_score: 0,
                needed_margin,
                share_ppm: 0,
                share_floor,
                decision: OverrideDecision::ClassOutOfRange,
            });
        }

        if ((allow_mask >> current) & 1) == 0 {
            return Ok(OverrideTrace {
                pos,
                current,
                desired,
                allow_mask,
                desired_score: 0,
                current_score: 0,
                needed_margin,
                share_ppm: 0,
                share_floor,
                decision: OverrideDecision::DisallowedFromMask,
            });
        }

        let scores = self.score_at(pos)?;
        let desired_score = scores[desired as usize];
        let current_score = scores[current as usize];

        let share_ppm = Self::score_share_ppm(&scores, desired);
        if share_ppm < share_floor {
            return Ok(OverrideTrace {
                pos,
                current,
                desired,
                allow_mask,
                desired_score,
                current_score,
                needed_margin,
                share_ppm,
                share_floor,
                decision: OverrideDecision::ShareBelowFloor,
            });
        }

        let decision = if desired_score >= current_score.saturating_add(needed_margin) {
            let budget = if (desired as usize) < cfg.desired_apply_budget.len() {
                cfg.desired_apply_budget[desired as usize]
            } else {
                0
            };
            if budget != 0 && applied_counts[desired as usize] >= budget {
                OverrideDecision::BudgetExceeded
            } else {
                OverrideDecision::Applied
            }
        } else {
            OverrideDecision::MarginTooSmall
        };

        Ok(OverrideTrace {
            pos,
            current,
            desired,
            allow_mask,
            desired_score,
            current_score,
            needed_margin,
            share_ppm,
            share_floor,
            decision,
        })
    }


    fn score_share_ppm(scores: &[u64; 4], desired: u8) -> u32 {
        let total: u128 = scores.iter().map(|&v| v as u128).sum();
        if total == 0 {
            return 0;
        }
        let desired_score = scores[desired as usize] as u128;
        ((desired_score.saturating_mul(1_000_000u128)) / total) as u32
    }

    fn build_nodes(
        nodes: &mut Vec<ApexMapNode>,
        symbols: &[u8],
        start: usize,
        end: usize,
        depth: u8,
        cfg: ApexMapCfg,
    ) {
        if start >= end {
            return;
        }

        let mut hist = [0u64; 4];
        for &sym in &symbols[start..end] {
            hist[sym as usize] = hist[sym as usize].saturating_add(1);
        }

        nodes.push(ApexMapNode {
            start,
            end,
            depth,
            hist,
        });

        let stop_for_depth = cfg.max_depth != 0 && depth >= cfg.max_depth;
        if stop_for_depth || end.saturating_sub(start) <= 1 {
            return;
        }

        let mid = start + (end - start) / 2;
        if mid == start || mid == end {
            return;
        }

        Self::build_nodes(nodes, symbols, start, mid, depth.saturating_add(1), cfg);
        Self::build_nodes(nodes, symbols, mid, end, depth.saturating_add(1), cfg);
    }
}