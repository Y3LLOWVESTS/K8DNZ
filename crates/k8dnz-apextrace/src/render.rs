use crate::dibit::bytes_to_quats;
use crate::error::{ApexError, Result};
use crate::generator::generate_quats;
use crate::key::ApexKey;
use crate::law::{emit_quat, trace_leaf, BRANCH_LEFT, BRANCH_RIGHT, NodeState};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LatticePoint {
    pub row: u16,
    pub k: u16,
    pub x: i32,
    pub y: i32,
    pub visits: u64,
    pub leaf_span: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PathPoint {
    pub leaf: u64,
    pub step: u16,
    pub row: u16,
    pub k: u16,
    pub x: i32,
    pub y: i32,
    pub q: u8,
    pub u: u64,
    pub branch: u8,
    pub quat: Option<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BaseLabel {
    pub leaf: u64,
    pub row: u16,
    pub k: u16,
    pub x: i32,
    pub y: i32,
    pub quat: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubtreeStats {
    pub row: u16,
    pub k: u16,
    pub x: i32,
    pub y: i32,
    pub subtree_size: u64,
    pub leaf_range_start: u64,
    pub leaf_range_end: u64,
    pub target_hist: [u64; 4],
    pub pred_hist: [u64; 4],
    pub matches: u64,
    pub mismatches: u64,
}

impl SubtreeStats {
    pub fn active(&self) -> bool {
        self.subtree_size > 0
    }

    pub fn match_rate_ppm(&self) -> u64 {
        scaled_ppm(self.matches, self.subtree_size)
    }

    pub fn match_excess_ppm(&self) -> i64 {
        self.match_rate_ppm() as i64 - 250_000
    }

    pub fn target_purity_ppm(&self) -> u64 {
        scaled_ppm(hist_max(&self.target_hist), self.subtree_size)
    }

    pub fn pred_purity_ppm(&self) -> u64 {
        scaled_ppm(hist_max(&self.pred_hist), self.subtree_size)
    }

    pub fn target_entropy_bits(&self) -> f64 {
        hist_entropy_bits(&self.target_hist, self.subtree_size)
    }

    pub fn pred_entropy_bits(&self) -> f64 {
        hist_entropy_bits(&self.pred_hist, self.subtree_size)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct SubtreeAccum {
    subtree_size: u64,
    leaf_range_start: Option<u64>,
    leaf_range_end: Option<u64>,
    target_hist: [u64; 4],
    pred_hist: [u64; 4],
    matches: u64,
    mismatches: u64,
}

pub fn point_xy(row: u16, k: u16) -> (i32, i32) {
    let row_i = i32::from(row);
    let k_i = i32::from(k);
    ((2 * k_i) - row_i, row_i)
}

pub fn render_lattice(key: &ApexKey, max_quats: Option<u64>) -> Result<Vec<LatticePoint>> {
    key.validate()?;

    let limit = max_quats.unwrap_or(key.quat_len).min(key.quat_len);
    let mut visits: Vec<Vec<u64>> = (0..=key.depth)
        .map(|row| vec![0u64; usize::from(row) + 1])
        .collect();

    let mut leaf = 0u64;
    while leaf < limit {
        let trace = trace_leaf(key.root_quadrant, key.root_seed, key.depth, leaf, key.recipe_seed);
        for node in trace {
            visits[usize::from(node.row)][usize::from(node.k)] += 1;
        }
        leaf += 1;
    }

    let mut out = Vec::new();
    let mut row = 0u16;
    while row <= key.depth {
        let mut k = 0u16;
        while k <= row {
            let (x, y) = point_xy(row, k);
            let shift = u32::from(key.depth.saturating_sub(row));
            let leaf_span = 1u64.checked_shl(shift).unwrap_or(0);
            out.push(LatticePoint {
                row,
                k,
                x,
                y,
                visits: visits[usize::from(row)][usize::from(k)],
                leaf_span,
            });
            k = k.saturating_add(1);
        }

        if row == u16::MAX {
            break;
        }
        row = row.saturating_add(1);
    }

    Ok(out)
}

pub fn render_paths(key: &ApexKey, max_quats: Option<u64>) -> Result<(Vec<PathPoint>, Vec<BaseLabel>)> {
    key.validate()?;

    let limit = max_quats.unwrap_or(key.quat_len).min(key.quat_len);
    let mut points = Vec::with_capacity(limit as usize * (usize::from(key.depth) + 1));
    let mut labels = Vec::with_capacity(limit as usize);

    let mut leaf = 0u64;
    while leaf < limit {
        let trace = trace_leaf(key.root_quadrant, key.root_seed, key.depth, leaf, key.recipe_seed);
        let last = trace.last().copied().expect("trace always contains root");
        let quat = emit_quat(NodeState { q: last.q, u: last.u });

        for node in trace {
            let (x, y) = point_xy(node.row, node.k);
            let node_quat = if node.row == key.depth { Some(quat) } else { None };
            points.push(PathPoint {
                leaf,
                step: node.step,
                row: node.row,
                k: node.k,
                x,
                y,
                q: node.q,
                u: node.u,
                branch: node.branch,
                quat: node_quat,
            });
        }

        let (x, y) = point_xy(last.row, last.k);
        labels.push(BaseLabel {
            leaf,
            row: last.row,
            k: last.k,
            x,
            y,
            quat,
        });

        leaf += 1;
    }

    Ok((points, labels))
}

pub fn render_subtree_stats(
    key: &ApexKey,
    target_bytes: &[u8],
    max_quats: Option<u64>,
) -> Result<Vec<SubtreeStats>> {
    key.validate()?;

    let target_quats = bytes_to_quats(target_bytes)?;
    let pred_quats = generate_quats(key)?;

    if target_quats.len() != pred_quats.len() {
        return Err(ApexError::Validation(format!(
            "target quat len {} != predicted quat len {}",
            target_quats.len(),
            pred_quats.len()
        )));
    }

    let limit = max_quats
        .unwrap_or(key.quat_len)
        .min(key.quat_len)
        .min(target_quats.len() as u64);

    let mut acc: Vec<Vec<SubtreeAccum>> = (0..=key.depth)
        .map(|row| vec![SubtreeAccum::default(); usize::from(row) + 1])
        .collect();

    let mut leaf = 0u64;
    while leaf < limit {
        let trace = trace_leaf(key.root_quadrant, key.root_seed, key.depth, leaf, key.recipe_seed);
        let target = target_quats[leaf as usize];
        let pred = pred_quats[leaf as usize];
        let t_slot = quat_slot(target)?;
        let p_slot = quat_slot(pred)?;
        let matched = target == pred;

        for node in trace {
            let a = &mut acc[usize::from(node.row)][usize::from(node.k)];
            a.subtree_size += 1;
            if a.leaf_range_start.is_none() {
                a.leaf_range_start = Some(leaf);
            }
            a.leaf_range_end = Some(leaf);
            a.target_hist[t_slot] += 1;
            a.pred_hist[p_slot] += 1;
            if matched {
                a.matches += 1;
            } else {
                a.mismatches += 1;
            }
        }

        leaf += 1;
    }

    let mut out = Vec::new();
    let mut row = 0u16;
    while row <= key.depth {
        let mut k = 0u16;
        while k <= row {
            let (x, y) = point_xy(row, k);
            let a = &acc[usize::from(row)][usize::from(k)];
            out.push(SubtreeStats {
                row,
                k,
                x,
                y,
                subtree_size: a.subtree_size,
                leaf_range_start: a.leaf_range_start.unwrap_or(0),
                leaf_range_end: a.leaf_range_end.unwrap_or(0),
                target_hist: a.target_hist,
                pred_hist: a.pred_hist,
                matches: a.matches,
                mismatches: a.mismatches,
            });
            k = k.saturating_add(1);
        }

        if row == u16::MAX {
            break;
        }
        row = row.saturating_add(1);
    }

    Ok(out)
}

pub fn branch_name(branch: u8) -> &'static str {
    match branch {
        BRANCH_LEFT => "L",
        BRANCH_RIGHT => "R",
        _ => "ROOT",
    }
}

fn quat_slot(v: u8) -> Result<usize> {
    match v {
        1..=4 => Ok((v - 1) as usize),
        _ => Err(ApexError::Validation(format!(
            "invalid quaternary symbol {v}; expected 1..=4"
        ))),
    }
}

fn scaled_ppm(num: u64, den: u64) -> u64 {
    if den == 0 {
        return 0;
    }
    num.saturating_mul(1_000_000) / den
}

fn hist_max(hist: &[u64; 4]) -> u64 {
    hist.iter().copied().max().unwrap_or(0)
}

fn hist_entropy_bits(hist: &[u64; 4], total: u64) -> f64 {
    if total == 0 {
        return 0.0;
    }

    let mut entropy = 0.0;
    for &count in hist {
        if count == 0 {
            continue;
        }
        let p = (count as f64) / (total as f64);
        entropy -= p * p.log2();
    }
    entropy
}
