use super::types::{
    BodySelectObjective, ClosureShapeMetrics, OverrideCandidateRef, OverridePathMode,
    OverridePathPlan, OverrideSubsetScore, SelectedFilePlan,
};

const CLOSURE_PER_OVERRIDE_EXACT: usize = 48;
const CLOSURE_PER_RUN_EXACT: usize = 96;
const CLOSURE_PER_PATH_BYTE_EXACT: usize = 2;
const MAX_EXACT_SUBSET_COUNT: u64 = 1 << 15;

pub(crate) fn select_override_plan(
    candidates: &[OverrideCandidateRef],
    exact_subset_limit: usize,
    objective: BodySelectObjective,
    target_window_count: usize,
) -> SelectedFilePlan {
    let filtered = candidates
        .iter()
        .copied()
        .filter(|row| row.gain_exact > 0)
        .collect::<Vec<_>>();

    if filtered.is_empty() {
        return SelectedFilePlan {
            mode: OverridePathMode::None,
            path_bytes_exact: 0,
            selected_window_ordinals: Vec::new(),
            closure_shape: compute_closure_shape_metrics(
                OverridePathMode::None,
                0,
                &[],
                target_window_count,
            ),
        };
    }

    if objective == BodySelectObjective::SelectedTarget {
        return select_override_plan_selected_target(&filtered, target_window_count);
    }

    let subset_count = exact_subset_count(filtered.len());
    if filtered.len() <= exact_subset_limit && subset_count <= MAX_EXACT_SUBSET_COUNT {
        return select_override_plan_exact(&filtered, subset_count, objective, target_window_count);
    }

    select_override_plan_greedy(&filtered, objective, target_window_count)
}

pub(crate) fn compute_closure_shape_metrics(
    mode: OverridePathMode,
    path_bytes_exact: usize,
    ordinals: &[usize],
    target_window_count: usize,
) -> ClosureShapeMetrics {
    let mut normalized = ordinals.to_vec();
    normalized.sort_unstable();
    normalized.dedup();

    let runs = ordinal_runs(&normalized);
    let override_count = normalized.len();
    let override_run_count = runs.len();
    let max_override_run_length = runs.iter().map(|(_, len)| *len).max().unwrap_or(0);
    let untouched_window_count = target_window_count.saturating_sub(override_count);

    let override_density_ppm = if target_window_count == 0 {
        0
    } else {
        scaled_ppm(override_count, target_window_count)
    };

    let untouched_window_pct_ppm = if target_window_count == 0 {
        1_000_000
    } else {
        scaled_ppm(untouched_window_count, target_window_count)
    };

    let mode_penalty_exact = match mode {
        OverridePathMode::None => 0,
        OverridePathMode::Delta => 0,
        OverridePathMode::Runs => 0,
        OverridePathMode::Ordinals => 64,
    };

    let closure_penalty_exact = override_count
        .saturating_mul(CLOSURE_PER_OVERRIDE_EXACT)
        .saturating_add(override_run_count.saturating_mul(CLOSURE_PER_RUN_EXACT))
        .saturating_add(path_bytes_exact.saturating_mul(CLOSURE_PER_PATH_BYTE_EXACT))
        .saturating_add(mode_penalty_exact);

    ClosureShapeMetrics {
        override_count,
        override_run_count,
        max_override_run_length,
        untouched_window_count,
        override_density_ppm,
        untouched_window_pct_ppm,
        mode_penalty_exact,
        closure_penalty_exact,
    }
}

fn select_override_plan_selected_target(
    candidates: &[OverrideCandidateRef],
    target_window_count: usize,
) -> SelectedFilePlan {
    let ordinals = candidates
        .iter()
        .map(|row| row.target_ordinal)
        .collect::<Vec<_>>();
    let plan = choose_best_override_path_plan(&ordinals);
    let closure_shape =
        compute_closure_shape_metrics(plan.mode, plan.bytes, &plan.ordinals, target_window_count);

    SelectedFilePlan {
        mode: plan.mode,
        path_bytes_exact: plan.bytes,
        selected_window_ordinals: plan.ordinals,
        closure_shape,
    }
}

fn select_override_plan_exact(
    candidates: &[OverrideCandidateRef],
    subset_count: u64,
    objective: BodySelectObjective,
    target_window_count: usize,
) -> SelectedFilePlan {
    let mut best = score_override_subset(&[], target_window_count);

    for mask in 1..subset_count {
        let mut subset = Vec::new();
        for (bit, row) in candidates.iter().enumerate() {
            if (mask & (1u64 << bit)) != 0 {
                subset.push(*row);
            }
        }

        let score = score_override_subset(&subset, target_window_count);
        if is_better_override_score(&score, &best, objective) {
            best = score;
        }
    }

    SelectedFilePlan {
        mode: best.plan.mode,
        path_bytes_exact: best.plan.bytes,
        selected_window_ordinals: best.plan.ordinals,
        closure_shape: best.closure_shape,
    }
}

fn select_override_plan_greedy(
    candidates: &[OverrideCandidateRef],
    objective: BodySelectObjective,
    target_window_count: usize,
) -> SelectedFilePlan {
    let mut remaining = candidates.to_vec();
    remaining.sort_by_key(|row| row.target_ordinal);

    let mut selected = Vec::<OverrideCandidateRef>::new();
    let mut best = score_override_subset(&selected, target_window_count);

    loop {
        let mut best_next = best.clone();
        let mut best_idx = None;

        for (idx, candidate) in remaining.iter().enumerate() {
            let mut trial = selected.clone();
            trial.push(*candidate);
            let score = score_override_subset(&trial, target_window_count);
            if is_better_override_score(&score, &best_next, objective) {
                best_next = score;
                best_idx = Some(idx);
            }
        }

        match best_idx {
            Some(idx) => {
                selected.push(remaining.remove(idx));
                best = best_next;
            }
            None => break,
        }
    }

    SelectedFilePlan {
        mode: best.plan.mode,
        path_bytes_exact: best.plan.bytes,
        selected_window_ordinals: best.plan.ordinals,
        closure_shape: best.closure_shape,
    }
}

fn score_override_subset(
    subset: &[OverrideCandidateRef],
    target_window_count: usize,
) -> OverrideSubsetScore {
    let total_gain_exact = subset.iter().map(|row| row.gain_exact as i64).sum::<i64>();
    let ordinals = subset.iter().map(|row| row.target_ordinal).collect::<Vec<_>>();
    let plan = choose_best_override_path_plan(&ordinals);
    let closure_shape =
        compute_closure_shape_metrics(plan.mode, plan.bytes, &plan.ordinals, target_window_count);

    let codec_net_total_delta_exact = plan.bytes as i64 - total_gain_exact;
    let closure_net_total_delta_exact =
        codec_net_total_delta_exact + closure_shape.closure_penalty_exact as i64;

    OverrideSubsetScore {
        codec_net_total_delta_exact,
        closure_net_total_delta_exact,
        path_bytes_exact: plan.bytes,
        selected_count: plan.ordinals.len(),
        run_count: closure_shape.override_run_count,
        max_run_length: closure_shape.max_override_run_length,
        mode_rank: plan.mode.tie_rank(),
        ordinals: plan.ordinals.clone(),
        plan,
        closure_shape,
    }
}

fn is_better_override_score(
    a: &OverrideSubsetScore,
    b: &OverrideSubsetScore,
    objective: BodySelectObjective,
) -> bool {
    match objective {
        BodySelectObjective::SelectedTotal => (
            a.codec_net_total_delta_exact,
            a.path_bytes_exact,
            a.selected_count,
            a.run_count,
            a.mode_rank,
            a.ordinals.clone(),
        ) < (
            b.codec_net_total_delta_exact,
            b.path_bytes_exact,
            b.selected_count,
            b.run_count,
            b.mode_rank,
            b.ordinals.clone(),
        ),
        BodySelectObjective::ClosureTotal => (
            a.closure_net_total_delta_exact,
            a.codec_net_total_delta_exact,
            a.selected_count,
            a.run_count,
            a.path_bytes_exact,
            a.mode_rank,
            a.ordinals.clone(),
        ) < (
            b.closure_net_total_delta_exact,
            b.codec_net_total_delta_exact,
            b.selected_count,
            b.run_count,
            b.path_bytes_exact,
            b.mode_rank,
            b.ordinals.clone(),
        ),
        BodySelectObjective::SelectedTarget => unreachable!("selected-target uses direct selection"),
    }
}

pub(crate) fn choose_best_override_path_plan(ordinals: &[usize]) -> OverridePathPlan {
    if ordinals.is_empty() {
        return OverridePathPlan {
            mode: OverridePathMode::None,
            bytes: 0,
            ordinals: Vec::new(),
        };
    }

    let mut normalized = ordinals.to_vec();
    normalized.sort_unstable();
    normalized.dedup();

    [
        OverridePathPlan {
            mode: OverridePathMode::Delta,
            bytes: override_path_bytes_delta(&normalized),
            ordinals: normalized.clone(),
        },
        OverridePathPlan {
            mode: OverridePathMode::Runs,
            bytes: override_path_bytes_runs(&normalized),
            ordinals: normalized.clone(),
        },
        OverridePathPlan {
            mode: OverridePathMode::Ordinals,
            bytes: override_path_bytes_ordinals(&normalized),
            ordinals: normalized.clone(),
        },
    ]
    .into_iter()
    .min_by_key(|plan| (plan.bytes, plan.mode.tie_rank(), plan.ordinals.clone()))
    .expect("override path plans should not be empty")
}

fn override_path_bytes_ordinals(ordinals: &[usize]) -> usize {
    1 + varint_len(ordinals.len() as u64)
        + ordinals
            .iter()
            .map(|ordinal| varint_len(*ordinal as u64))
            .sum::<usize>()
}

fn override_path_bytes_delta(ordinals: &[usize]) -> usize {
    let mut bytes = 1 + varint_len(ordinals.len() as u64);
    let mut prev = 0usize;

    for (idx, ordinal) in ordinals.iter().enumerate() {
        let delta = if idx == 0 {
            *ordinal
        } else {
            ordinal.saturating_sub(prev)
        };
        bytes += varint_len(delta as u64);
        prev = *ordinal;
    }

    bytes
}

fn override_path_bytes_runs(ordinals: &[usize]) -> usize {
    let runs = ordinal_runs(ordinals);
    1 + varint_len(runs.len() as u64)
        + runs
            .iter()
            .map(|(start, len)| varint_len(*start as u64) + varint_len(*len as u64))
            .sum::<usize>()
}

fn ordinal_runs(ordinals: &[usize]) -> Vec<(usize, usize)> {
    if ordinals.is_empty() {
        return Vec::new();
    }

    let mut runs = Vec::new();
    let mut start = ordinals[0];
    let mut prev = ordinals[0];
    let mut len = 1usize;

    for ordinal in ordinals.iter().copied().skip(1) {
        if ordinal == prev + 1 {
            len += 1;
        } else {
            runs.push((start, len));
            start = ordinal;
            len = 1;
        }
        prev = ordinal;
    }

    runs.push((start, len));
    runs
}

fn exact_subset_count(len: usize) -> u64 {
    if len >= 63 {
        u64::MAX
    } else {
        1u64 << len
    }
}

fn scaled_ppm(num: usize, den: usize) -> u32 {
    if den == 0 {
        return 0;
    }
    (((num as u128) * 1_000_000u128) / den as u128) as u32
}

fn varint_len(mut value: u64) -> usize {
    let mut bytes = 1usize;
    while value >= 0x80 {
        value >>= 7;
        bytes += 1;
    }
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn choose_best_override_path_plan_prefers_runs_for_dense_sequence() {
        let plan = choose_best_override_path_plan(&[4, 5, 6, 7]);
        assert_eq!(plan.mode, OverridePathMode::Runs);
    }

    #[test]
    fn select_override_plan_skips_unprofitable_singleton_codec_total() {
        let candidates = vec![OverrideCandidateRef {
            window_idx: 0,
            target_ordinal: 11,
            gain_exact: 1,
        }];
        let plan = select_override_plan(
            &candidates,
            20,
            BodySelectObjective::SelectedTotal,
            12,
        );
        assert_eq!(plan.mode, OverridePathMode::None);
        assert_eq!(plan.path_bytes_exact, 0);
        assert!(plan.selected_window_ordinals.is_empty());
    }

    #[test]
    fn selected_target_takes_all_positive_gain_candidates() {
        let candidates = vec![
            OverrideCandidateRef {
                window_idx: 0,
                target_ordinal: 3,
                gain_exact: 2,
            },
            OverrideCandidateRef {
                window_idx: 1,
                target_ordinal: 4,
                gain_exact: 3,
            },
        ];

        let plan = select_override_plan(
            &candidates,
            20,
            BodySelectObjective::SelectedTarget,
            8,
        );

        assert_eq!(plan.selected_window_ordinals, vec![3, 4]);
        assert_eq!(plan.path_bytes_exact, 4);
        assert_eq!(plan.closure_shape.override_count, 2);
        assert_eq!(plan.closure_shape.override_run_count, 1);
        assert_eq!(plan.closure_shape.max_override_run_length, 2);
    }

    #[test]
    fn closure_total_rejects_dense_low_gain_run() {
        let candidates = vec![
            OverrideCandidateRef {
                window_idx: 0,
                target_ordinal: 4,
                gain_exact: 20,
            },
            OverrideCandidateRef {
                window_idx: 1,
                target_ordinal: 5,
                gain_exact: 20,
            },
            OverrideCandidateRef {
                window_idx: 2,
                target_ordinal: 6,
                gain_exact: 20,
            },
            OverrideCandidateRef {
                window_idx: 3,
                target_ordinal: 7,
                gain_exact: 20,
            },
        ];

        let plan = select_override_plan(
            &candidates,
            20,
            BodySelectObjective::ClosureTotal,
            12,
        );

        assert_eq!(plan.mode, OverridePathMode::None);
        assert!(plan.selected_window_ordinals.is_empty());
    }

    #[test]
    fn exact_subset_count_caps_large_candidate_sets() {
        assert_eq!(exact_subset_count(15), 1 << 15);
        assert_eq!(exact_subset_count(63), u64::MAX);
    }
}