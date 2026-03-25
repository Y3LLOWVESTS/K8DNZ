use std::collections::BTreeMap;

use super::types::{FileReport, LawProfile, ReplayLawTuple};

pub(crate) fn build_shared_law_ids(reports: &[FileReport]) -> BTreeMap<ReplayLawTuple, String> {
    let mut tuples = Vec::<ReplayLawTuple>::new();
    for report in reports {
        for law in &report.laws {
            tuples.push(law.law.clone());
        }
    }
    tuples.sort();
    tuples.dedup();

    let mut out = BTreeMap::<ReplayLawTuple, String>::new();
    for (idx, law) in tuples.into_iter().enumerate() {
        out.insert(law, format!("G{}", idx));
    }
    out
}

pub(crate) fn build_profiles(
    reports: &[FileReport],
    shared_law_ids: &BTreeMap<ReplayLawTuple, String>,
) -> Vec<LawProfile> {
    let mut per_law = Vec::<LawProfile>::new();

    for (law_tuple, global_id) in shared_law_ids {
        let mut file_count = 0usize;
        let mut path_hits = 0usize;
        let mut total_window_count = 0usize;
        let mut total_segment_count = 0usize;
        let mut total_covered_bytes = 0usize;
        let mut weighted_payload_sum = 0.0f64;
        let mut weighted_match_sum = 0.0f64;
        let mut weighted_match_vs_majority_sum = 0.0f64;
        let mut weighted_balanced_sum = 0.0f64;
        let mut weighted_macro_f1_sum = 0.0f64;
        let mut weighted_f1_newline_sum = 0.0f64;
        let mut weight_total = 0usize;
        let mut window_payload_sum = 0usize;
        let mut window_match_sum = 0.0f64;
        let mut window_seen = 0usize;
        let mut best_window_payload_exact = usize::MAX;
        let mut best_window_input = String::new();
        let mut best_window_idx = 0usize;
        let mut worst_window_payload_exact = 0usize;
        let mut worst_window_input = String::new();
        let mut worst_window_idx = 0usize;
        let mut knob_counts = BTreeMap::<String, usize>::new();

        for report in reports {
            let matching_laws = report
                .laws
                .iter()
                .filter(|law| &law.law == law_tuple)
                .collect::<Vec<_>>();
            if !matching_laws.is_empty() {
                file_count += 1;
            }
            for law in matching_laws {
                total_window_count += law.window_count;
                total_segment_count += law.segment_count;
                total_covered_bytes += law.covered_bytes;
                weighted_payload_sum +=
                    law.mean_compact_field_total_payload_exact * law.window_count as f64;
                weighted_match_sum += law.mean_field_match_pct * law.window_count as f64;
                weighted_match_vs_majority_sum +=
                    law.mean_field_match_vs_majority_pct * law.window_count as f64;
                weighted_balanced_sum +=
                    law.mean_field_balanced_accuracy_pct * law.window_count as f64;
                weighted_macro_f1_sum += law.mean_field_macro_f1_pct * law.window_count as f64;
                weighted_f1_newline_sum += law.mean_field_f1_newline_pct * law.window_count as f64;
                weight_total += law.window_count;
            }

            let local_to_global = report
                .laws
                .iter()
                .map(|law| {
                    let mapped = shared_law_ids
                        .get(&law.law)
                        .cloned()
                        .unwrap_or_else(|| "G?".to_string());
                    (law.local_law_id.clone(), mapped)
                })
                .collect::<BTreeMap<_, _>>();

            path_hits += report
                .law_path
                .iter()
                .filter(|local_id| {
                    local_to_global
                        .get(*local_id)
                        .map(|g| g == global_id)
                        .unwrap_or(false)
                })
                .count();

            for window in &report.windows {
                if let Some(mapped) = local_to_global.get(&window.local_law_id) {
                    if mapped == global_id {
                        window_seen += 1;
                        window_payload_sum += window.compact_field_total_payload_exact;
                        window_match_sum += window.field_match_pct;
                        let sig = format!(
                            "chunk_bytes={} chunk_search_objective={} chunk_raw_slack={}",
                            window.chunk_bytes,
                            window.chunk_search_objective,
                            window.chunk_raw_slack
                        );
                        *knob_counts.entry(sig).or_default() += 1;

                        if window.compact_field_total_payload_exact < best_window_payload_exact {
                            best_window_payload_exact = window.compact_field_total_payload_exact;
                            best_window_input = report.input.clone();
                            best_window_idx = window.window_idx;
                        }
                        if window.compact_field_total_payload_exact > worst_window_payload_exact {
                            worst_window_payload_exact = window.compact_field_total_payload_exact;
                            worst_window_input = report.input.clone();
                            worst_window_idx = window.window_idx;
                        }
                    }
                }
            }
        }

        let (dominant_knob_signature, dominant_knob_count) = knob_counts
            .into_iter()
            .max_by_key(|(_, count)| *count)
            .unwrap_or_else(|| ("unknown".to_string(), 0));

        per_law.push(LawProfile {
            global_law_id: global_id.clone(),
            law: law_tuple.clone(),
            file_count,
            path_hits,
            total_window_count,
            total_segment_count,
            total_covered_bytes,
            weighted_mean_compact_field_total_payload_exact: if weight_total == 0 {
                0.0
            } else {
                weighted_payload_sum / weight_total as f64
            },
            weighted_mean_field_match_pct: if weight_total == 0 {
                0.0
            } else {
                weighted_match_sum / weight_total as f64
            },
            weighted_mean_field_match_vs_majority_pct: if weight_total == 0 {
                0.0
            } else {
                weighted_match_vs_majority_sum / weight_total as f64
            },
            weighted_mean_field_balanced_accuracy_pct: if weight_total == 0 {
                0.0
            } else {
                weighted_balanced_sum / weight_total as f64
            },
            weighted_mean_field_macro_f1_pct: if weight_total == 0 {
                0.0
            } else {
                weighted_macro_f1_sum / weight_total as f64
            },
            weighted_mean_field_f1_newline_pct: if weight_total == 0 {
                0.0
            } else {
                weighted_f1_newline_sum / weight_total as f64
            },
            mean_window_payload_exact: if window_seen == 0 {
                0.0
            } else {
                window_payload_sum as f64 / window_seen as f64
            },
            mean_window_match_pct: if window_seen == 0 {
                0.0
            } else {
                window_match_sum / window_seen as f64
            },
            best_window_payload_exact: if best_window_payload_exact == usize::MAX {
                0
            } else {
                best_window_payload_exact
            },
            best_window_input,
            best_window_idx,
            worst_window_payload_exact,
            worst_window_input,
            worst_window_idx,
            dominant_knob_signature,
            dominant_knob_count,
        });
    }

    per_law
}
