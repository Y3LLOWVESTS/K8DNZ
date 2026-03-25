#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ReplayLawTuple {
    pub(crate) boundary_band: usize,
    pub(crate) field_margin: u64,
    pub(crate) newline_demote_margin: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SearchKnobTuple {
    pub(crate) chunk_bytes: usize,
    pub(crate) chunk_search_objective: String,
    pub(crate) chunk_raw_slack: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct LawRow {
    pub(crate) local_law_id: String,
    pub(crate) law: ReplayLawTuple,
    pub(crate) window_count: usize,
    pub(crate) segment_count: usize,
    pub(crate) covered_bytes: usize,
    pub(crate) mean_compact_field_total_payload_exact: f64,
    pub(crate) mean_field_match_pct: f64,
    pub(crate) mean_field_match_vs_majority_pct: f64,
    pub(crate) mean_field_balanced_accuracy_pct: f64,
    pub(crate) mean_field_macro_f1_pct: f64,
    pub(crate) mean_field_f1_newline_pct: f64,
}

#[derive(Clone, Debug)]
pub(crate) struct ManifestWindowRow {
    pub(crate) window_idx: usize,
    pub(crate) local_law_id: String,
    pub(crate) start: usize,
    pub(crate) end: usize,
    pub(crate) span_bytes: usize,
    pub(crate) chunk_bytes: usize,
    pub(crate) chunk_search_objective: String,
    pub(crate) chunk_raw_slack: u64,
    pub(crate) compact_field_total_payload_exact: usize,
    pub(crate) field_match_pct: f64,
}

#[derive(Clone, Debug)]
pub(crate) struct FileReport {
    pub(crate) input: String,
    pub(crate) recipe: String,
    pub(crate) input_bytes: usize,
    pub(crate) windows_analyzed: usize,
    pub(crate) honest_non_overlapping: bool,
    pub(crate) shared_header_bytes_exact: usize,
    pub(crate) total_piecewise_payload_exact: usize,
    pub(crate) law_path: Vec<String>,
    pub(crate) laws: Vec<LawRow>,
    pub(crate) windows: Vec<ManifestWindowRow>,
}

#[derive(Clone, Debug)]
pub(crate) struct LawProfile {
    pub(crate) global_law_id: String,
    pub(crate) law: ReplayLawTuple,
    pub(crate) file_count: usize,
    pub(crate) path_hits: usize,
    pub(crate) total_window_count: usize,
    pub(crate) total_segment_count: usize,
    pub(crate) total_covered_bytes: usize,
    pub(crate) weighted_mean_compact_field_total_payload_exact: f64,
    pub(crate) weighted_mean_field_match_pct: f64,
    pub(crate) weighted_mean_field_match_vs_majority_pct: f64,
    pub(crate) weighted_mean_field_balanced_accuracy_pct: f64,
    pub(crate) weighted_mean_field_macro_f1_pct: f64,
    pub(crate) weighted_mean_field_f1_newline_pct: f64,
    pub(crate) mean_window_payload_exact: f64,
    pub(crate) mean_window_match_pct: f64,
    pub(crate) best_window_payload_exact: usize,
    pub(crate) best_window_input: String,
    pub(crate) best_window_idx: usize,
    pub(crate) worst_window_payload_exact: usize,
    pub(crate) worst_window_input: String,
    pub(crate) worst_window_idx: usize,
    pub(crate) dominant_knob_signature: String,
    pub(crate) dominant_knob_count: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct EvalConfig {
    pub(crate) law: ReplayLawTuple,
    pub(crate) search: SearchKnobTuple,
}

#[derive(Clone, Debug)]
pub(crate) struct FrozenEvalRow {
    pub(crate) law: ReplayLawTuple,
    pub(crate) search: SearchKnobTuple,
    pub(crate) compact_field_total_payload_exact: usize,
    pub(crate) field_patch_bytes: usize,
    pub(crate) field_match_pct: f64,
    pub(crate) field_match_vs_majority_pct: f64,
    pub(crate) field_balanced_accuracy_pct: f64,
    pub(crate) field_macro_f1_pct: f64,
    pub(crate) field_f1_newline_pct: f64,
    pub(crate) field_pred_dominant_label: String,
    pub(crate) field_pred_dominant_share_pct: f64,
    pub(crate) field_pred_collapse_90_flag: bool,
    pub(crate) field_pred_newline_delta: i64,
    pub(crate) field_newline_demoted: usize,
    pub(crate) field_newline_after_demote: usize,
    pub(crate) field_newline_floor_used: usize,
    pub(crate) field_newline_extinct_flag: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct WindowEvalRow {
    pub(crate) input: String,
    pub(crate) window_idx: usize,
    pub(crate) target_ordinal: usize,
    pub(crate) start: usize,
    pub(crate) end: usize,
    pub(crate) span_bytes: usize,
    pub(crate) searched_local_law_id: String,
    pub(crate) searched_global_law_id: String,
    pub(crate) searched_chunk_bytes: usize,
    pub(crate) searched_payload_exact: usize,
    pub(crate) default_payload_exact: usize,
    pub(crate) best_chunk_bytes: usize,
    pub(crate) best_payload_exact: usize,
    pub(crate) selected_chunk_bytes: usize,
    pub(crate) selected_payload_exact: usize,
    pub(crate) default_gain_exact: i64,
    pub(crate) best_gain_exact: i64,
    pub(crate) selected_gain_exact: i64,
}

#[derive(Clone, Debug)]
pub(crate) struct OverrideCandidate {
    pub(crate) input: String,
    pub(crate) window_idx: usize,
    pub(crate) target_ordinal: usize,
    pub(crate) best_chunk_bytes: usize,
    pub(crate) default_payload_exact: usize,
    pub(crate) best_payload_exact: usize,
    pub(crate) gain_exact: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct FileSummary {
    pub(crate) input: String,
    pub(crate) searched_total_piecewise_payload_exact: usize,
    pub(crate) projected_default_total_piecewise_payload_exact: isize,
    pub(crate) delta_default_total_piecewise_payload_exact: i64,
    pub(crate) projected_unpriced_best_mix_total_piecewise_payload_exact: isize,
    pub(crate) delta_unpriced_best_mix_total_piecewise_payload_exact: i64,
    pub(crate) selected_total_piecewise_payload_exact: isize,
    pub(crate) delta_selected_total_piecewise_payload_exact: i64,
    pub(crate) target_window_count: usize,
    pub(crate) searched_target_window_payload_exact: usize,
    pub(crate) default_target_window_payload_exact: usize,
    pub(crate) best_mix_target_window_payload_exact: usize,
    pub(crate) selected_target_window_payload_exact: usize,
    pub(crate) delta_selected_target_window_payload_exact: i64,
    pub(crate) override_path_bytes_exact: usize,
    pub(crate) selected_override_window_count: usize,
    pub(crate) improved_target_window_count: usize,
    pub(crate) equal_target_window_count: usize,
    pub(crate) worsened_target_window_count: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct LocalMixSummary {
    pub(crate) recipe: String,
    pub(crate) file_count: usize,
    pub(crate) honest_file_count: usize,
    pub(crate) union_law_count: usize,
    pub(crate) target_global_law_id: String,
    pub(crate) target_global_law_path_hits: usize,
    pub(crate) target_global_law_file_count: usize,
    pub(crate) target_global_law_total_window_count: usize,
    pub(crate) target_global_law_total_segment_count: usize,
    pub(crate) target_global_law_total_covered_bytes: usize,
    pub(crate) target_global_law_dominant_knob_signature: String,
    pub(crate) eval_boundary_band: usize,
    pub(crate) eval_field_margin: u64,
    pub(crate) eval_newline_demote_margin: u64,
    pub(crate) eval_chunk_search_objective: String,
    pub(crate) eval_chunk_raw_slack: u64,
    pub(crate) eval_chunk_candidates: String,
    pub(crate) eval_chunk_candidate_count: usize,
    pub(crate) default_local_chunk_bytes: usize,
    pub(crate) default_local_chunk_window_wins: usize,
    pub(crate) searched_total_piecewise_payload_exact: usize,
    pub(crate) projected_default_total_piecewise_payload_exact: isize,
    pub(crate) delta_default_total_piecewise_payload_exact: i64,
    pub(crate) projected_unpriced_best_mix_total_piecewise_payload_exact: isize,
    pub(crate) delta_unpriced_best_mix_total_piecewise_payload_exact: i64,
    pub(crate) selected_total_piecewise_payload_exact: isize,
    pub(crate) delta_selected_total_piecewise_payload_exact: i64,
    pub(crate) target_window_count: usize,
    pub(crate) searched_target_window_payload_exact: usize,
    pub(crate) default_target_window_payload_exact: usize,
    pub(crate) best_mix_target_window_payload_exact: usize,
    pub(crate) selected_target_window_payload_exact: usize,
    pub(crate) delta_selected_target_window_payload_exact: i64,
    pub(crate) override_path_bytes_exact: usize,
    pub(crate) selected_override_window_count: usize,
    pub(crate) improved_target_window_count: usize,
    pub(crate) equal_target_window_count: usize,
    pub(crate) worsened_target_window_count: usize,
    pub(crate) best_gain_input: String,
    pub(crate) best_gain_window_idx: usize,
    pub(crate) best_gain_delta_payload_exact: i64,
    pub(crate) worst_loss_input: String,
    pub(crate) worst_loss_window_idx: usize,
    pub(crate) worst_loss_delta_payload_exact: i64,
}
