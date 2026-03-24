use anyhow::{bail, Result};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReplayConfig {
    pub(crate) recipe: String,
    pub(crate) inputs: Vec<String>,
    pub(crate) max_ticks: u64,
    pub(crate) window_bytes: usize,
    pub(crate) step_bytes: usize,
    pub(crate) max_windows: usize,
    pub(crate) seed_from: u64,
    pub(crate) seed_count: u64,
    pub(crate) seed_step: u64,
    pub(crate) recipe_seed: u64,
    pub(crate) chunk_sweep: String,
    pub(crate) chunk_search_objective: String,
    pub(crate) chunk_raw_slack: u64,
    pub(crate) map_max_depth: u8,
    pub(crate) map_depth_shift: u8,
    pub(crate) boundary_band_sweep: String,
    pub(crate) boundary_delta: usize,
    pub(crate) field_margin_sweep: String,
    pub(crate) newline_margin_add: u64,
    pub(crate) space_to_newline_margin_add: u64,
    pub(crate) newline_share_ppm_min: u32,
    pub(crate) newline_override_budget: usize,
    pub(crate) newline_demote_margin_sweep: String,
    pub(crate) newline_demote_keep_ppm_min: u32,
    pub(crate) newline_demote_keep_min: usize,
    pub(crate) newline_only_from_spacelike: bool,
    pub(crate) merge_gap_bytes: usize,
    pub(crate) allow_overlap_scout: bool,
    pub(crate) freeze_boundary_band: Option<usize>,
    pub(crate) freeze_field_margin: Option<u64>,
    pub(crate) freeze_newline_demote_margin: Option<u64>,
    pub(crate) local_chunk_sweep: String,
    pub(crate) local_chunk_search_objective: Option<String>,
    pub(crate) local_chunk_raw_slack: Option<u64>,
    pub(crate) default_local_chunk_bytes_arg: Option<usize>,
    pub(crate) tune_default_body: bool,
    pub(crate) default_body_chunk_sweep: Option<String>,
    pub(crate) body_select_objective: String,
    pub(crate) emit_body_scoreboard: bool,
    pub(crate) min_override_gain_exact: usize,
    pub(crate) exact_subset_limit: usize,
    pub(crate) global_law_id_arg: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProgramSummary {
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
    pub(crate) searched_total_piecewise_payload_exact: i64,
    pub(crate) projected_default_total_piecewise_payload_exact: i64,
    pub(crate) delta_default_total_piecewise_payload_exact: i64,
    pub(crate) projected_unpriced_best_mix_total_piecewise_payload_exact: i64,
    pub(crate) delta_unpriced_best_mix_total_piecewise_payload_exact: i64,
    pub(crate) selected_total_piecewise_payload_exact: i64,
    pub(crate) delta_selected_total_piecewise_payload_exact: i64,
    pub(crate) target_window_count: usize,
    pub(crate) searched_target_window_payload_exact: usize,
    pub(crate) default_target_window_payload_exact: usize,
    pub(crate) best_mix_target_window_payload_exact: usize,
    pub(crate) selected_target_window_payload_exact: usize,
    pub(crate) delta_selected_target_window_payload_exact: i64,
    pub(crate) override_path_mode: String,
    pub(crate) override_path_bytes_exact: usize,
    pub(crate) selected_override_window_count: usize,
    pub(crate) improved_target_window_count: usize,
    pub(crate) equal_target_window_count: usize,
    pub(crate) worsened_target_window_count: usize,
    pub(crate) closure_override_count: usize,
    pub(crate) closure_override_run_count: usize,
    pub(crate) closure_max_override_run_length: usize,
    pub(crate) closure_untouched_window_count: usize,
    pub(crate) closure_override_density_ppm: u32,
    pub(crate) closure_untouched_window_pct_ppm: u32,
    pub(crate) closure_mode_penalty_exact: usize,
    pub(crate) closure_penalty_exact: usize,
    pub(crate) closure_total_exact: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProgramFile {
    pub(crate) input: String,
    pub(crate) searched_total_piecewise_payload_exact: i64,
    pub(crate) projected_default_total_piecewise_payload_exact: i64,
    pub(crate) projected_unpriced_best_mix_total_piecewise_payload_exact: i64,
    pub(crate) selected_total_piecewise_payload_exact: i64,
    pub(crate) target_window_count: usize,
    pub(crate) override_path_mode: String,
    pub(crate) override_path_bytes_exact: usize,
    pub(crate) selected_override_window_count: usize,
    pub(crate) closure_override_count: usize,
    pub(crate) closure_override_run_count: usize,
    pub(crate) closure_max_override_run_length: usize,
    pub(crate) closure_untouched_window_count: usize,
    pub(crate) closure_override_density_ppm: u32,
    pub(crate) closure_untouched_window_pct_ppm: u32,
    pub(crate) closure_mode_penalty_exact: usize,
    pub(crate) closure_penalty_exact: usize,
    pub(crate) closure_total_exact: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProgramWindow {
    pub(crate) input_index: usize,
    pub(crate) input: String,
    pub(crate) window_idx: usize,
    pub(crate) target_ordinal: usize,
    pub(crate) start: usize,
    pub(crate) end: usize,
    pub(crate) span_bytes: usize,
    pub(crate) searched_payload_exact: usize,
    pub(crate) default_payload_exact: usize,
    pub(crate) best_payload_exact: usize,
    pub(crate) selected_payload_exact: usize,
    pub(crate) searched_chunk_bytes: usize,
    pub(crate) best_chunk_bytes: usize,
    pub(crate) selected_chunk_bytes: usize,
    pub(crate) selected_gain_exact: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProgramOverride {
    pub(crate) input_index: usize,
    pub(crate) input: String,
    pub(crate) window_idx: usize,
    pub(crate) target_ordinal: usize,
    pub(crate) best_chunk_bytes: usize,
    pub(crate) default_payload_exact: usize,
    pub(crate) best_payload_exact: usize,
    pub(crate) gain_exact: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LawProgramArtifact {
    pub(crate) config: ReplayConfig,
    pub(crate) summary: ProgramSummary,
    pub(crate) files: Vec<ProgramFile>,
    pub(crate) windows: Vec<ProgramWindow>,
    pub(crate) overrides: Vec<ProgramOverride>,
}

#[derive(Clone, Debug)]
pub(crate) struct ReplayEvalRow {
    pub(crate) input_index: usize,
    pub(crate) input: String,
    pub(crate) window_idx: usize,
    pub(crate) target_ordinal: usize,
    pub(crate) start: usize,
    pub(crate) end: usize,
    pub(crate) selected_chunk_bytes: usize,
    pub(crate) searched_payload_exact: usize,
    pub(crate) artifact_selected_payload_exact: usize,
    pub(crate) replay_payload_exact: usize,
    pub(crate) delta_vs_artifact_exact: i64,
    pub(crate) delta_vs_searched_exact: i64,
    pub(crate) field_match_pct: f64,
    pub(crate) collapse_90_flag: bool,
    pub(crate) newline_extinct_flag: bool,
    pub(crate) newline_floor_used: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct ReplayFileSummary {
    pub(crate) input: String,
    pub(crate) searched_total_piecewise_payload_exact: i64,
    pub(crate) artifact_selected_total_piecewise_payload_exact: i64,
    pub(crate) replay_selected_total_piecewise_payload_exact: i64,
    pub(crate) searched_target_window_payload_exact: usize,
    pub(crate) artifact_selected_target_window_payload_exact: usize,
    pub(crate) replay_target_window_payload_exact: usize,
    pub(crate) override_path_bytes_exact: usize,
    pub(crate) target_window_count: usize,
    pub(crate) drift_exact: i64,
    pub(crate) improved_vs_searched_count: usize,
    pub(crate) equal_vs_searched_count: usize,
    pub(crate) worsened_vs_searched_count: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct SurfaceScoreboard {
    pub(crate) searched_total_piecewise_payload_exact: i64,
    pub(crate) artifact_selected_total_piecewise_payload_exact: i64,
    pub(crate) replay_selected_total_piecewise_payload_exact: i64,
    pub(crate) frozen_total_piecewise_payload_exact: Option<i64>,
    pub(crate) split_total_piecewise_payload_exact: Option<i64>,
    pub(crate) bridge_total_piecewise_payload_exact: Option<i64>,
    pub(crate) best_surface: String,
    pub(crate) best_total_piecewise_payload_exact: i64,
    pub(crate) best_delta_vs_artifact_exact: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ParsedCsvSections {
    pub(crate) summary_rows: Vec<BTreeMap<String, String>>,
    pub(crate) file_rows: Vec<BTreeMap<String, String>>,
    pub(crate) window_rows: Vec<BTreeMap<String, String>>,
    pub(crate) override_selected_rows: Vec<BTreeMap<String, String>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ManifestWindowPos {
    pub(crate) start: usize,
    pub(crate) end: usize,
    pub(crate) span_bytes: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct FrozenEvalRow {
    pub(crate) compact_field_total_payload_exact: usize,
    pub(crate) field_match_pct: f64,
    pub(crate) field_pred_collapse_90_flag: bool,
    pub(crate) field_newline_extinct_flag: bool,
    pub(crate) field_newline_floor_used: usize,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ClosureShapeMetrics {
    pub(crate) override_count: usize,
    pub(crate) override_run_count: usize,
    pub(crate) max_override_run_length: usize,
    pub(crate) untouched_window_count: usize,
    pub(crate) override_density_ppm: u32,
    pub(crate) untouched_window_pct_ppm: u32,
    pub(crate) mode_penalty_exact: usize,
    pub(crate) closure_penalty_exact: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BodyCandidateScore {
    pub(crate) chunk_bytes: usize,
    pub(crate) selected_total_piecewise_payload_exact: i64,
    pub(crate) closure_total_exact: i64,
    pub(crate) closure_penalty_exact: usize,
    pub(crate) mode_penalty_exact: usize,
    pub(crate) selected_target_window_payload_exact: usize,
    pub(crate) selected_override_window_count: usize,
    pub(crate) override_run_count: usize,
    pub(crate) max_override_run_length: usize,
    pub(crate) override_path_bytes_exact: usize,
    pub(crate) projected_default_total_piecewise_payload_exact: i64,
    pub(crate) target_window_count: usize,
    pub(crate) untouched_window_count: usize,
    pub(crate) override_density_ppm: u32,
    pub(crate) untouched_window_pct_ppm: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BuildMaterialized {
    pub(crate) artifact: LawProgramArtifact,
    pub(crate) body_scores: Vec<BodyCandidateScore>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BodySelectObjective {
    SelectedTotal,
    ClosureTotal,
    SelectedTarget,
}

impl BodySelectObjective {
    pub(crate) fn parse(raw: &str) -> Result<Self> {
        match raw {
            "selected-total" => Ok(Self::SelectedTotal),
            "default-total" | "closure-total" => Ok(Self::ClosureTotal),
            "selected-target" => Ok(Self::SelectedTarget),
            other => bail!(
                "unsupported --body-select-objective {}; expected selected-total, closure-total, default-total, or selected-target",
                other
            ),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum OverridePathMode {
    None,
    Delta,
    Runs,
    Ordinals,
}

impl OverridePathMode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Delta => "delta",
            Self::Runs => "runs",
            Self::Ordinals => "ordinals",
        }
    }

    pub(crate) fn tie_rank(self) -> u8 {
        match self {
            Self::None => 0,
            Self::Delta => 1,
            Self::Runs => 2,
            Self::Ordinals => 3,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OverridePathPlan {
    pub(crate) mode: OverridePathMode,
    pub(crate) bytes: usize,
    pub(crate) ordinals: Vec<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OverrideCandidateRef {
    pub(crate) window_idx: usize,
    pub(crate) target_ordinal: usize,
    pub(crate) gain_exact: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SelectedFilePlan {
    pub(crate) mode: OverridePathMode,
    pub(crate) path_bytes_exact: usize,
    pub(crate) selected_window_ordinals: Vec<usize>,
    pub(crate) closure_shape: ClosureShapeMetrics,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OverrideSubsetScore {
    pub(crate) codec_net_total_delta_exact: i64,
    pub(crate) closure_net_total_delta_exact: i64,
    pub(crate) path_bytes_exact: usize,
    pub(crate) selected_count: usize,
    pub(crate) run_count: usize,
    pub(crate) max_run_length: usize,
    pub(crate) mode_rank: u8,
    pub(crate) ordinals: Vec<usize>,
    pub(crate) plan: OverridePathPlan,
    pub(crate) closure_shape: ClosureShapeMetrics,
}
