use anyhow::{anyhow, Context, Result};
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::cmd::apextrace::{ApexLaneLawBridgeFreezeArgs, ChunkSearchObjective, RenderFormat};

use super::common::write_or_print;

const HEADER_MAGIC: &[u8; 4] = b"AKMH";
const LAW_MAGIC: &[u8; 4] = b"AKML";
const WINDOW_PATH_MAGIC: &[u8; 4] = b"AKMW";
const SEGMENT_PATH_MAGIC: &[u8; 4] = b"AKMS";
const VERSION: u8 = 1;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ReplayLawTuple {
    boundary_band: usize,
    field_margin: u64,
    newline_demote_margin: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SearchKnobTuple {
    chunk_bytes: usize,
    chunk_search_objective: String,
    chunk_raw_slack: u64,
}

#[derive(Clone, Debug)]
struct LawRow {
    local_law_id: String,
    law: ReplayLawTuple,
    window_count: usize,
    segment_count: usize,
    covered_bytes: usize,
    mean_compact_field_total_payload_exact: f64,
    mean_field_match_pct: f64,
    mean_field_match_vs_majority_pct: f64,
    mean_field_balanced_accuracy_pct: f64,
    mean_field_macro_f1_pct: f64,
    mean_field_f1_newline_pct: f64,
}

#[derive(Clone, Debug)]
struct ManifestWindowRow {
    window_idx: usize,
    local_law_id: String,
    start: usize,
    end: usize,
    span_bytes: usize,
    chunk_bytes: usize,
    chunk_search_objective: String,
    chunk_raw_slack: u64,
    compact_field_total_payload_exact: usize,
    field_match_pct: f64,
}

#[derive(Clone, Debug)]
struct FileReport {
    input: String,
    recipe: String,
    input_bytes: usize,
    windows_analyzed: usize,
    honest_non_overlapping: bool,
    shared_header_bytes_exact: usize,
    total_piecewise_payload_exact: usize,
    law_path: Vec<String>,
    laws: Vec<LawRow>,
    windows: Vec<ManifestWindowRow>,
}

#[derive(Clone, Debug)]
struct LawProfile {
    global_law_id: String,
    law: ReplayLawTuple,
    file_count: usize,
    path_hits: usize,
    total_window_count: usize,
    total_segment_count: usize,
    total_covered_bytes: usize,
    weighted_mean_compact_field_total_payload_exact: f64,
    weighted_mean_field_match_pct: f64,
    weighted_mean_field_match_vs_majority_pct: f64,
    weighted_mean_field_balanced_accuracy_pct: f64,
    weighted_mean_field_macro_f1_pct: f64,
    weighted_mean_field_f1_newline_pct: f64,
    mean_window_payload_exact: f64,
    mean_window_match_pct: f64,
    best_window_payload_exact: usize,
    best_window_input: String,
    best_window_idx: usize,
    worst_window_payload_exact: usize,
    worst_window_input: String,
    worst_window_idx: usize,
    dominant_knob_signature: String,
    dominant_knob_count: usize,
}

#[derive(Clone, Debug)]
struct FreezeConfig {
    global_law_id: String,
    law: ReplayLawTuple,
    search: SearchKnobTuple,
    chunk_candidates: Vec<usize>,
    chunk_candidate_source: String,
}

#[derive(Clone, Debug)]
struct FrozenEvalRow {
    law: ReplayLawTuple,
    search: SearchKnobTuple,
    compact_field_total_payload_exact: usize,
    field_patch_bytes: usize,
    field_match_pct: f64,
    field_match_vs_majority_pct: f64,
    field_balanced_accuracy_pct: f64,
    field_macro_f1_pct: f64,
    field_f1_newline_pct: f64,
    field_pred_dominant_label: String,
    field_pred_dominant_share_pct: f64,
    field_pred_collapse_90_flag: bool,
    field_pred_newline_delta: i64,
    field_newline_demoted: usize,
    field_newline_after_demote: usize,
    field_newline_floor_used: usize,
    field_newline_extinct_flag: bool,
}

#[derive(Clone, Debug)]
struct WindowDeltaRow {
    input: String,
    window_idx: usize,
    start: usize,
    end: usize,
    span_bytes: usize,
    searched_local_law_id: String,
    searched_global_law_id: String,
    searched_payload_exact: usize,
    frozen_payload_exact: usize,
    delta_payload_exact: i64,
    searched_match_pct: f64,
    frozen_match_pct: f64,
    searched_chunk_bytes: usize,
    frozen_chunk_bytes: usize,
    searched_chunk_search_objective: String,
    frozen_chunk_search_objective: String,
    searched_chunk_raw_slack: u64,
    frozen_chunk_raw_slack: u64,
    searched_boundary_band: usize,
    frozen_boundary_band: usize,
    searched_field_margin: u64,
    frozen_field_margin: u64,
    searched_newline_demote_margin: u64,
    frozen_newline_demote_margin: u64,
}

#[derive(Clone, Debug)]
struct FrozenWindowRow {
    window_idx: usize,
    start: usize,
    end: usize,
    span_bytes: usize,
    law_id: String,
    law: ReplayLawTuple,
    compact_field_total_payload_exact: usize,
    field_match_pct: f64,
    field_match_vs_majority_pct: f64,
    field_balanced_accuracy_pct: f64,
    field_macro_f1_pct: f64,
    field_f1_newline_pct: f64,
}

#[derive(Clone, Debug)]
struct SegmentRow {
    segment_idx: usize,
    law_id: String,
    start: usize,
    end: usize,
    span_bytes: usize,
    window_count: usize,
    first_window_idx: usize,
    last_window_idx: usize,
    mean_compact_field_total_payload_exact: f64,
    mean_field_match_pct: f64,
    mean_field_balanced_accuracy_pct: f64,
    mean_field_macro_f1_pct: f64,
    mean_field_f1_newline_pct: f64,
}

#[derive(Clone, Debug)]
struct FrozenLawSummary {
    law_id: String,
    law: ReplayLawTuple,
    window_count: usize,
    segment_count: usize,
    covered_bytes: usize,
    mean_compact_field_total_payload_exact: f64,
    mean_field_match_pct: f64,
    mean_field_match_vs_majority_pct: f64,
    mean_field_balanced_accuracy_pct: f64,
    mean_field_macro_f1_pct: f64,
    mean_field_f1_newline_pct: f64,
}

#[derive(Clone, Debug)]
struct ReplayHeader {
    window_bytes: usize,
    step_bytes: usize,
    boundary_delta: usize,
    map_max_depth: u8,
    map_depth_shift: u8,
    newline_margin_add: u64,
    space_to_newline_margin_add: u64,
    newline_share_ppm_min: u32,
    newline_override_budget: usize,
    newline_demote_keep_ppm_min: u32,
    newline_demote_keep_min: usize,
    newline_only_from_spacelike: bool,
}


#[derive(Clone, Debug)]
struct FrozenFileSummary {
    input: String,
    searched_total_piecewise_payload_exact: usize,
    frozen_total_piecewise_payload_exact: usize,
    delta_total_piecewise_payload_exact: i64,
    frozen_local_compact_payload_bytes_exact: usize,
    frozen_shared_header_bytes_exact: usize,
    frozen_law_dictionary_bytes_exact: usize,
    frozen_window_path_bytes_exact: usize,
    frozen_segment_path_bytes_exact: usize,
    frozen_selected_path_mode: String,
    frozen_selected_path_bytes_exact: usize,
    all_window_count: usize,
    target_window_count: usize,
    searched_target_window_payload_exact: usize,
    frozen_target_window_payload_exact: usize,
    delta_target_window_payload_exact: i64,
    bridged_window_count: usize,
    bridge_candidate_count: usize,
    bridge_accepted_count: usize,
    bridge_local_penalty_exact: i64,
    bridge_total_gain_exact: i64,
    improved_window_count: usize,
    equal_window_count: usize,
    worsened_window_count: usize,
    improved_target_window_count: usize,
    equal_target_window_count: usize,
    worsened_target_window_count: usize,
}

#[derive(Clone, Debug)]
struct FreezeSummary {
    recipe: String,
    file_count: usize,
    honest_file_count: usize,
    union_law_count: usize,
    target_global_law_id: String,
    target_global_law_path_hits: usize,
    target_global_law_file_count: usize,
    target_global_law_total_window_count: usize,
    target_global_law_total_segment_count: usize,
    target_global_law_total_covered_bytes: usize,
    target_global_law_dominant_knob_signature: String,
    bridge_chunk_candidates: String,
    bridge_chunk_candidate_count: usize,
    bridge_chunk_candidate_source: String,
    freeze_chunk_bytes: usize,
    freeze_chunk_search_objective: String,
    freeze_chunk_raw_slack: u64,
    freeze_boundary_band: usize,
    freeze_field_margin: u64,
    freeze_newline_demote_margin: u64,
    searched_total_piecewise_payload_exact: usize,
    frozen_total_piecewise_payload_exact: usize,
    delta_total_piecewise_payload_exact: i64,
    all_window_count: usize,
    target_window_count: usize,
    searched_target_window_payload_exact: usize,
    frozen_target_window_payload_exact: usize,
    delta_target_window_payload_exact: i64,
    bridged_window_count: usize,
    bridge_candidate_count: usize,
    bridge_accepted_count: usize,
    bridge_local_penalty_exact: i64,
    bridge_total_gain_exact: i64,
    improved_window_count: usize,
    equal_window_count: usize,
    worsened_window_count: usize,
    improved_target_window_count: usize,
    equal_target_window_count: usize,
    worsened_target_window_count: usize,
    best_gain_input: String,
    best_gain_window_idx: usize,
    best_gain_delta_payload_exact: i64,
    worst_loss_input: String,
    worst_loss_window_idx: usize,
    worst_loss_delta_payload_exact: i64,
}

#[derive(Clone, Debug)]
struct EncodedTotals {
    total_piecewise_payload_exact: usize,
    local_compact_payload_bytes_exact: usize,
    shared_header_bytes_exact: usize,
    law_dictionary_bytes_exact: usize,
    window_path_bytes_exact: usize,
    segment_path_bytes_exact: usize,
    selected_path_mode: String,
    selected_path_bytes_exact: usize,
}

#[derive(Clone, Debug)]
struct BridgeCandidate {
    candidate_idx: usize,
    first_window_idx: usize,
    last_window_idx: usize,
    start: usize,
    end: usize,
    span_bytes: usize,
    window_indices: Vec<usize>,
}

pub fn run_apex_lane_law_bridge_freeze(args: ApexLaneLawBridgeFreezeArgs) -> Result<()> {
    if args.inputs.is_empty() {
        return Err(anyhow!("apex-lane-law-bridge-freeze requires at least one --in input"));
    }

    let exe = env::current_exe().context("resolve current executable for apex-lane-law-bridge-freeze")?;
    let mut reports = Vec::with_capacity(args.inputs.len());
    for input in &args.inputs {
        let output = run_child_apex_lane_manifest(&exe, &args, input)?;
        let report = parse_manifest_txt(&output)
            .with_context(|| format!("parse apex-lane-manifest output for {}", input))?;
        reports.push(report);
    }

    let shared_law_ids = build_shared_law_ids(&reports);
    let profiles = build_profiles(&reports, &shared_law_ids);
    let target_profile = select_target_profile(&profiles, args.global_law_id.as_deref())?;
    let dominant_chunk_bytes = parse_knob_signature(&target_profile.dominant_knob_signature)
        .with_context(|| {
            format!(
                "parse dominant knob signature for {} from {}",
                target_profile.global_law_id, target_profile.dominant_knob_signature
            )
        })?
        .chunk_bytes;
    let (chunk_candidates, chunk_candidate_source) = select_chunk_candidates(
        &args,
        &reports,
        &shared_law_ids,
        &target_profile.global_law_id,
        dominant_chunk_bytes,
    )?;
    let freeze = build_freeze_config(
        &args,
        target_profile,
        chunk_candidates,
        chunk_candidate_source,
    )?;

    let temp_dir = make_temp_dir("apex_lane_law_bridge_freeze")?;
    let mut file_summaries = Vec::with_capacity(reports.len());
    let mut all_window_deltas = Vec::new();

    for report in &reports {
        let input_bytes = fs::read(&report.input)
            .with_context(|| format!("read input for bridge freeze eval {}", report.input))?;
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

        let replay_header = ReplayHeader {
            window_bytes: args.window_bytes,
            step_bytes: args.step_bytes,
            boundary_delta: args.boundary_delta,
            map_max_depth: args.map_max_depth,
            map_depth_shift: args.map_depth_shift,
            newline_margin_add: args.newline_margin_add,
            space_to_newline_margin_add: args.space_to_newline_margin_add,
            newline_share_ppm_min: args.newline_share_ppm_min,
            newline_override_budget: args.newline_override_budget,
            newline_demote_keep_ppm_min: args.newline_demote_keep_ppm_min,
            newline_demote_keep_min: args.newline_demote_keep_min,
            newline_only_from_spacelike: args.newline_only_from_spacelike,
        };

        let mut eval_cache = BTreeMap::<usize, FrozenEvalRow>::new();
        let mut base_rows = Vec::with_capacity(report.windows.len());
        let mut target_window_count = 0usize;
        let mut searched_target_window_payload_exact = 0usize;
        let mut frozen_target_window_payload_exact = 0usize;

        for window in &report.windows {
            let global_law_id = local_to_global
                .get(&window.local_law_id)
                .cloned()
                .unwrap_or_else(|| "G?".to_string());
            let searched_law = report
                .laws
                .iter()
                .find(|law| law.local_law_id == window.local_law_id)
                .map(|law| law.law.clone())
                .unwrap_or_else(|| freeze.law.clone());

            if global_law_id == freeze.global_law_id {
                let frozen = eval_window_under_freeze(
                    &exe,
                    &args,
                    &freeze,
                    report,
                    &input_bytes,
                    window,
                    &temp_dir,
                    &mut eval_cache,
                )?;
                target_window_count += 1;
                searched_target_window_payload_exact = searched_target_window_payload_exact
                    .saturating_add(window.compact_field_total_payload_exact);
                frozen_target_window_payload_exact = frozen_target_window_payload_exact
                    .saturating_add(frozen.compact_field_total_payload_exact);

                base_rows.push(FrozenWindowRow {
                    window_idx: window.window_idx,
                    start: window.start,
                    end: window.end,
                    span_bytes: window.span_bytes,
                    law_id: "F0".to_string(),
                    law: freeze.law.clone(),
                    compact_field_total_payload_exact: frozen.compact_field_total_payload_exact,
                    field_match_pct: frozen.field_match_pct,
                    field_match_vs_majority_pct: frozen.field_match_vs_majority_pct,
                    field_balanced_accuracy_pct: frozen.field_balanced_accuracy_pct,
                    field_macro_f1_pct: frozen.field_macro_f1_pct,
                    field_f1_newline_pct: frozen.field_f1_newline_pct,
                });
            } else {
                base_rows.push(FrozenWindowRow {
                    window_idx: window.window_idx,
                    start: window.start,
                    end: window.end,
                    span_bytes: window.span_bytes,
                    law_id: window.local_law_id.clone(),
                    law: searched_law,
                    compact_field_total_payload_exact: window.compact_field_total_payload_exact,
                    field_match_pct: window.field_match_pct,
                    field_match_vs_majority_pct: 0.0,
                    field_balanced_accuracy_pct: 0.0,
                    field_macro_f1_pct: 0.0,
                    field_f1_newline_pct: 0.0,
                });
            }
        }

        let mut current_rows = base_rows.clone();
        let mut current_totals = compute_encoded_totals(
            report.input_bytes,
            &current_rows,
            &replay_header,
            args.merge_gap_bytes,
        );

        let bridge_candidates = build_bridge_candidates(
            report,
            &local_to_global,
            &freeze.global_law_id,
            args.bridge_max_windows,
            args.bridge_max_span_bytes,
        );

        let mut candidate_plans = Vec::new();
        for candidate in &bridge_candidates {
            let mut replacements = BTreeMap::<usize, FrozenWindowRow>::new();
            let mut local_penalty_exact = 0i64;
            for window_idx in &candidate.window_indices {
                let window = report
                    .windows
                    .iter()
                    .find(|row| row.window_idx == *window_idx)
                    .ok_or_else(|| anyhow!("missing bridge candidate window {}", window_idx))?;
                let frozen = eval_window_under_freeze(
                    &exe,
                    &args,
                    &freeze,
                    report,
                    &input_bytes,
                    window,
                    &temp_dir,
                    &mut eval_cache,
                )?;
                local_penalty_exact += (frozen.compact_field_total_payload_exact as i64)
                    - (window.compact_field_total_payload_exact as i64);
                replacements.insert(
                    *window_idx,
                    FrozenWindowRow {
                        window_idx: window.window_idx,
                        start: window.start,
                        end: window.end,
                        span_bytes: window.span_bytes,
                        law_id: "F0".to_string(),
                        law: freeze.law.clone(),
                        compact_field_total_payload_exact: frozen.compact_field_total_payload_exact,
                        field_match_pct: frozen.field_match_pct,
                        field_match_vs_majority_pct: frozen.field_match_vs_majority_pct,
                        field_balanced_accuracy_pct: frozen.field_balanced_accuracy_pct,
                        field_macro_f1_pct: frozen.field_macro_f1_pct,
                        field_f1_newline_pct: frozen.field_f1_newline_pct,
                    },
                );
            }
            let hypothetical_rows = apply_replacements(&current_rows, &replacements);
            let hypothetical_totals = compute_encoded_totals(
                report.input_bytes,
                &hypothetical_rows,
                &replay_header,
                args.merge_gap_bytes,
            );
            let bridge_total_gain_exact =
                (current_totals.total_piecewise_payload_exact as i64)
                    - (hypothetical_totals.total_piecewise_payload_exact as i64);

            candidate_plans.push((candidate.clone(), replacements, local_penalty_exact, bridge_total_gain_exact));
        }

        candidate_plans.sort_by(|a, b| {
            b.3.cmp(&a.3)
                .then_with(|| a.0.first_window_idx.cmp(&b.0.first_window_idx))
                .then_with(|| a.0.last_window_idx.cmp(&b.0.last_window_idx))
        });

        let mut accepted_windows = std::collections::BTreeSet::<usize>::new();
        let mut bridge_accepted_count = 0usize;
        let mut bridge_local_penalty_exact = 0i64;
        let mut bridge_total_gain_exact = 0i64;
        let mut bridged_window_count = 0usize;

        for (candidate, replacements, cached_local_penalty_exact, _cached_gain_exact) in candidate_plans {
            if candidate
                .window_indices
                .iter()
                .any(|idx| accepted_windows.contains(idx))
            {
                continue;
            }

            if cached_local_penalty_exact > args.bridge_max_local_penalty_exact as i64 {
                continue;
            }

            let hypothetical_rows = apply_replacements(&current_rows, &replacements);
            let hypothetical_totals = compute_encoded_totals(
                report.input_bytes,
                &hypothetical_rows,
                &replay_header,
                args.merge_gap_bytes,
            );
            let bridge_gain_exact =
                (current_totals.total_piecewise_payload_exact as i64)
                    - (hypothetical_totals.total_piecewise_payload_exact as i64);

            if bridge_gain_exact < args.bridge_min_total_gain_exact as i64 {
                continue;
            }

            current_rows = hypothetical_rows;
            current_totals = hypothetical_totals;
            bridge_accepted_count += 1;
            bridge_local_penalty_exact += cached_local_penalty_exact;
            bridge_total_gain_exact += bridge_gain_exact;
            bridged_window_count += candidate.window_indices.len();
            for idx in candidate.window_indices {
                accepted_windows.insert(idx);
            }
        }

        let mut improved_window_count = 0usize;
        let mut equal_window_count = 0usize;
        let mut worsened_window_count = 0usize;
        let mut improved_target_window_count = 0usize;
        let mut equal_target_window_count = 0usize;
        let mut worsened_target_window_count = 0usize;
        let final_rows_by_idx = current_rows
            .iter()
            .map(|row| (row.window_idx, row))
            .collect::<BTreeMap<_, _>>();

        for window in &report.windows {
            let global_law_id = local_to_global
                .get(&window.local_law_id)
                .cloned()
                .unwrap_or_else(|| "G?".to_string());
            let is_target_window = global_law_id == freeze.global_law_id;
            let searched_law = report
                .laws
                .iter()
                .find(|law| law.local_law_id == window.local_law_id)
                .map(|law| law.law.clone())
                .unwrap_or_else(|| freeze.law.clone());
            let final_row = final_rows_by_idx
                .get(&window.window_idx)
                .ok_or_else(|| anyhow!("missing final row for window {}", window.window_idx))?;
            let delta_payload_exact = (final_row.compact_field_total_payload_exact as i64)
                - (window.compact_field_total_payload_exact as i64);
            match delta_payload_exact.cmp(&0) {
                std::cmp::Ordering::Less => improved_window_count += 1,
                std::cmp::Ordering::Equal => equal_window_count += 1,
                std::cmp::Ordering::Greater => worsened_window_count += 1,
            }
            if is_target_window {
                match delta_payload_exact.cmp(&0) {
                    std::cmp::Ordering::Less => improved_target_window_count += 1,
                    std::cmp::Ordering::Equal => equal_target_window_count += 1,
                    std::cmp::Ordering::Greater => worsened_target_window_count += 1,
                }
            }

            all_window_deltas.push(WindowDeltaRow {
                input: report.input.clone(),
                window_idx: window.window_idx,
                start: window.start,
                end: window.end,
                span_bytes: window.span_bytes,
                searched_local_law_id: window.local_law_id.clone(),
                searched_global_law_id: global_law_id,
                searched_payload_exact: window.compact_field_total_payload_exact,
                frozen_payload_exact: final_row.compact_field_total_payload_exact,
                delta_payload_exact,
                searched_match_pct: window.field_match_pct,
                frozen_match_pct: final_row.field_match_pct,
                searched_chunk_bytes: window.chunk_bytes,
                frozen_chunk_bytes: if final_row.law_id == "F0" {
                    eval_cache
                        .get(&window.window_idx)
                        .map(|row| row.search.chunk_bytes)
                        .unwrap_or(window.chunk_bytes)
                } else {
                    window.chunk_bytes
                },
                searched_chunk_search_objective: window.chunk_search_objective.clone(),
                frozen_chunk_search_objective: if final_row.law_id == "F0" {
                    eval_cache
                        .get(&window.window_idx)
                        .map(|row| row.search.chunk_search_objective.clone())
                        .unwrap_or_else(|| window.chunk_search_objective.clone())
                } else {
                    window.chunk_search_objective.clone()
                },
                searched_chunk_raw_slack: window.chunk_raw_slack,
                frozen_chunk_raw_slack: if final_row.law_id == "F0" {
                    eval_cache
                        .get(&window.window_idx)
                        .map(|row| row.search.chunk_raw_slack)
                        .unwrap_or(window.chunk_raw_slack)
                } else {
                    window.chunk_raw_slack
                },
                searched_boundary_band: searched_law.boundary_band,
                frozen_boundary_band: final_row.law.boundary_band,
                searched_field_margin: searched_law.field_margin,
                frozen_field_margin: final_row.law.field_margin,
                searched_newline_demote_margin: searched_law.newline_demote_margin,
                frozen_newline_demote_margin: final_row.law.newline_demote_margin,
            });
        }

        file_summaries.push(FrozenFileSummary {
            input: report.input.clone(),
            searched_total_piecewise_payload_exact: report.total_piecewise_payload_exact,
            frozen_total_piecewise_payload_exact: current_totals.total_piecewise_payload_exact,
            delta_total_piecewise_payload_exact: (current_totals.total_piecewise_payload_exact as i64)
                - (report.total_piecewise_payload_exact as i64),
            frozen_local_compact_payload_bytes_exact: current_totals.local_compact_payload_bytes_exact,
            frozen_shared_header_bytes_exact: current_totals.shared_header_bytes_exact,
            frozen_law_dictionary_bytes_exact: current_totals.law_dictionary_bytes_exact,
            frozen_window_path_bytes_exact: current_totals.window_path_bytes_exact,
            frozen_segment_path_bytes_exact: current_totals.segment_path_bytes_exact,
            frozen_selected_path_mode: current_totals.selected_path_mode.clone(),
            frozen_selected_path_bytes_exact: current_totals.selected_path_bytes_exact,
            all_window_count: report.windows.len(),
            target_window_count,
            searched_target_window_payload_exact,
            frozen_target_window_payload_exact,
            delta_target_window_payload_exact: (frozen_target_window_payload_exact as i64)
                - (searched_target_window_payload_exact as i64),
            bridged_window_count,
            bridge_candidate_count: bridge_candidates.len(),
            bridge_accepted_count,
            bridge_local_penalty_exact,
            bridge_total_gain_exact,
            improved_window_count,
            equal_window_count,
            worsened_window_count,
            improved_target_window_count,
            equal_target_window_count,
            worsened_target_window_count,
        });
    }

    if !args.keep_temp_dir {
        let _ = fs::remove_dir_all(&temp_dir);
    }

    let searched_total_piecewise_payload_exact = file_summaries
        .iter()
        .map(|row| row.searched_total_piecewise_payload_exact)
        .sum::<usize>();
    let frozen_total_piecewise_payload_exact = file_summaries
        .iter()
        .map(|row| row.frozen_total_piecewise_payload_exact)
        .sum::<usize>();
    let searched_target_window_payload_exact = file_summaries
        .iter()
        .map(|row| row.searched_target_window_payload_exact)
        .sum::<usize>();
    let frozen_target_window_payload_exact = file_summaries
        .iter()
        .map(|row| row.frozen_target_window_payload_exact)
        .sum::<usize>();

    let mut gains = all_window_deltas.clone();
    gains.sort_by_key(|row| row.delta_payload_exact);
    let mut losses = all_window_deltas.clone();
    losses.sort_by_key(|row| std::cmp::Reverse(row.delta_payload_exact));

    let best_gain = gains
        .iter()
        .find(|row| row.delta_payload_exact < 0)
        .cloned()
        .unwrap_or_else(|| WindowDeltaRow {
            input: String::new(),
            window_idx: 0,
            start: 0,
            end: 0,
            span_bytes: 0,
            searched_local_law_id: String::new(),
            searched_global_law_id: String::new(),
            searched_payload_exact: 0,
            frozen_payload_exact: 0,
            delta_payload_exact: 0,
            searched_match_pct: 0.0,
            frozen_match_pct: 0.0,
            searched_chunk_bytes: 0,
            frozen_chunk_bytes: 0,
            searched_chunk_search_objective: String::new(),
            frozen_chunk_search_objective: String::new(),
            searched_chunk_raw_slack: 0,
            frozen_chunk_raw_slack: 0,
            searched_boundary_band: 0,
            frozen_boundary_band: 0,
            searched_field_margin: 0,
            frozen_field_margin: 0,
            searched_newline_demote_margin: 0,
            frozen_newline_demote_margin: 0,
        });
    let worst_loss = losses
        .iter()
        .find(|row| row.delta_payload_exact > 0)
        .cloned()
        .unwrap_or_else(|| WindowDeltaRow {
            input: String::new(),
            window_idx: 0,
            start: 0,
            end: 0,
            span_bytes: 0,
            searched_local_law_id: String::new(),
            searched_global_law_id: String::new(),
            searched_payload_exact: 0,
            frozen_payload_exact: 0,
            delta_payload_exact: 0,
            searched_match_pct: 0.0,
            frozen_match_pct: 0.0,
            searched_chunk_bytes: 0,
            frozen_chunk_bytes: 0,
            searched_chunk_search_objective: String::new(),
            frozen_chunk_search_objective: String::new(),
            searched_chunk_raw_slack: 0,
            frozen_chunk_raw_slack: 0,
            searched_boundary_band: 0,
            frozen_boundary_band: 0,
            searched_field_margin: 0,
            frozen_field_margin: 0,
            searched_newline_demote_margin: 0,
            frozen_newline_demote_margin: 0,
        });

    let summary = FreezeSummary {
        recipe: args.recipe.clone(),
        file_count: reports.len(),
        honest_file_count: reports.iter().filter(|r| r.honest_non_overlapping).count(),
        union_law_count: shared_law_ids.len(),
        target_global_law_id: freeze.global_law_id.clone(),
        target_global_law_path_hits: target_profile.path_hits,
        target_global_law_file_count: target_profile.file_count,
        target_global_law_total_window_count: target_profile.total_window_count,
        target_global_law_total_segment_count: target_profile.total_segment_count,
        target_global_law_total_covered_bytes: target_profile.total_covered_bytes,
        target_global_law_dominant_knob_signature: target_profile.dominant_knob_signature.clone(),
        bridge_chunk_candidates: join_usize_csv(&freeze.chunk_candidates),
        bridge_chunk_candidate_count: freeze.chunk_candidates.len(),
        bridge_chunk_candidate_source: freeze.chunk_candidate_source.clone(),
        freeze_chunk_bytes: freeze.search.chunk_bytes,
        freeze_chunk_search_objective: freeze.search.chunk_search_objective.clone(),
        freeze_chunk_raw_slack: freeze.search.chunk_raw_slack,
        freeze_boundary_band: freeze.law.boundary_band,
        freeze_field_margin: freeze.law.field_margin,
        freeze_newline_demote_margin: freeze.law.newline_demote_margin,
        searched_total_piecewise_payload_exact,
        frozen_total_piecewise_payload_exact,
        delta_total_piecewise_payload_exact: (frozen_total_piecewise_payload_exact as i64)
            - (searched_total_piecewise_payload_exact as i64),
        all_window_count: file_summaries.iter().map(|row| row.all_window_count).sum::<usize>(),
        target_window_count: file_summaries.iter().map(|row| row.target_window_count).sum::<usize>(),
        searched_target_window_payload_exact,
        frozen_target_window_payload_exact,
        delta_target_window_payload_exact: (frozen_target_window_payload_exact as i64)
            - (searched_target_window_payload_exact as i64),
        bridged_window_count: file_summaries.iter().map(|row| row.bridged_window_count).sum::<usize>(),
        bridge_candidate_count: file_summaries.iter().map(|row| row.bridge_candidate_count).sum::<usize>(),
        bridge_accepted_count: file_summaries.iter().map(|row| row.bridge_accepted_count).sum::<usize>(),
        bridge_local_penalty_exact: file_summaries.iter().map(|row| row.bridge_local_penalty_exact).sum::<i64>(),
        bridge_total_gain_exact: file_summaries.iter().map(|row| row.bridge_total_gain_exact).sum::<i64>(),
        improved_window_count: file_summaries.iter().map(|row| row.improved_window_count).sum::<usize>(),
        equal_window_count: file_summaries.iter().map(|row| row.equal_window_count).sum::<usize>(),
        worsened_window_count: file_summaries.iter().map(|row| row.worsened_window_count).sum::<usize>(),
        improved_target_window_count: file_summaries.iter().map(|row| row.improved_target_window_count).sum::<usize>(),
        equal_target_window_count: file_summaries.iter().map(|row| row.equal_target_window_count).sum::<usize>(),
        worsened_target_window_count: file_summaries.iter().map(|row| row.worsened_target_window_count).sum::<usize>(),
        best_gain_input: best_gain.input.clone(),
        best_gain_window_idx: best_gain.window_idx,
        best_gain_delta_payload_exact: best_gain.delta_payload_exact,
        worst_loss_input: worst_loss.input.clone(),
        worst_loss_window_idx: worst_loss.window_idx,
        worst_loss_delta_payload_exact: worst_loss.delta_payload_exact,
    };

    let body = match args.format {
        RenderFormat::Txt => render_txt(&summary, &file_summaries, &all_window_deltas, args.top_rows),
        RenderFormat::Csv => render_csv(&summary, &file_summaries, &all_window_deltas),
    };
    write_or_print(args.out.as_deref(), &body)?;
    if let Some(path) = args.out.as_deref() {
        eprintln!(
            "apex-lane-law-bridge-freeze: out={} files={} target={} searched_total_piecewise_payload_exact={} frozen_total_piecewise_payload_exact={} delta_total_piecewise_payload_exact={}",
            path,
            summary.file_count,
            summary.target_global_law_id,
            summary.searched_total_piecewise_payload_exact,
            summary.frozen_total_piecewise_payload_exact,
            summary.delta_total_piecewise_payload_exact,
        );
    }
    Ok(())
}

fn eval_window_under_freeze(
    exe: &Path,
    args: &ApexLaneLawBridgeFreezeArgs,
    freeze: &FreezeConfig,
    report: &FileReport,
    input_bytes: &[u8],
    window: &ManifestWindowRow,
    temp_dir: &Path,
    cache: &mut BTreeMap<usize, FrozenEvalRow>,
) -> Result<FrozenEvalRow> {
    if let Some(cached) = cache.get(&window.window_idx) {
        return Ok(cached.clone());
    }

    let slice = &input_bytes[window.start..window.end];
    let window_path = temp_dir.join(format!(
        "bridge_{}_window_{:04}_{:08}_{:08}.bin",
        sanitize_file_stem(&report.input),
        window.window_idx,
        window.start,
        window.end
    ));
    fs::write(&window_path, slice)
        .with_context(|| format!("write bridge freeze slice {}", window_path.display()))?;
    let frozen = run_child_frozen_apex_map_lane(exe, args, freeze, &window_path).with_context(|| {
        format!(
            "run bridge apex-map-lane input={} window_idx={}",
            report.input, window.window_idx
        )
    })?;
    cache.insert(window.window_idx, frozen.clone());
    Ok(frozen)
}

fn build_bridge_candidates(
    report: &FileReport,
    local_to_global: &BTreeMap<String, String>,
    target_global_law_id: &str,
    bridge_max_windows: usize,
    bridge_max_span_bytes: usize,
) -> Vec<BridgeCandidate> {
    if report.windows.len() < 3 || bridge_max_windows == 0 {
        return Vec::new();
    }

    let mut out = Vec::new();
    let mut idx = 0usize;
    while idx < report.windows.len() {
        let row = &report.windows[idx];
        let global = local_to_global
            .get(&row.local_law_id)
            .map(|s| s.as_str())
            .unwrap_or("G?");
        if global == target_global_law_id {
            idx += 1;
            continue;
        }

        let start_idx = idx;
        let mut end_idx = idx;
        while end_idx + 1 < report.windows.len() {
            let next = &report.windows[end_idx + 1];
            let next_global = local_to_global
                .get(&next.local_law_id)
                .map(|s| s.as_str())
                .unwrap_or("G?");
            if next_global == target_global_law_id {
                break;
            }
            end_idx += 1;
        }

        let bounded_by_target = start_idx > 0
            && end_idx + 1 < report.windows.len()
            && local_to_global
                .get(&report.windows[start_idx - 1].local_law_id)
                .map(|s| s.as_str())
                == Some(target_global_law_id)
            && local_to_global
                .get(&report.windows[end_idx + 1].local_law_id)
                .map(|s| s.as_str())
                == Some(target_global_law_id);

        if bounded_by_target {
            let window_count = end_idx - start_idx + 1;
            let start = report.windows[start_idx].start;
            let end = report.windows[end_idx].end;
            let span_bytes = end.saturating_sub(start);
            if window_count <= bridge_max_windows
                && (bridge_max_span_bytes == 0 || span_bytes <= bridge_max_span_bytes)
            {
                out.push(BridgeCandidate {
                    candidate_idx: out.len(),
                    first_window_idx: report.windows[start_idx].window_idx,
                    last_window_idx: report.windows[end_idx].window_idx,
                    start,
                    end,
                    span_bytes,
                    window_indices: report.windows[start_idx..=end_idx]
                        .iter()
                        .map(|row| row.window_idx)
                        .collect(),
                });
            }
        }

        idx = end_idx + 1;
    }

    out
}

fn apply_replacements(
    current_rows: &[FrozenWindowRow],
    replacements: &BTreeMap<usize, FrozenWindowRow>,
) -> Vec<FrozenWindowRow> {
    current_rows
        .iter()
        .map(|row| {
            replacements
                .get(&row.window_idx)
                .cloned()
                .unwrap_or_else(|| row.clone())
        })
        .collect()
}

fn compute_encoded_totals(
    input_bytes: usize,
    rows: &[FrozenWindowRow],
    replay_header: &ReplayHeader,
    merge_gap_bytes: usize,
) -> EncodedTotals {
    let segments = build_frozen_segments(rows, merge_gap_bytes);
    let laws = build_frozen_law_summaries(rows, &segments);
    let law_index_by_id = laws
        .iter()
        .enumerate()
        .map(|(idx, law)| (law.law_id.clone(), idx))
        .collect::<BTreeMap<_, _>>();

    let shared_header_bytes_exact = encode_replay_header(replay_header).len();
    let law_dictionary_bytes_exact = encode_law_dictionary(&laws).len();
    let window_path_bytes_exact =
        encode_window_path(input_bytes, rows, &law_index_by_id).len();
    let segment_path_bytes_exact =
        encode_segment_path(input_bytes, &segments, &law_index_by_id).len();
    let (selected_path_mode, selected_path_bytes_exact) =
        if segment_path_bytes_exact <= window_path_bytes_exact {
            ("segment".to_string(), segment_path_bytes_exact)
        } else {
            ("window".to_string(), window_path_bytes_exact)
        };
    let local_compact_payload_bytes_exact = rows
        .iter()
        .map(|row| row.compact_field_total_payload_exact)
        .sum::<usize>();
    let total_piecewise_payload_exact = local_compact_payload_bytes_exact
        .saturating_add(shared_header_bytes_exact)
        .saturating_add(law_dictionary_bytes_exact)
        .saturating_add(selected_path_bytes_exact);

    EncodedTotals {
        total_piecewise_payload_exact,
        local_compact_payload_bytes_exact,
        shared_header_bytes_exact,
        law_dictionary_bytes_exact,
        window_path_bytes_exact,
        segment_path_bytes_exact,
        selected_path_mode,
        selected_path_bytes_exact,
    }
}

fn select_target_profile<'a>(
    profiles: &'a [LawProfile],
    global_law_id: Option<&str>,
) -> Result<&'a LawProfile> {
    if profiles.is_empty() {
        return Err(anyhow!("apex-lane-law-bridge-freeze found no shared laws to freeze"));
    }

    if let Some(id) = global_law_id {
        return profiles
            .iter()
            .find(|profile| profile.global_law_id == id)
            .ok_or_else(|| anyhow!("requested --global-law-id {} was not present", id));
    }

    profiles
        .iter()
        .max_by_key(|profile| profile.path_hits)
        .ok_or_else(|| anyhow!("apex-lane-law-bridge-freeze could not select dominant law"))
}


fn build_freeze_config(
    args: &ApexLaneLawBridgeFreezeArgs,
    target: &LawProfile,
    chunk_candidates: Vec<usize>,
    chunk_candidate_source: String,
) -> Result<FreezeConfig> {
    let mut search = parse_knob_signature(&target.dominant_knob_signature)
        .with_context(|| {
            format!(
                "parse dominant knob signature for {} from {}",
                target.global_law_id, target.dominant_knob_signature
            )
        })?;

    if let Some(v) = args.bridge_chunk_search_objective {
        search.chunk_search_objective = chunk_search_objective_name(v).to_string();
    }
    if let Some(v) = args.bridge_chunk_raw_slack {
        search.chunk_raw_slack = v;
    }

    let mut candidates = if chunk_candidates.is_empty() {
        vec![search.chunk_bytes]
    } else {
        chunk_candidates
    };
    candidates.sort_unstable();
    candidates.dedup();
    if !candidates.contains(&search.chunk_bytes) {
        candidates.push(search.chunk_bytes);
        candidates.sort_unstable();
        candidates.dedup();
    }

    let law = ReplayLawTuple {
        boundary_band: args.freeze_boundary_band.unwrap_or(target.law.boundary_band),
        field_margin: args.freeze_field_margin.unwrap_or(target.law.field_margin),
        newline_demote_margin: args
            .freeze_newline_demote_margin
            .unwrap_or(target.law.newline_demote_margin),
    };

    Ok(FreezeConfig {
        global_law_id: target.global_law_id.clone(),
        law,
        search,
        chunk_candidates: candidates,
        chunk_candidate_source,
    })
}

fn select_chunk_candidates(
    args: &ApexLaneLawBridgeFreezeArgs,
    reports: &[FileReport],
    shared_law_ids: &BTreeMap<ReplayLawTuple, String>,
    target_global_law_id: &str,
    dominant_chunk_bytes: usize,
) -> Result<(Vec<usize>, String)> {
    if let Some(raw) = &args.bridge_chunk_sweep {
        let mut values = parse_usize_csv(raw)
            .with_context(|| format!("parse --bridge-chunk-sweep {}", raw))?;
        if values.is_empty() {
            return Err(anyhow!("--bridge-chunk-sweep must contain at least one chunk size"));
        }
        values.sort_unstable();
        values.dedup();
        return Ok((values, "explicit".to_string()));
    }

    let mut observed = Vec::new();
    for report in reports {
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
        for window in &report.windows {
            if local_to_global
                .get(&window.local_law_id)
                .map(|id| id.as_str())
                == Some(target_global_law_id)
            {
                observed.push(window.chunk_bytes);
            }
        }
    }

    if observed.is_empty() {
        Ok((vec![dominant_chunk_bytes], "dominant-fallback".to_string()))
    } else {
        observed.sort_unstable();
        observed.dedup();
        Ok((observed, "observed-target".to_string()))
    }
}

fn parse_usize_csv(raw: &str) -> Result<Vec<usize>> {
    let mut out = Vec::new();
    for token in raw.split(',') {
        let trimmed = token.trim();
        if trimmed.is_empty() {
            continue;
        }
        out.push(
            trimmed
                .parse::<usize>()
                .with_context(|| format!("parse usize token {}", trimmed))?,
        );
    }
    Ok(out)
}

fn parse_knob_signature(raw: &str) -> Result<SearchKnobTuple> {
    let tokens = tokenize_kv_line(raw);
    Ok(SearchKnobTuple {
        chunk_bytes: parse_required_usize(&tokens, "chunk_bytes")?,
        chunk_search_objective: parse_required_string(&tokens, "chunk_search_objective")?,
        chunk_raw_slack: parse_required_u64(&tokens, "chunk_raw_slack")?,
    })
}

fn run_child_apex_lane_manifest(
    exe: &Path,
    args: &ApexLaneLawBridgeFreezeArgs,
    input: &str,
) -> Result<String> {
    let mut cmd = Command::new(exe);
    cmd.arg("apextrace")
        .arg("apex-lane-manifest")
        .arg("--recipe")
        .arg(&args.recipe)
        .arg("--in")
        .arg(input)
        .arg("--max-ticks")
        .arg(args.max_ticks.to_string())
        .arg("--window-bytes")
        .arg(args.window_bytes.to_string())
        .arg("--step-bytes")
        .arg(args.step_bytes.to_string())
        .arg("--max-windows")
        .arg(args.max_windows.to_string())
        .arg("--seed-from")
        .arg(args.seed_from.to_string())
        .arg("--seed-count")
        .arg(args.seed_count.to_string())
        .arg("--seed-step")
        .arg(args.seed_step.to_string())
        .arg("--recipe-seed")
        .arg(args.recipe_seed.to_string())
        .arg("--chunk-sweep")
        .arg(&args.chunk_sweep)
        .arg("--chunk-search-objective")
        .arg(chunk_search_objective_name(args.chunk_search_objective))
        .arg("--chunk-raw-slack")
        .arg(args.chunk_raw_slack.to_string())
        .arg("--map-max-depth")
        .arg(args.map_max_depth.to_string())
        .arg("--map-depth-shift")
        .arg(args.map_depth_shift.to_string())
        .arg("--boundary-band-sweep")
        .arg(&args.boundary_band_sweep)
        .arg("--boundary-delta")
        .arg(args.boundary_delta.to_string())
        .arg("--field-margin-sweep")
        .arg(&args.field_margin_sweep)
        .arg("--newline-margin-add")
        .arg(args.newline_margin_add.to_string())
        .arg("--space-to-newline-margin-add")
        .arg(args.space_to_newline_margin_add.to_string())
        .arg("--newline-share-ppm-min")
        .arg(args.newline_share_ppm_min.to_string())
        .arg("--newline-override-budget")
        .arg(args.newline_override_budget.to_string())
        .arg("--newline-demote-margin-sweep")
        .arg(&args.newline_demote_margin_sweep)
        .arg("--newline-demote-keep-ppm-min")
        .arg(args.newline_demote_keep_ppm_min.to_string())
        .arg("--newline-demote-keep-min")
        .arg(args.newline_demote_keep_min.to_string())
        .arg(format!(
            "--newline-only-from-spacelike={}",
            if args.newline_only_from_spacelike { "true" } else { "false" }
        ))
        .arg("--merge-gap-bytes")
        .arg(args.merge_gap_bytes.to_string())
        .arg("--format")
        .arg("txt");

    if args.allow_overlap_scout {
        cmd.arg("--allow-overlap-scout");
    }
    if args.keep_temp_dir {
        cmd.arg("--keep-temp-dir");
    }

    let output = cmd
        .output()
        .with_context(|| format!("spawn child apex-lane-manifest for {}", input))?;

    if !output.status.success() {
        return Err(anyhow!(
            "child apex-lane-manifest failed input={} status={} stderr={} stdout={}",
            input,
            output.status,
            String::from_utf8_lossy(&output.stderr),
            String::from_utf8_lossy(&output.stdout),
        ));
    }

    String::from_utf8(output.stdout).context("child apex-lane-manifest stdout was not valid UTF-8")
}

fn run_child_frozen_apex_map_lane(
    exe: &Path,
    args: &ApexLaneLawBridgeFreezeArgs,
    freeze: &FreezeConfig,
    window_path: &Path,
) -> Result<FrozenEvalRow> {
    let mut cmd = Command::new(exe);
    cmd.arg("apextrace")
        .arg("apex-map-lane")
        .arg("--recipe")
        .arg(&args.recipe)
        .arg("--in")
        .arg(window_path)
        .arg("--max-ticks")
        .arg(args.max_ticks.to_string())
        .arg("--seed-from")
        .arg(args.seed_from.to_string())
        .arg("--seed-count")
        .arg(args.seed_count.to_string())
        .arg("--seed-step")
        .arg(args.seed_step.to_string())
        .arg("--recipe-seed")
        .arg(args.recipe_seed.to_string())
        .arg("--chunk-bytes")
        .arg(
            freeze
                .chunk_candidates
                .first()
                .copied()
                .unwrap_or(freeze.search.chunk_bytes)
                .to_string(),
        )
        .arg("--chunk-sweep")
        .arg(join_usize_csv(&freeze.chunk_candidates))
        .arg("--chunk-search-objective")
        .arg(&freeze.search.chunk_search_objective)
        .arg("--chunk-raw-slack")
        .arg(freeze.search.chunk_raw_slack.to_string())
        .arg("--map-max-depth")
        .arg(args.map_max_depth.to_string())
        .arg("--map-depth-shift")
        .arg(args.map_depth_shift.to_string())
        .arg("--boundary-band")
        .arg(freeze.law.boundary_band.to_string())
        .arg("--boundary-band-sweep")
        .arg(freeze.law.boundary_band.to_string())
        .arg("--boundary-delta")
        .arg(args.boundary_delta.to_string())
        .arg("--field-margin")
        .arg(freeze.law.field_margin.to_string())
        .arg("--field-margin-sweep")
        .arg(freeze.law.field_margin.to_string())
        .arg("--newline-margin-add")
        .arg(args.newline_margin_add.to_string())
        .arg("--space-to-newline-margin-add")
        .arg(args.space_to_newline_margin_add.to_string())
        .arg("--newline-share-ppm-min")
        .arg(args.newline_share_ppm_min.to_string())
        .arg("--newline-override-budget")
        .arg(args.newline_override_budget.to_string())
        .arg("--newline-demote-margin")
        .arg(freeze.law.newline_demote_margin.to_string())
        .arg("--newline-demote-margin-sweep")
        .arg(freeze.law.newline_demote_margin.to_string())
        .arg("--newline-demote-keep-ppm-min")
        .arg(args.newline_demote_keep_ppm_min.to_string())
        .arg("--newline-demote-keep-min")
        .arg(args.newline_demote_keep_min.to_string())
        .arg(format!(
            "--newline-only-from-spacelike={}",
            if args.newline_only_from_spacelike { "true" } else { "false" }
        ))
        .arg("--format")
        .arg("txt");

    let output = cmd
        .output()
        .with_context(|| format!("spawn frozen apex-map-lane for {}", window_path.display()))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Err(anyhow!(
            "child frozen apex-map-lane failed status={} window={} stderr={} stdout={}",
            output.status,
            window_path.display(),
            truncate_for_error(&stderr),
            truncate_for_error(&stdout)
        ));
    }

    parse_best_line(&output.stderr)
}

fn parse_manifest_txt(raw: &str) -> Result<FileReport> {
    let mut summary = BTreeMap::<String, String>::new();
    let mut section = "summary";
    let mut laws = Vec::new();
    let mut windows = Vec::new();

    for line in raw.lines().map(str::trim).filter(|line| !line.is_empty()) {
        match line {
            "--- laws ---" => {
                section = "laws";
                continue;
            }
            "--- segments ---" => {
                section = "segments";
                continue;
            }
            "--- windows ---" => {
                section = "windows";
                continue;
            }
            _ => {}
        }

        match section {
            "summary" => {
                if let Some((k, v)) = line.split_once('=') {
                    summary.insert(k.to_string(), v.to_string());
                }
            }
            "laws" => laws.push(parse_law_row(line)?),
            "windows" => windows.push(parse_window_row(line)?),
            _ => {}
        }
    }

    Ok(FileReport {
        input: parse_required_string(&summary, "input")?,
        recipe: parse_required_string(&summary, "recipe")?,
        input_bytes: parse_required_usize(&summary, "input_bytes")?,
        windows_analyzed: parse_required_usize(&summary, "windows_analyzed")?,
        honest_non_overlapping: parse_required_bool(&summary, "honest_non_overlapping")?,
        shared_header_bytes_exact: parse_required_usize(&summary, "shared_header_bytes_exact")?,
        total_piecewise_payload_exact: parse_required_usize(
            &summary,
            "total_piecewise_payload_exact",
        )?,
        law_path: parse_required_string(&summary, "law_path")?
            .split(',')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect(),
        laws,
        windows,
    })
}

fn parse_law_row(line: &str) -> Result<LawRow> {
    let tokens = tokenize_kv_line(line);
    Ok(LawRow {
        local_law_id: parse_required_string(&tokens, "law_id")?,
        law: ReplayLawTuple {
            boundary_band: parse_required_usize(&tokens, "boundary_band")?,
            field_margin: parse_required_u64(&tokens, "field_margin")?,
            newline_demote_margin: parse_required_u64(&tokens, "newline_demote_margin")?,
        },
        window_count: parse_required_usize(&tokens, "window_count")?,
        segment_count: parse_required_usize(&tokens, "segment_count")?,
        covered_bytes: parse_required_usize(&tokens, "covered_bytes")?,
        mean_compact_field_total_payload_exact: parse_required_f64(
            &tokens,
            "mean_compact_field_total_payload_exact",
        )?,
        mean_field_match_pct: parse_required_f64(&tokens, "mean_field_match_pct")?,
        mean_field_match_vs_majority_pct: parse_required_f64(
            &tokens,
            "mean_field_match_vs_majority_pct",
        )?,
        mean_field_balanced_accuracy_pct: parse_required_f64(
            &tokens,
            "mean_field_balanced_accuracy_pct",
        )?,
        mean_field_macro_f1_pct: parse_required_f64(&tokens, "mean_field_macro_f1_pct")?,
        mean_field_f1_newline_pct: parse_required_f64(&tokens, "mean_field_f1_newline_pct")?,
    })
}

fn parse_window_row(line: &str) -> Result<ManifestWindowRow> {
    let tokens = tokenize_kv_line(line);
    Ok(ManifestWindowRow {
        window_idx: parse_required_usize(&tokens, "window_idx")?,
        local_law_id: parse_required_string(&tokens, "law_id")?,
        start: parse_required_usize(&tokens, "start")?,
        end: parse_required_usize(&tokens, "end")?,
        span_bytes: parse_required_usize(&tokens, "span_bytes")?,
        chunk_bytes: parse_required_usize(&tokens, "chunk_bytes")?,
        chunk_search_objective: parse_required_string(&tokens, "chunk_search_objective")?,
        chunk_raw_slack: parse_required_u64(&tokens, "chunk_raw_slack")?,
        compact_field_total_payload_exact: parse_required_usize(
            &tokens,
            "compact_field_total_payload_exact",
        )?,
        field_match_pct: parse_required_f64(&tokens, "field_match_pct")?,
    })
}

fn tokenize_kv_line(line: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for token in line.split_whitespace() {
        if let Some((k, v)) = token.split_once('=') {
            out.insert(k.to_string(), v.to_string());
        }
    }
    out
}

fn build_shared_law_ids(reports: &[FileReport]) -> BTreeMap<ReplayLawTuple, String> {
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

fn build_profiles(
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
                weighted_f1_newline_sum +=
                    law.mean_field_f1_newline_pct * law.window_count as f64;
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

fn build_frozen_segments(rows: &[FrozenWindowRow], merge_gap_bytes: usize) -> Vec<SegmentRow> {
    if rows.is_empty() {
        return Vec::new();
    }

    let mut out = Vec::new();
    let mut cur_law = rows[0].law_id.clone();
    let mut cur_start = rows[0].start;
    let mut cur_end = rows[0].end;
    let mut cur_first_idx = rows[0].window_idx;
    let mut cur_last_idx = rows[0].window_idx;
    let mut cur_windows = vec![rows[0].clone()];

    for row in rows.iter().skip(1) {
        if row.law_id == cur_law && row.start <= cur_end.saturating_add(merge_gap_bytes) {
            cur_end = cur_end.max(row.end);
            cur_last_idx = row.window_idx;
            cur_windows.push(row.clone());
        } else {
            out.push(finish_frozen_segment(
                out.len(),
                &cur_law,
                cur_start,
                cur_end,
                cur_first_idx,
                cur_last_idx,
                &cur_windows,
            ));
            cur_law = row.law_id.clone();
            cur_start = row.start;
            cur_end = row.end;
            cur_first_idx = row.window_idx;
            cur_last_idx = row.window_idx;
            cur_windows.clear();
            cur_windows.push(row.clone());
        }
    }

    out.push(finish_frozen_segment(
        out.len(),
        &cur_law,
        cur_start,
        cur_end,
        cur_first_idx,
        cur_last_idx,
        &cur_windows,
    ));
    out
}

fn finish_frozen_segment(
    segment_idx: usize,
    law_id: &str,
    start: usize,
    end: usize,
    first_window_idx: usize,
    last_window_idx: usize,
    windows: &[FrozenWindowRow],
) -> SegmentRow {
    let count = windows.len().max(1) as f64;
    SegmentRow {
        segment_idx,
        law_id: law_id.to_string(),
        start,
        end,
        span_bytes: end.saturating_sub(start),
        window_count: windows.len(),
        first_window_idx,
        last_window_idx,
        mean_compact_field_total_payload_exact: windows
            .iter()
            .map(|w| w.compact_field_total_payload_exact as f64)
            .sum::<f64>()
            / count,
        mean_field_match_pct: windows.iter().map(|w| w.field_match_pct).sum::<f64>() / count,
        mean_field_balanced_accuracy_pct: windows
            .iter()
            .map(|w| w.field_balanced_accuracy_pct)
            .sum::<f64>()
            / count,
        mean_field_macro_f1_pct: windows
            .iter()
            .map(|w| w.field_macro_f1_pct)
            .sum::<f64>()
            / count,
        mean_field_f1_newline_pct: windows
            .iter()
            .map(|w| w.field_f1_newline_pct)
            .sum::<f64>()
            / count,
    }
}

fn build_frozen_law_summaries(
    rows: &[FrozenWindowRow],
    segments: &[SegmentRow],
) -> Vec<FrozenLawSummary> {
    let mut by_law = BTreeMap::<String, Vec<&FrozenWindowRow>>::new();
    for row in rows {
        by_law.entry(row.law_id.clone()).or_default().push(row);
    }

    let mut covered_bytes = BTreeMap::<String, usize>::new();
    let mut segment_count = BTreeMap::<String, usize>::new();
    for seg in segments {
        *covered_bytes.entry(seg.law_id.clone()).or_default() += seg.span_bytes;
        *segment_count.entry(seg.law_id.clone()).or_default() += 1;
    }

    let mut out = Vec::new();
    for (law_id, law_rows) in by_law {
        let count = law_rows.len().max(1) as f64;
        out.push(FrozenLawSummary {
            law_id: law_id.clone(),
            law: law_rows[0].law.clone(),
            window_count: law_rows.len(),
            segment_count: *segment_count.get(&law_id).unwrap_or(&0),
            covered_bytes: *covered_bytes.get(&law_id).unwrap_or(&0),
            mean_compact_field_total_payload_exact: law_rows
                .iter()
                .map(|w| w.compact_field_total_payload_exact as f64)
                .sum::<f64>()
                / count,
            mean_field_match_pct: law_rows.iter().map(|w| w.field_match_pct).sum::<f64>() / count,
            mean_field_match_vs_majority_pct: law_rows
                .iter()
                .map(|w| w.field_match_vs_majority_pct)
                .sum::<f64>()
                / count,
            mean_field_balanced_accuracy_pct: law_rows
                .iter()
                .map(|w| w.field_balanced_accuracy_pct)
                .sum::<f64>()
                / count,
            mean_field_macro_f1_pct: law_rows
                .iter()
                .map(|w| w.field_macro_f1_pct)
                .sum::<f64>()
                / count,
            mean_field_f1_newline_pct: law_rows
                .iter()
                .map(|w| w.field_f1_newline_pct)
                .sum::<f64>()
                / count,
        });
    }
    out
}

fn encode_replay_header(header: &ReplayHeader) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(HEADER_MAGIC);
    out.push(VERSION);
    out.push(0);
    put_varint(header.window_bytes as u64, &mut out);
    put_varint(header.step_bytes as u64, &mut out);
    put_varint(header.boundary_delta as u64, &mut out);
    out.push(header.map_max_depth);
    out.push(header.map_depth_shift);
    put_varint(header.newline_margin_add, &mut out);
    put_varint(header.space_to_newline_margin_add, &mut out);
    put_varint(header.newline_share_ppm_min as u64, &mut out);
    put_varint(header.newline_override_budget as u64, &mut out);
    put_varint(header.newline_demote_keep_ppm_min as u64, &mut out);
    put_varint(header.newline_demote_keep_min as u64, &mut out);
    out.push(if header.newline_only_from_spacelike { 1 } else { 0 });
    out
}

fn encode_law_dictionary(laws: &[FrozenLawSummary]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(LAW_MAGIC);
    out.push(VERSION);
    out.push(0);
    put_varint(laws.len() as u64, &mut out);
    for law in laws {
        put_varint(law.law.boundary_band as u64, &mut out);
        put_varint(law.law.field_margin, &mut out);
        put_varint(law.law.newline_demote_margin, &mut out);
    }
    out
}

fn encode_window_path(
    input_bytes: usize,
    windows: &[FrozenWindowRow],
    law_index_by_id: &BTreeMap<String, usize>,
) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(WINDOW_PATH_MAGIC);
    out.push(VERSION);
    out.push(0);
    put_varint(input_bytes as u64, &mut out);
    put_varint(windows.len() as u64, &mut out);
    for row in windows {
        let law_idx = *law_index_by_id.get(&row.law_id).unwrap_or(&0);
        put_varint(law_idx as u64, &mut out);
        put_varint(row.span_bytes as u64, &mut out);
    }
    out
}

fn encode_segment_path(
    input_bytes: usize,
    segments: &[SegmentRow],
    law_index_by_id: &BTreeMap<String, usize>,
) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(SEGMENT_PATH_MAGIC);
    out.push(VERSION);
    out.push(0);
    put_varint(input_bytes as u64, &mut out);
    put_varint(segments.len() as u64, &mut out);
    let mut prev_end = 0usize;
    for seg in segments {
        let law_idx = *law_index_by_id.get(&seg.law_id).unwrap_or(&0);
        put_varint(law_idx as u64, &mut out);
        put_varint(seg.start.saturating_sub(prev_end) as u64, &mut out);
        put_varint(seg.span_bytes as u64, &mut out);
        prev_end = seg.end;
    }
    out
}

fn parse_best_line(stderr: &[u8]) -> Result<FrozenEvalRow> {
    let stderr = String::from_utf8_lossy(stderr);
    let prefix = "apextrace apex-map-lane best:";
    let line = stderr
        .lines()
        .rev()
        .find(|line| line.trim_start().starts_with(prefix))
        .ok_or_else(|| anyhow!("missing best line in apex-map-lane stderr"))?;
    let payload = line
        .trim_start()
        .strip_prefix(prefix)
        .ok_or_else(|| anyhow!("apex-map-lane best line missing prefix"))?
        .trim();

    let map = tokenize_kv_line(payload);
    let search = SearchKnobTuple {
        chunk_bytes: parse_required_usize(&map, "chunk_bytes")?,
        chunk_search_objective: parse_required_string(&map, "chunk_search_objective")?,
        chunk_raw_slack: parse_required_u64(&map, "chunk_raw_slack")?,
    };
    let law = ReplayLawTuple {
        boundary_band: parse_required_usize(&map, "boundary_band")?,
        field_margin: parse_required_u64(&map, "field_margin")?,
        newline_demote_margin: parse_required_u64(&map, "newline_demote_margin")?,
    };

    Ok(FrozenEvalRow {
        law,
        search,
        compact_field_total_payload_exact: parse_required_usize(
            &map,
            "compact_field_total_payload_exact",
        )?,
        field_patch_bytes: parse_required_usize(&map, "field_patch_bytes")?,
        field_match_pct: parse_required_f64(&map, "field_match_pct")?,
        field_match_vs_majority_pct: parse_required_f64(&map, "field_match_vs_majority_pct")?,
        field_balanced_accuracy_pct: parse_required_f64(&map, "field_balanced_accuracy_pct")?,
        field_macro_f1_pct: parse_required_f64(&map, "field_macro_f1_pct")?,
        field_f1_newline_pct: parse_required_f64(&map, "field_f1_newline_pct")?,
        field_pred_dominant_label: parse_required_string(&map, "field_pred_dominant_label")?,
        field_pred_dominant_share_pct: parse_required_f64(
            &map,
            "field_pred_dominant_share_pct",
        )?,
        field_pred_collapse_90_flag: parse_required_bool(&map, "field_pred_collapse_90_flag")?,
        field_pred_newline_delta: parse_required_i64(&map, "field_pred_newline_delta")?,
        field_newline_demoted: parse_required_usize(&map, "field_newline_demoted")?,
        field_newline_after_demote: parse_required_usize(&map, "field_newline_after_demote")?,
        field_newline_floor_used: parse_required_usize(&map, "field_newline_floor_used")?,
        field_newline_extinct_flag: parse_required_bool(&map, "field_newline_extinct_flag")?,
    })
}

fn render_txt(
    summary: &FreezeSummary,
    file_summaries: &[FrozenFileSummary],
    window_deltas: &[WindowDeltaRow],
    top_rows: usize,
) -> String {
    let mut out = String::new();
    push_line(&mut out, "recipe", &summary.recipe);
    push_line(&mut out, "file_count", summary.file_count);
    push_line(&mut out, "honest_file_count", summary.honest_file_count);
    push_line(&mut out, "union_law_count", summary.union_law_count);
    push_line(&mut out, "target_global_law_id", &summary.target_global_law_id);
    push_line(
        &mut out,
        "target_global_law_path_hits",
        summary.target_global_law_path_hits,
    );
    push_line(
        &mut out,
        "target_global_law_file_count",
        summary.target_global_law_file_count,
    );
    push_line(
        &mut out,
        "target_global_law_total_window_count",
        summary.target_global_law_total_window_count,
    );
    push_line(
        &mut out,
        "target_global_law_total_segment_count",
        summary.target_global_law_total_segment_count,
    );
    push_line(
        &mut out,
        "target_global_law_total_covered_bytes",
        summary.target_global_law_total_covered_bytes,
    );
    push_line(
        &mut out,
        "target_global_law_dominant_knob_signature",
        summary.target_global_law_dominant_knob_signature.replace(' ', "|"),
    );
    push_line(&mut out, "bridge_chunk_candidates", &summary.bridge_chunk_candidates);
    push_line(
        &mut out,
        "bridge_chunk_candidate_count",
        summary.bridge_chunk_candidate_count,
    );
    push_line(
        &mut out,
        "bridge_chunk_candidate_source",
        &summary.bridge_chunk_candidate_source,
    );
    push_line(&mut out, "freeze_chunk_bytes", summary.freeze_chunk_bytes);
    push_line(
        &mut out,
        "freeze_chunk_search_objective",
        &summary.freeze_chunk_search_objective,
    );
    push_line(
        &mut out,
        "freeze_chunk_raw_slack",
        summary.freeze_chunk_raw_slack,
    );
    push_line(&mut out, "freeze_boundary_band", summary.freeze_boundary_band);
    push_line(&mut out, "freeze_field_margin", summary.freeze_field_margin);
    push_line(
        &mut out,
        "freeze_newline_demote_margin",
        summary.freeze_newline_demote_margin,
    );
    push_line(
        &mut out,
        "searched_total_piecewise_payload_exact",
        summary.searched_total_piecewise_payload_exact,
    );
    push_line(
        &mut out,
        "frozen_total_piecewise_payload_exact",
        summary.frozen_total_piecewise_payload_exact,
    );
    push_line(
        &mut out,
        "delta_total_piecewise_payload_exact",
        summary.delta_total_piecewise_payload_exact,
    );
    push_line(&mut out, "all_window_count", summary.all_window_count);
    push_line(&mut out, "target_window_count", summary.target_window_count);
    push_line(
        &mut out,
        "searched_target_window_payload_exact",
        summary.searched_target_window_payload_exact,
    );
    push_line(
        &mut out,
        "frozen_target_window_payload_exact",
        summary.frozen_target_window_payload_exact,
    );
    push_line(
        &mut out,
        "delta_target_window_payload_exact",
        summary.delta_target_window_payload_exact,
    );
    push_line(&mut out, "bridged_window_count", summary.bridged_window_count);
    push_line(&mut out, "bridge_candidate_count", summary.bridge_candidate_count);
    push_line(&mut out, "bridge_accepted_count", summary.bridge_accepted_count);
    push_line(&mut out, "bridge_local_penalty_exact", summary.bridge_local_penalty_exact);
    push_line(&mut out, "bridge_total_gain_exact", summary.bridge_total_gain_exact);
    push_line(&mut out, "improved_window_count", summary.improved_window_count);
    push_line(&mut out, "equal_window_count", summary.equal_window_count);
    push_line(&mut out, "worsened_window_count", summary.worsened_window_count);
    push_line(
        &mut out,
        "improved_target_window_count",
        summary.improved_target_window_count,
    );
    push_line(
        &mut out,
        "equal_target_window_count",
        summary.equal_target_window_count,
    );
    push_line(
        &mut out,
        "worsened_target_window_count",
        summary.worsened_target_window_count,
    );
    push_line(&mut out, "best_gain_input", &summary.best_gain_input);
    push_line(&mut out, "best_gain_window_idx", summary.best_gain_window_idx);
    push_line(
        &mut out,
        "best_gain_delta_payload_exact",
        summary.best_gain_delta_payload_exact,
    );
    push_line(&mut out, "worst_loss_input", &summary.worst_loss_input);
    push_line(&mut out, "worst_loss_window_idx", summary.worst_loss_window_idx);
    push_line(
        &mut out,
        "worst_loss_delta_payload_exact",
        summary.worst_loss_delta_payload_exact,
    );

    out.push_str("\n--- files ---\n");
    for file in file_summaries {
        out.push_str(&format!(
            "input={} searched_total_piecewise_payload_exact={} frozen_total_piecewise_payload_exact={} delta_total_piecewise_payload_exact={} frozen_local_compact_payload_bytes_exact={} frozen_shared_header_bytes_exact={} frozen_law_dictionary_bytes_exact={} frozen_window_path_bytes_exact={} frozen_segment_path_bytes_exact={} frozen_selected_path_mode={} frozen_selected_path_bytes_exact={} all_window_count={} target_window_count={} searched_target_window_payload_exact={} frozen_target_window_payload_exact={} delta_target_window_payload_exact={} bridged_window_count={} bridge_candidate_count={} bridge_accepted_count={} bridge_local_penalty_exact={} bridge_total_gain_exact={} improved_window_count={} equal_window_count={} worsened_window_count={} improved_target_window_count={} equal_target_window_count={} worsened_target_window_count={}\n",
            file.input,
            file.searched_total_piecewise_payload_exact,
            file.frozen_total_piecewise_payload_exact,
            file.delta_total_piecewise_payload_exact,
            file.frozen_local_compact_payload_bytes_exact,
            file.frozen_shared_header_bytes_exact,
            file.frozen_law_dictionary_bytes_exact,
            file.frozen_window_path_bytes_exact,
            file.frozen_segment_path_bytes_exact,
            file.frozen_selected_path_mode,
            file.frozen_selected_path_bytes_exact,
            file.all_window_count,
            file.target_window_count,
            file.searched_target_window_payload_exact,
            file.frozen_target_window_payload_exact,
            file.delta_target_window_payload_exact,
            file.bridged_window_count,
            file.bridge_candidate_count,
            file.bridge_accepted_count,
            file.bridge_local_penalty_exact,
            file.bridge_total_gain_exact,
            file.improved_window_count,
            file.equal_window_count,
            file.worsened_window_count,
            file.improved_target_window_count,
            file.equal_target_window_count,
            file.worsened_target_window_count,
        ));
    }

    let mut losses = window_deltas.to_vec();
    losses.sort_by_key(|row| std::cmp::Reverse(row.delta_payload_exact));
    let mut gains = window_deltas.to_vec();
    gains.sort_by_key(|row| row.delta_payload_exact);

    out.push_str("\n--- top-losses ---\n");
    for row in losses.into_iter().filter(|row| row.delta_payload_exact > 0).take(top_rows) {
        out.push_str(&format!(
            "input={} window_idx={} start={} end={} span_bytes={} searched_global_law_id={} searched_payload_exact={} frozen_payload_exact={} delta_payload_exact={} searched_match_pct={:.6} frozen_match_pct={:.6} searched_chunk_bytes={} frozen_chunk_bytes={} searched_chunk_search_objective={} frozen_chunk_search_objective={} searched_chunk_raw_slack={} frozen_chunk_raw_slack={} searched_boundary_band={} frozen_boundary_band={} searched_field_margin={} frozen_field_margin={} searched_newline_demote_margin={} frozen_newline_demote_margin={}\n",
            row.input,
            row.window_idx,
            row.start,
            row.end,
            row.span_bytes,
            row.searched_global_law_id,
            row.searched_payload_exact,
            row.frozen_payload_exact,
            row.delta_payload_exact,
            row.searched_match_pct,
            row.frozen_match_pct,
            row.searched_chunk_bytes,
            row.frozen_chunk_bytes,
            row.searched_chunk_search_objective,
            row.frozen_chunk_search_objective,
            row.searched_chunk_raw_slack,
            row.frozen_chunk_raw_slack,
            row.searched_boundary_band,
            row.frozen_boundary_band,
            row.searched_field_margin,
            row.frozen_field_margin,
            row.searched_newline_demote_margin,
            row.frozen_newline_demote_margin,
        ));
    }

    out.push_str("\n--- top-gains ---\n");
    for row in gains.into_iter().filter(|row| row.delta_payload_exact < 0).take(top_rows) {
        out.push_str(&format!(
            "input={} window_idx={} start={} end={} span_bytes={} searched_global_law_id={} searched_payload_exact={} frozen_payload_exact={} delta_payload_exact={} searched_match_pct={:.6} frozen_match_pct={:.6} searched_chunk_bytes={} frozen_chunk_bytes={} searched_chunk_search_objective={} frozen_chunk_search_objective={} searched_chunk_raw_slack={} frozen_chunk_raw_slack={} searched_boundary_band={} frozen_boundary_band={} searched_field_margin={} frozen_field_margin={} searched_newline_demote_margin={} frozen_newline_demote_margin={}\n",
            row.input,
            row.window_idx,
            row.start,
            row.end,
            row.span_bytes,
            row.searched_global_law_id,
            row.searched_payload_exact,
            row.frozen_payload_exact,
            row.delta_payload_exact,
            row.searched_match_pct,
            row.frozen_match_pct,
            row.searched_chunk_bytes,
            row.frozen_chunk_bytes,
            row.searched_chunk_search_objective,
            row.frozen_chunk_search_objective,
            row.searched_chunk_raw_slack,
            row.frozen_chunk_raw_slack,
            row.searched_boundary_band,
            row.frozen_boundary_band,
            row.searched_field_margin,
            row.frozen_field_margin,
            row.searched_newline_demote_margin,
            row.frozen_newline_demote_margin,
        ));
    }

    out
}

fn render_csv(
    summary: &FreezeSummary,
    file_summaries: &[FrozenFileSummary],
    window_deltas: &[WindowDeltaRow],
) -> String {
    let mut out = String::new();
    push_csv_row(
        &mut out,
        &[
            "row_kind",
            "id",
            "input",
            "window_idx",
            "searched_global_law_id",
            "searched_payload_exact",
            "frozen_payload_exact",
            "delta_payload_exact",
            "searched_match_pct",
            "frozen_match_pct",
            "freeze_chunk_bytes",
            "freeze_chunk_search_objective",
            "freeze_chunk_raw_slack",
            "freeze_boundary_band",
            "freeze_field_margin",
            "freeze_newline_demote_margin",
            "searched_total_piecewise_payload_exact",
            "frozen_total_piecewise_payload_exact",
            "delta_total_piecewise_payload_exact",
            "target_window_count",
            "searched_target_window_payload_exact",
            "frozen_target_window_payload_exact",
            "delta_target_window_payload_exact",
            "improved_window_count",
            "equal_window_count",
            "worsened_window_count",
            "improved_target_window_count",
            "equal_target_window_count",
            "worsened_target_window_count",
        ],
    );

    push_csv_row(
        &mut out,
        &[
            "summary",
            &summary.target_global_law_id,
            "",
            "",
            &summary.target_global_law_id,
            "",
            "",
            "",
            "",
            "",
            &summary.freeze_chunk_bytes.to_string(),
            &summary.freeze_chunk_search_objective,
            &summary.freeze_chunk_raw_slack.to_string(),
            &summary.freeze_boundary_band.to_string(),
            &summary.freeze_field_margin.to_string(),
            &summary.freeze_newline_demote_margin.to_string(),
            &summary.searched_total_piecewise_payload_exact.to_string(),
            &summary.frozen_total_piecewise_payload_exact.to_string(),
            &summary.delta_total_piecewise_payload_exact.to_string(),
            &summary.target_window_count.to_string(),
            &summary.searched_target_window_payload_exact.to_string(),
            &summary.frozen_target_window_payload_exact.to_string(),
            &summary.delta_target_window_payload_exact.to_string(),
            &summary.improved_window_count.to_string(),
            &summary.equal_window_count.to_string(),
            &summary.worsened_window_count.to_string(),
            &summary.improved_target_window_count.to_string(),
            &summary.equal_target_window_count.to_string(),
            &summary.worsened_target_window_count.to_string(),
        ],
    );

    for file in file_summaries {
        push_csv_row(
            &mut out,
            &[
                "file",
                &file.input,
                &file.input,
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                &file.searched_total_piecewise_payload_exact.to_string(),
                &file.frozen_total_piecewise_payload_exact.to_string(),
                &file.delta_total_piecewise_payload_exact.to_string(),
                &file.target_window_count.to_string(),
                &file.searched_target_window_payload_exact.to_string(),
                &file.frozen_target_window_payload_exact.to_string(),
                &file.delta_target_window_payload_exact.to_string(),
                &file.improved_window_count.to_string(),
                &file.equal_window_count.to_string(),
                &file.worsened_window_count.to_string(),
                &file.improved_target_window_count.to_string(),
                &file.equal_target_window_count.to_string(),
                &file.worsened_target_window_count.to_string(),
            ],
        );
    }

    for row in window_deltas {
        push_csv_row(
            &mut out,
            &[
                "window",
                &format!("{}:{}", row.input, row.window_idx),
                &row.input,
                &row.window_idx.to_string(),
                &row.searched_global_law_id,
                &row.searched_payload_exact.to_string(),
                &row.frozen_payload_exact.to_string(),
                &row.delta_payload_exact.to_string(),
                &format!("{:.6}", row.searched_match_pct),
                &format!("{:.6}", row.frozen_match_pct),
                &row.frozen_chunk_bytes.to_string(),
                &row.frozen_chunk_search_objective,
                &row.frozen_chunk_raw_slack.to_string(),
                &row.frozen_boundary_band.to_string(),
                &row.frozen_field_margin.to_string(),
                &row.frozen_newline_demote_margin.to_string(),
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
            ],
        );
    }

    out
}

fn push_line(out: &mut String, key: &str, value: impl ToString) {
    out.push_str(key);
    out.push('=');
    out.push_str(&value.to_string());
    out.push('\n');
}

fn push_csv_row(out: &mut String, cells: &[&str]) {
    let escaped = cells.iter().map(|s| csv_escape(s)).collect::<Vec<_>>();
    out.push_str(&escaped.join(","));
    out.push('\n');
}

fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

fn parse_required_string(map: &BTreeMap<String, String>, key: &str) -> Result<String> {
    map.get(key)
        .cloned()
        .ok_or_else(|| anyhow!("missing key {}", key))
}

fn parse_required_usize(map: &BTreeMap<String, String>, key: &str) -> Result<usize> {
    let raw = parse_required_string(map, key)?;
    raw.parse::<usize>()
        .with_context(|| format!("parse usize key {} from {}", key, raw))
}

fn parse_required_u64(map: &BTreeMap<String, String>, key: &str) -> Result<u64> {
    let raw = parse_required_string(map, key)?;
    raw.parse::<u64>()
        .with_context(|| format!("parse u64 key {} from {}", key, raw))
}

fn parse_required_i64(map: &BTreeMap<String, String>, key: &str) -> Result<i64> {
    let raw = parse_required_string(map, key)?;
    raw.parse::<i64>()
        .with_context(|| format!("parse i64 key {} from {}", key, raw))
}

fn parse_required_f64(map: &BTreeMap<String, String>, key: &str) -> Result<f64> {
    let raw = parse_required_string(map, key)?;
    raw.parse::<f64>()
        .with_context(|| format!("parse f64 key {} from {}", key, raw))
}

fn parse_required_bool(map: &BTreeMap<String, String>, key: &str) -> Result<bool> {
    let raw = parse_required_string(map, key)?;
    raw.parse::<bool>()
        .with_context(|| format!("parse bool key {} from {}", key, raw))
}

fn chunk_search_objective_name(value: ChunkSearchObjective) -> &'static str {
    match value {
        ChunkSearchObjective::Raw => "raw",
        ChunkSearchObjective::RawGuarded => "raw-guarded",
        ChunkSearchObjective::Honest => "honest",
        ChunkSearchObjective::Newline => "newline",
    }
}

fn join_usize_csv(values: &[usize]) -> String {
    values
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn truncate_for_error(s: &str) -> String {
    const LIMIT: usize = 600;
    if s.len() <= LIMIT {
        s.to_string()
    } else {
        format!("{}...", &s[..LIMIT])
    }
}

fn sanitize_file_stem(path: &str) -> String {
    path.chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}

fn make_temp_dir(prefix: &str) -> Result<PathBuf> {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time before unix epoch")?
        .as_nanos();
    let dir = env::temp_dir().join(format!("{}_{}_{}", prefix, std::process::id(), stamp));
    fs::create_dir_all(&dir).with_context(|| format!("create temp dir {}", dir.display()))?;
    Ok(dir)
}

fn put_varint(mut v: u64, out: &mut Vec<u8>) {
    loop {
        let byte = (v & 0x7F) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            break;
        } else {
            out.push(byte | 0x80);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_freeze_config, build_profiles, build_shared_law_ids, parse_knob_signature,
        parse_manifest_txt, select_chunk_candidates, ReplayLawTuple,
    };
    use crate::cmd::apextrace::{ApexLaneLawBridgeFreezeArgs, ChunkSearchObjective, RenderFormat};

    fn sample_manifest(input: &str, law_path: &str, laws: &[(&str, usize, u64, u64)]) -> String {
        let mut out = String::new();
        out.push_str(&format!("input={}\n", input));
        out.push_str("recipe=configs/tuned_validated.k8r\n");
        out.push_str("input_bytes=3072\n");
        out.push_str("window_bytes=256\n");
        out.push_str("step_bytes=256\n");
        out.push_str("windows_analyzed=3\n");
        out.push_str("total_window_span_bytes=768\n");
        out.push_str("coverage_bytes=768\n");
        out.push_str("overlap_bytes=0\n");
        out.push_str("honest_non_overlapping=true\n");
        out.push_str("allow_overlap_scout=false\n");
        out.push_str(&format!("distinct_law_count={}\n", laws.len()));
        out.push_str("segment_count=2\n");
        out.push_str("law_switch_count=1\n");
        out.push_str("total_elapsed_ms=1\n");
        out.push_str("boundary_delta=1\n");
        out.push_str("map_max_depth=0\n");
        out.push_str("map_depth_shift=1\n");
        out.push_str("newline_margin_add=96\n");
        out.push_str("space_to_newline_margin_add=64\n");
        out.push_str("newline_share_ppm_min=550000\n");
        out.push_str("newline_override_budget=0\n");
        out.push_str("newline_demote_keep_ppm_min=150000\n");
        out.push_str("newline_demote_keep_min=1\n");
        out.push_str("newline_only_from_spacelike=true\n");
        out.push_str("local_compact_payload_bytes_exact=530\n");
        out.push_str("shared_header_bytes_exact=24\n");
        out.push_str("law_dictionary_bytes_exact=13\n");
        out.push_str("window_path_bytes_exact=18\n");
        out.push_str("segment_path_bytes_exact=14\n");
        out.push_str("selected_path_mode=segment\n");
        out.push_str("selected_path_bytes_exact=14\n");
        out.push_str("total_piecewise_payload_exact=567\n");
        out.push_str(&format!("law_path={}\n", law_path));
        out.push_str("\n--- laws ---\n");
        for (id, band, margin, demote) in laws {
            out.push_str(&format!(
                "law_id={} boundary_band={} field_margin={} newline_demote_margin={} window_count=2 segment_count=1 covered_bytes=512 mean_compact_field_total_payload_exact=176.500 mean_field_match_pct=70.000000 mean_field_match_vs_majority_pct=-20.000000 mean_field_balanced_accuracy_pct=50.000000 mean_field_macro_f1_pct=45.000000 mean_field_f1_newline_pct=10.000000\n",
                id, band, margin, demote,
            ));
        }
        out.push_str("\n--- segments ---\nsegment_idx=0 law_id=L0 start=0 end=512 span_bytes=512 window_count=2 first_window_idx=0 last_window_idx=1 mean_compact_field_total_payload_exact=176.500 mean_field_match_pct=70.000000 mean_field_balanced_accuracy_pct=50.000000 mean_field_macro_f1_pct=45.000000 mean_field_f1_newline_pct=10.000000\n");
        out.push_str("\n--- windows ---\n");
        out.push_str("window_idx=0 law_id=L0 start=0 end=256 span_bytes=256 chunk_bytes=64 boundary_band=12 field_margin=4 newline_demote_margin=4 chunk_search_objective=raw chunk_raw_slack=1 compact_field_total_payload_exact=177 field_patch_bytes=90 field_match_pct=70.312500 majority_baseline_match_pct=89.000000 field_match_vs_majority_pct=-18.687500 field_balanced_accuracy_pct=50.000000 field_macro_f1_pct=45.000000 field_f1_newline_pct=10.000000 field_pred_dominant_label=space field_pred_dominant_share_pct=90.000000 field_pred_collapse_90_flag=true field_pred_newline_delta=-3 field_newline_demoted=1 field_newline_after_demote=5 field_newline_floor_used=1 field_newline_extinct_flag=false\n");
        out.push_str("window_idx=1 law_id=L0 start=256 end=512 span_bytes=256 chunk_bytes=64 boundary_band=12 field_margin=4 newline_demote_margin=4 chunk_search_objective=raw chunk_raw_slack=1 compact_field_total_payload_exact=176 field_patch_bytes=88 field_match_pct=71.000000 majority_baseline_match_pct=89.000000 field_match_vs_majority_pct=-18.000000 field_balanced_accuracy_pct=50.000000 field_macro_f1_pct=45.000000 field_f1_newline_pct=10.000000 field_pred_dominant_label=space field_pred_dominant_share_pct=90.000000 field_pred_collapse_90_flag=true field_pred_newline_delta=-3 field_newline_demoted=1 field_newline_after_demote=5 field_newline_floor_used=1 field_newline_extinct_flag=false\n");
        out.push_str("window_idx=2 law_id=L1 start=512 end=768 span_bytes=256 chunk_bytes=32 boundary_band=8 field_margin=4 newline_demote_margin=4 chunk_search_objective=raw chunk_raw_slack=1 compact_field_total_payload_exact=180 field_patch_bytes=96 field_match_pct=68.000000 majority_baseline_match_pct=89.000000 field_match_vs_majority_pct=-21.000000 field_balanced_accuracy_pct=50.000000 field_macro_f1_pct=45.000000 field_f1_newline_pct=10.000000 field_pred_dominant_label=space field_pred_dominant_share_pct=90.000000 field_pred_collapse_90_flag=true field_pred_newline_delta=-3 field_newline_demoted=1 field_newline_after_demote=5 field_newline_floor_used=1 field_newline_extinct_flag=false\n");
        out
    }

    fn sample_args() -> ApexLaneLawBridgeFreezeArgs {
        ApexLaneLawBridgeFreezeArgs {
            recipe: "configs/tuned_validated.k8r".to_string(),
            inputs: vec!["text/Genesis1.txt".to_string(), "text/Genesis2.txt".to_string()],
            max_ticks: 20_000_000,
            window_bytes: 256,
            step_bytes: 256,
            max_windows: 12,
            seed_from: 0,
            seed_count: 64,
            seed_step: 1,
            recipe_seed: 1,
            chunk_sweep: "32,64".to_string(),
            chunk_search_objective: ChunkSearchObjective::Raw,
            chunk_raw_slack: 1,
            map_max_depth: 0,
            map_depth_shift: 1,
            boundary_band_sweep: "8,12".to_string(),
            boundary_delta: 1,
            field_margin_sweep: "4,8".to_string(),
            newline_margin_add: 96,
            space_to_newline_margin_add: 64,
            newline_share_ppm_min: 550_000,
            newline_override_budget: 0,
            newline_demote_margin_sweep: "0,4".to_string(),
            newline_demote_keep_ppm_min: 150_000,
            newline_demote_keep_min: 1,
            newline_only_from_spacelike: true,
            merge_gap_bytes: 0,
            allow_overlap_scout: false,
            freeze_boundary_band: None,
            freeze_field_margin: None,
            freeze_newline_demote_margin: None,
            bridge_chunk_sweep: None,
            bridge_chunk_search_objective: None,
            bridge_chunk_raw_slack: None,
            bridge_max_windows: 2,
            bridge_max_span_bytes: 512,
            bridge_max_local_penalty_exact: 8,
            bridge_min_total_gain_exact: 1,
            global_law_id: None,
            top_rows: 4,
            keep_temp_dir: false,
            format: RenderFormat::Txt,
            out: None,
        }
    }

    #[test]
    fn parse_manifest_extracts_window_positions() {
        let raw = sample_manifest(
            "text/Genesis1.txt",
            "L0,L0,L1",
            &[("L0", 12, 4, 4), ("L1", 8, 4, 4)],
        );
        let report = parse_manifest_txt(&raw).expect("parse manifest txt");
        assert_eq!(report.windows_analyzed, 3);
        assert_eq!(report.windows[0].start, 0);
        assert_eq!(report.windows[2].end, 768);
    }

    #[test]
    fn parse_knob_signature_extracts_search_tuple() {
        let sig = parse_knob_signature(
            "chunk_bytes=64 chunk_search_objective=raw chunk_raw_slack=1",
        )
        .expect("parse knob signature");
        assert_eq!(sig.chunk_bytes, 64);
        assert_eq!(sig.chunk_search_objective, "raw");
        assert_eq!(sig.chunk_raw_slack, 1);
    }

    #[test]
    fn build_freeze_config_defaults_to_dominant_profile() {
        let a = parse_manifest_txt(&sample_manifest(
            "text/Genesis1.txt",
            "L0,L0,L1",
            &[("L0", 12, 4, 4), ("L1", 8, 4, 4)],
        ))
        .expect("parse A");
        let b = parse_manifest_txt(&sample_manifest(
            "text/Genesis2.txt",
            "L0,L0,L0",
            &[("L0", 12, 4, 4)],
        ))
        .expect("parse B");
        let ids = build_shared_law_ids(&[a.clone(), b.clone()]);
        let profiles = build_profiles(&[a.clone(), b.clone()], &ids);
        let g1 = profiles
            .iter()
            .find(|p| {
                p.law
                    == ReplayLawTuple {
                        boundary_band: 12,
                        field_margin: 4,
                        newline_demote_margin: 4,
                    }
            })
            .expect("find dominant law");
        let (chunk_candidates, chunk_candidate_source) =
            select_chunk_candidates(&sample_args(), &[a, b], &ids, &g1.global_law_id, 64)
                .expect("select chunk candidates");
        let freeze = build_freeze_config(&sample_args(), g1, chunk_candidates, chunk_candidate_source)
            .expect("freeze config");
        assert_eq!(freeze.global_law_id, g1.global_law_id);
        assert_eq!(freeze.law, g1.law);
        assert_eq!(freeze.search.chunk_bytes, 64);
        assert_eq!(freeze.search.chunk_search_objective, "raw");
        assert_eq!(freeze.search.chunk_raw_slack, 1);
    }
}
