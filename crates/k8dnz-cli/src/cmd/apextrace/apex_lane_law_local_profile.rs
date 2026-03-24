use anyhow::{anyhow, Context, Result};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::cmd::apextrace::{
    ApexLaneLawLocalProfileArgs, ChunkSearchObjective, RenderFormat,
};

use super::common::write_or_print;

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
struct FileReport {
    input: String,
    recipe: String,
    windows_analyzed: usize,
    honest_non_overlapping: bool,
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
struct ParsedKnobSignature {
    chunk_bytes: usize,
    chunk_search_objective: String,
    chunk_raw_slack: u64,
}

#[derive(Clone, Debug)]
struct FrozenEvalRow {
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
struct CandidateEvalRow {
    input: String,
    window_idx: usize,
    candidate_chunk_bytes: usize,
    searched_payload_exact: usize,
    candidate_payload_exact: usize,
    delta_payload_exact: i64,
    searched_patch_bytes: usize,
    candidate_patch_bytes: usize,
    delta_patch_bytes: i64,
    searched_match_pct: f64,
    candidate_match_pct: f64,
    searched_match_vs_majority_pct: f64,
    candidate_match_vs_majority_pct: f64,
    searched_balanced_accuracy_pct: f64,
    candidate_balanced_accuracy_pct: f64,
    searched_macro_f1_pct: f64,
    candidate_macro_f1_pct: f64,
    searched_f1_newline_pct: f64,
    candidate_f1_newline_pct: f64,
    candidate_pred_dominant_label: String,
    candidate_pred_dominant_share_pct: f64,
    candidate_collapse_90_flag: bool,
    candidate_newline_delta: i64,
    candidate_newline_demoted: usize,
    candidate_newline_after_demote: usize,
    candidate_newline_floor_used: usize,
    candidate_newline_extinct_flag: bool,
}

#[derive(Clone, Debug)]
struct WindowBestRow {
    input: String,
    window_idx: usize,
    start: usize,
    end: usize,
    span_bytes: usize,
    searched_local_law_id: String,
    searched_global_law_id: String,
    searched_chunk_bytes: usize,
    searched_chunk_search_objective: String,
    searched_chunk_raw_slack: u64,
    searched_payload_exact: usize,
    searched_patch_bytes: usize,
    searched_match_pct: f64,
    searched_match_vs_majority_pct: f64,
    searched_balanced_accuracy_pct: f64,
    searched_macro_f1_pct: f64,
    searched_f1_newline_pct: f64,
    best_chunk_bytes: usize,
    best_payload_exact: usize,
    best_patch_bytes: usize,
    delta_payload_exact: i64,
    delta_patch_bytes: i64,
    best_match_pct: f64,
    best_match_vs_majority_pct: f64,
    best_balanced_accuracy_pct: f64,
    best_macro_f1_pct: f64,
    best_f1_newline_pct: f64,
    best_pred_dominant_label: String,
    best_pred_dominant_share_pct: f64,
    best_collapse_90_flag: bool,
    best_newline_delta: i64,
    best_newline_demoted: usize,
    best_newline_after_demote: usize,
    best_newline_floor_used: usize,
    best_newline_extinct_flag: bool,
}

#[derive(Clone, Debug)]
struct CandidateSummary {
    chunk_bytes: usize,
    eval_window_count: usize,
    best_for_window_count: usize,
    improved_window_count: usize,
    equal_window_count: usize,
    worsened_window_count: usize,
    total_delta_payload_exact: i64,
    total_delta_patch_bytes: i64,
    mean_payload_exact: f64,
    mean_patch_bytes: f64,
    mean_delta_payload_exact: f64,
    mean_delta_patch_bytes: f64,
    mean_match_pct: f64,
    mean_match_vs_majority_pct: f64,
    mean_balanced_accuracy_pct: f64,
    mean_macro_f1_pct: f64,
    mean_f1_newline_pct: f64,
    mean_pred_dominant_share_pct: f64,
    collapse_90_count: usize,
    newline_extinct_count: usize,
    mean_abs_newline_delta: f64,
    projected_total_piecewise_payload_exact: isize,
}

#[derive(Clone, Debug)]
struct FileSummary {
    input: String,
    searched_total_piecewise_payload_exact: usize,
    projected_total_piecewise_payload_exact: isize,
    delta_total_piecewise_payload_exact: i64,
    target_window_count: usize,
    searched_target_window_payload_exact: usize,
    best_mix_target_window_payload_exact: usize,
    delta_best_mix_target_window_payload_exact: i64,
    improved_target_window_count: usize,
    equal_target_window_count: usize,
    worsened_target_window_count: usize,
}

#[derive(Clone, Debug)]
struct LocalProfileSummary {
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
    target_global_law_weighted_mean_compact_field_total_payload_exact: f64,
    target_global_law_dominant_knob_signature: String,
    eval_boundary_band: usize,
    eval_field_margin: u64,
    eval_newline_demote_margin: u64,
    eval_chunk_search_objective: String,
    eval_chunk_raw_slack: u64,
    eval_chunk_candidates: String,
    eval_chunk_candidate_count: usize,
    searched_total_piecewise_payload_exact: usize,
    projected_best_mix_total_piecewise_payload_exact: isize,
    delta_best_mix_total_piecewise_payload_exact: i64,
    target_window_count: usize,
    searched_target_window_payload_exact: usize,
    best_mix_target_window_payload_exact: usize,
    delta_best_mix_target_window_payload_exact: i64,
    improved_target_window_count: usize,
    equal_target_window_count: usize,
    worsened_target_window_count: usize,
    dominant_best_chunk_bytes: usize,
    dominant_best_chunk_count: usize,
    best_gain_input: String,
    best_gain_window_idx: usize,
    best_gain_delta_payload_exact: i64,
    worst_loss_input: String,
    worst_loss_window_idx: usize,
    worst_loss_delta_payload_exact: i64,
}

pub fn run_apex_lane_law_local_profile(args: ApexLaneLawLocalProfileArgs) -> Result<()> {
    if args.inputs.is_empty() {
        return Err(anyhow!(
            "apex-lane-law-local-profile requires at least one --in input"
        ));
    }

    let exe = env::current_exe().context("resolve current executable for apex-lane-law-local-profile")?;
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
    let dominant = parse_knob_signature(&target_profile.dominant_knob_signature)
        .with_context(|| {
            format!(
                "parse dominant knob signature {} for {}",
                target_profile.dominant_knob_signature, target_profile.global_law_id
            )
        })?;
    let chunk_candidates = select_chunk_candidates(&args, &reports, &shared_law_ids, &target_profile, dominant.chunk_bytes)?;
    let eval_objective = args
        .local_chunk_search_objective
        .map(chunk_search_objective_name)
        .unwrap_or(dominant.chunk_search_objective.as_str())
        .to_string();
    let eval_raw_slack = args.local_chunk_raw_slack.unwrap_or(dominant.chunk_raw_slack);
    let eval_boundary_band = args
        .freeze_boundary_band
        .unwrap_or(target_profile.law.boundary_band);
    let eval_field_margin = args
        .freeze_field_margin
        .unwrap_or(target_profile.law.field_margin);
    let eval_newline_demote_margin = args
        .freeze_newline_demote_margin
        .unwrap_or(target_profile.law.newline_demote_margin);

    let temp_dir = make_temp_dir("apex_lane_law_local_profile")?;
    let mut candidate_rows = Vec::<CandidateEvalRow>::new();
    let mut best_rows = Vec::<WindowBestRow>::new();
    let mut file_summaries = Vec::<FileSummary>::new();
    let mut per_candidate_acc = BTreeMap::<usize, Vec<CandidateEvalRow>>::new();
    let mut best_chunk_counts = BTreeMap::<usize, usize>::new();
    let mut searched_total_piecewise_payload_exact = 0usize;
    let mut best_mix_target_window_payload_exact = 0usize;
    let mut searched_target_window_payload_exact = 0usize;
    let mut target_window_count = 0usize;
    let mut improved_target_window_count = 0usize;
    let mut equal_target_window_count = 0usize;
    let mut worsened_target_window_count = 0usize;
    let mut eval_cache = BTreeMap::<(String, usize, usize), FrozenEvalRow>::new();

    for report in &reports {
        let input_bytes = fs::read(&report.input)
            .with_context(|| format!("read input for local profile {}", report.input))?;
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

        searched_total_piecewise_payload_exact = searched_total_piecewise_payload_exact
            .saturating_add(report.total_piecewise_payload_exact);

        let mut file_target_window_count = 0usize;
        let mut file_searched_target_window_payload_exact = 0usize;
        let mut file_best_mix_target_window_payload_exact = 0usize;
        let mut file_improved_target_window_count = 0usize;
        let mut file_equal_target_window_count = 0usize;
        let mut file_worsened_target_window_count = 0usize;

        for window in &report.windows {
            let global_law_id = local_to_global
                .get(&window.local_law_id)
                .cloned()
                .unwrap_or_else(|| "G?".to_string());
            if global_law_id != target_profile.global_law_id {
                continue;
            }

            file_target_window_count += 1;
            file_searched_target_window_payload_exact = file_searched_target_window_payload_exact
                .saturating_add(window.compact_field_total_payload_exact);
            target_window_count += 1;
            searched_target_window_payload_exact = searched_target_window_payload_exact
                .saturating_add(window.compact_field_total_payload_exact);

            let mut per_window = Vec::<FrozenEvalRow>::new();
            for &chunk_bytes in &chunk_candidates {
                let frozen = eval_window_under_local_candidate(
                    &exe,
                    &args,
                    &report.input,
                    &input_bytes,
                    window,
                    &temp_dir,
                    eval_boundary_band,
                    eval_field_margin,
                    eval_newline_demote_margin,
                    chunk_bytes,
                    &eval_objective,
                    eval_raw_slack,
                    &mut eval_cache,
                )?;

                let candidate_row = CandidateEvalRow {
                    input: report.input.clone(),
                    window_idx: window.window_idx,
                    candidate_chunk_bytes: chunk_bytes,
                    searched_payload_exact: window.compact_field_total_payload_exact,
                    candidate_payload_exact: frozen.compact_field_total_payload_exact,
                    delta_payload_exact: (frozen.compact_field_total_payload_exact as i64)
                        - (window.compact_field_total_payload_exact as i64),
                    searched_patch_bytes: window.field_patch_bytes,
                    candidate_patch_bytes: frozen.field_patch_bytes,
                    delta_patch_bytes: (frozen.field_patch_bytes as i64)
                        - (window.field_patch_bytes as i64),
                    searched_match_pct: window.field_match_pct,
                    candidate_match_pct: frozen.field_match_pct,
                    searched_match_vs_majority_pct: window.field_match_vs_majority_pct,
                    candidate_match_vs_majority_pct: frozen.field_match_vs_majority_pct,
                    searched_balanced_accuracy_pct: window.field_balanced_accuracy_pct,
                    candidate_balanced_accuracy_pct: frozen.field_balanced_accuracy_pct,
                    searched_macro_f1_pct: window.field_macro_f1_pct,
                    candidate_macro_f1_pct: frozen.field_macro_f1_pct,
                    searched_f1_newline_pct: window.field_f1_newline_pct,
                    candidate_f1_newline_pct: frozen.field_f1_newline_pct,
                    candidate_pred_dominant_label: frozen.field_pred_dominant_label.clone(),
                    candidate_pred_dominant_share_pct: frozen.field_pred_dominant_share_pct,
                    candidate_collapse_90_flag: frozen.field_pred_collapse_90_flag,
                    candidate_newline_delta: frozen.field_pred_newline_delta,
                    candidate_newline_demoted: frozen.field_newline_demoted,
                    candidate_newline_after_demote: frozen.field_newline_after_demote,
                    candidate_newline_floor_used: frozen.field_newline_floor_used,
                    candidate_newline_extinct_flag: frozen.field_newline_extinct_flag,
                };
                per_candidate_acc
                    .entry(chunk_bytes)
                    .or_default()
                    .push(candidate_row.clone());
                candidate_rows.push(candidate_row);
                per_window.push(frozen);
            }

            let best = select_best_eval(&per_window)
                .ok_or_else(|| anyhow!("missing local candidate evals for window {}", window.window_idx))?;
            *best_chunk_counts.entry(best.search.chunk_bytes).or_default() += 1;

            file_best_mix_target_window_payload_exact = file_best_mix_target_window_payload_exact
                .saturating_add(best.compact_field_total_payload_exact);
            best_mix_target_window_payload_exact = best_mix_target_window_payload_exact
                .saturating_add(best.compact_field_total_payload_exact);

            let delta_payload_exact = (best.compact_field_total_payload_exact as i64)
                - (window.compact_field_total_payload_exact as i64);
            match delta_payload_exact.cmp(&0) {
                std::cmp::Ordering::Less => {
                    file_improved_target_window_count += 1;
                    improved_target_window_count += 1;
                }
                std::cmp::Ordering::Equal => {
                    file_equal_target_window_count += 1;
                    equal_target_window_count += 1;
                }
                std::cmp::Ordering::Greater => {
                    file_worsened_target_window_count += 1;
                    worsened_target_window_count += 1;
                }
            }

            best_rows.push(WindowBestRow {
                input: report.input.clone(),
                window_idx: window.window_idx,
                start: window.start,
                end: window.end,
                span_bytes: window.span_bytes,
                searched_local_law_id: window.local_law_id.clone(),
                searched_global_law_id: global_law_id,
                searched_chunk_bytes: window.chunk_bytes,
                searched_chunk_search_objective: window.chunk_search_objective.clone(),
                searched_chunk_raw_slack: window.chunk_raw_slack,
                searched_payload_exact: window.compact_field_total_payload_exact,
                searched_patch_bytes: window.field_patch_bytes,
                searched_match_pct: window.field_match_pct,
                searched_match_vs_majority_pct: window.field_match_vs_majority_pct,
                searched_balanced_accuracy_pct: window.field_balanced_accuracy_pct,
                searched_macro_f1_pct: window.field_macro_f1_pct,
                searched_f1_newline_pct: window.field_f1_newline_pct,
                best_chunk_bytes: best.search.chunk_bytes,
                best_payload_exact: best.compact_field_total_payload_exact,
                best_patch_bytes: best.field_patch_bytes,
                delta_payload_exact,
                delta_patch_bytes: (best.field_patch_bytes as i64) - (window.field_patch_bytes as i64),
                best_match_pct: best.field_match_pct,
                best_match_vs_majority_pct: best.field_match_vs_majority_pct,
                best_balanced_accuracy_pct: best.field_balanced_accuracy_pct,
                best_macro_f1_pct: best.field_macro_f1_pct,
                best_f1_newline_pct: best.field_f1_newline_pct,
                best_pred_dominant_label: best.field_pred_dominant_label.clone(),
                best_pred_dominant_share_pct: best.field_pred_dominant_share_pct,
                best_collapse_90_flag: best.field_pred_collapse_90_flag,
                best_newline_delta: best.field_pred_newline_delta,
                best_newline_demoted: best.field_newline_demoted,
                best_newline_after_demote: best.field_newline_after_demote,
                best_newline_floor_used: best.field_newline_floor_used,
                best_newline_extinct_flag: best.field_newline_extinct_flag,
            });
        }

        file_summaries.push(FileSummary {
            input: report.input.clone(),
            searched_total_piecewise_payload_exact: report.total_piecewise_payload_exact,
            projected_total_piecewise_payload_exact: (report.total_piecewise_payload_exact as isize)
                + (file_best_mix_target_window_payload_exact as isize)
                - (file_searched_target_window_payload_exact as isize),
            delta_total_piecewise_payload_exact: (file_best_mix_target_window_payload_exact as i64)
                - (file_searched_target_window_payload_exact as i64),
            target_window_count: file_target_window_count,
            searched_target_window_payload_exact: file_searched_target_window_payload_exact,
            best_mix_target_window_payload_exact: file_best_mix_target_window_payload_exact,
            delta_best_mix_target_window_payload_exact: (file_best_mix_target_window_payload_exact as i64)
                - (file_searched_target_window_payload_exact as i64),
            improved_target_window_count: file_improved_target_window_count,
            equal_target_window_count: file_equal_target_window_count,
            worsened_target_window_count: file_worsened_target_window_count,
        });
    }

    if !args.keep_temp_dir {
        let _ = fs::remove_dir_all(&temp_dir);
    }

    let mut candidate_summaries = per_candidate_acc
        .into_iter()
        .map(|(chunk_bytes, rows)| {
            build_candidate_summary(
                chunk_bytes,
                &rows,
                searched_total_piecewise_payload_exact,
                best_rows.iter().filter(|row| row.best_chunk_bytes == chunk_bytes).count(),
            )
        })
        .collect::<Vec<_>>();
    candidate_summaries.sort_by(|a, b| {
        a.projected_total_piecewise_payload_exact
            .cmp(&b.projected_total_piecewise_payload_exact)
            .then_with(|| b.best_for_window_count.cmp(&a.best_for_window_count))
            .then_with(|| a.chunk_bytes.cmp(&b.chunk_bytes))
    });

    best_rows.sort_by(|a, b| {
        a.delta_payload_exact
            .cmp(&b.delta_payload_exact)
            .then_with(|| a.input.cmp(&b.input))
            .then_with(|| a.window_idx.cmp(&b.window_idx))
    });

    let dominant_best = best_chunk_counts
        .into_iter()
        .max_by_key(|(_, count)| *count)
        .unwrap_or((0usize, 0usize));
    let best_gain = best_rows
        .iter()
        .find(|row| row.delta_payload_exact < 0)
        .cloned()
        .unwrap_or_else(empty_best_row);
    let worst_loss = best_rows
        .iter()
        .rev()
        .find(|row| row.delta_payload_exact > 0)
        .cloned()
        .unwrap_or_else(empty_best_row);

    let summary = LocalProfileSummary {
        recipe: reports
            .first()
            .map(|r| r.recipe.clone())
            .unwrap_or_else(|| args.recipe.clone()),
        file_count: reports.len(),
        honest_file_count: reports.iter().filter(|r| r.honest_non_overlapping).count(),
        union_law_count: profiles.len(),
        target_global_law_id: target_profile.global_law_id.clone(),
        target_global_law_path_hits: target_profile.path_hits,
        target_global_law_file_count: target_profile.file_count,
        target_global_law_total_window_count: target_profile.total_window_count,
        target_global_law_total_segment_count: target_profile.total_segment_count,
        target_global_law_total_covered_bytes: target_profile.total_covered_bytes,
        target_global_law_weighted_mean_compact_field_total_payload_exact: target_profile
            .weighted_mean_compact_field_total_payload_exact,
        target_global_law_dominant_knob_signature: target_profile.dominant_knob_signature.clone(),
        eval_boundary_band,
        eval_field_margin,
        eval_newline_demote_margin,
        eval_chunk_search_objective: eval_objective.clone(),
        eval_chunk_raw_slack: eval_raw_slack,
        eval_chunk_candidates: join_usize_csv(&chunk_candidates),
        eval_chunk_candidate_count: chunk_candidates.len(),
        searched_total_piecewise_payload_exact,
        projected_best_mix_total_piecewise_payload_exact: (searched_total_piecewise_payload_exact as isize)
            + (best_mix_target_window_payload_exact as isize)
            - (searched_target_window_payload_exact as isize),
        delta_best_mix_total_piecewise_payload_exact: (best_mix_target_window_payload_exact as i64)
            - (searched_target_window_payload_exact as i64),
        target_window_count,
        searched_target_window_payload_exact,
        best_mix_target_window_payload_exact,
        delta_best_mix_target_window_payload_exact: (best_mix_target_window_payload_exact as i64)
            - (searched_target_window_payload_exact as i64),
        improved_target_window_count,
        equal_target_window_count,
        worsened_target_window_count,
        dominant_best_chunk_bytes: dominant_best.0,
        dominant_best_chunk_count: dominant_best.1,
        best_gain_input: best_gain.input.clone(),
        best_gain_window_idx: best_gain.window_idx,
        best_gain_delta_payload_exact: best_gain.delta_payload_exact,
        worst_loss_input: worst_loss.input.clone(),
        worst_loss_window_idx: worst_loss.window_idx,
        worst_loss_delta_payload_exact: worst_loss.delta_payload_exact,
    };

    let body = match args.format {
        RenderFormat::Txt => render_txt(&summary, &file_summaries, &candidate_summaries, &best_rows, args.top_rows),
        RenderFormat::Csv => render_csv(&summary, &file_summaries, &candidate_summaries, &best_rows),
    };
    write_or_print(args.out.as_deref(), &body)?;

    if let Some(path) = args.out.as_deref() {
        eprintln!(
            "apex-lane-law-local-profile: out={} files={} target={} searched_total_piecewise_payload_exact={} projected_best_mix_total_piecewise_payload_exact={} delta_best_mix_total_piecewise_payload_exact={}",
            path,
            summary.file_count,
            summary.target_global_law_id,
            summary.searched_total_piecewise_payload_exact,
            summary.projected_best_mix_total_piecewise_payload_exact,
            summary.delta_best_mix_total_piecewise_payload_exact,
        );
    }

    Ok(())
}

fn eval_window_under_local_candidate(
    exe: &Path,
    args: &ApexLaneLawLocalProfileArgs,
    input_name: &str,
    input_bytes: &[u8],
    window: &ManifestWindowRow,
    temp_dir: &Path,
    boundary_band: usize,
    field_margin: u64,
    newline_demote_margin: u64,
    chunk_bytes: usize,
    chunk_search_objective: &str,
    chunk_raw_slack: u64,
    cache: &mut BTreeMap<(String, usize, usize), FrozenEvalRow>,
) -> Result<FrozenEvalRow> {
    let cache_key = (input_name.to_string(), window.window_idx, chunk_bytes);
    if let Some(cached) = cache.get(&cache_key) {
        return Ok(cached.clone());
    }

    let slice = &input_bytes[window.start..window.end];
    let window_path = temp_dir.join(format!(
        "local_profile_{}_window_{:04}_{:08}_{:08}_chunk_{:04}.bin",
        sanitize_file_stem(input_name),
        window.window_idx,
        window.start,
        window.end,
        chunk_bytes
    ));
    fs::write(&window_path, slice)
        .with_context(|| format!("write local profile slice {}", window_path.display()))?;

    let mut cmd = Command::new(exe);
    cmd.arg("apextrace")
        .arg("apex-map-lane")
        .arg("--recipe")
        .arg(&args.recipe)
        .arg("--in")
        .arg(window_path.as_os_str())
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
        .arg(chunk_bytes.to_string())
        .arg("--chunk-search-objective")
        .arg(chunk_search_objective)
        .arg("--chunk-raw-slack")
        .arg(chunk_raw_slack.to_string())
        .arg("--map-max-depth")
        .arg(args.map_max_depth.to_string())
        .arg("--map-depth-shift")
        .arg(args.map_depth_shift.to_string())
        .arg("--boundary-band")
        .arg(boundary_band.to_string())
        .arg("--boundary-delta")
        .arg(args.boundary_delta.to_string())
        .arg("--field-margin")
        .arg(field_margin.to_string())
        .arg("--newline-margin-add")
        .arg(args.newline_margin_add.to_string())
        .arg("--space-to-newline-margin-add")
        .arg(args.space_to_newline_margin_add.to_string())
        .arg("--newline-share-ppm-min")
        .arg(args.newline_share_ppm_min.to_string())
        .arg("--newline-override-budget")
        .arg(args.newline_override_budget.to_string())
        .arg("--newline-demote-margin")
        .arg(newline_demote_margin.to_string())
        .arg("--newline-demote-keep-ppm-min")
        .arg(args.newline_demote_keep_ppm_min.to_string())
        .arg("--newline-demote-keep-min")
        .arg(args.newline_demote_keep_min.to_string())
        .arg(format!(
            "--newline-only-from-spacelike={}",
            bool_name(args.newline_only_from_spacelike)
        ))
        .arg("--format")
        .arg("txt");

    if args.field_from_global {
        cmd.arg("--field-from-global");
    }

    let output = cmd.output().with_context(|| {
        format!(
            "run apex-map-lane for local profile input={} window_idx={} chunk_bytes={}",
            input_name, window.window_idx, chunk_bytes
        )
    })?;
    if !output.status.success() {
        return Err(anyhow!(
            "apex-map-lane failed for input={} window_idx={} chunk_bytes={}: status={} stderr={}",
            input_name,
            window.window_idx,
            chunk_bytes,
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    let parsed = parse_best_line(&output.stderr).with_context(|| {
        format!(
            "parse apex-map-lane best line for input={} window_idx={} chunk_bytes={}",
            input_name, window.window_idx, chunk_bytes
        )
    })?;
    cache.insert(cache_key, parsed.clone());
    Ok(parsed)
}

fn run_child_apex_lane_manifest(
    exe: &Path,
    args: &ApexLaneLawLocalProfileArgs,
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
            bool_name(args.newline_only_from_spacelike)
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

    let output = cmd.output().with_context(|| {
        format!(
            "run apex-lane-manifest for {} in apex-lane-law-local-profile",
            input
        )
    })?;
    if !output.status.success() {
        return Err(anyhow!(
            "apex-lane-manifest failed for {}: status={} stderr={}",
            input,
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(String::from_utf8(output.stdout)
        .context("decode apex-lane-manifest stdout as utf8")?)
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
        windows_analyzed: parse_required_usize(&summary, "windows_analyzed")?,
        honest_non_overlapping: parse_required_bool(&summary, "honest_non_overlapping")?,
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
        field_patch_bytes: parse_required_usize(&tokens, "field_patch_bytes")?,
        field_match_pct: parse_required_f64(&tokens, "field_match_pct")?,
        field_match_vs_majority_pct: parse_required_f64(
            &tokens,
            "field_match_vs_majority_pct",
        )?,
        field_balanced_accuracy_pct: parse_required_f64(
            &tokens,
            "field_balanced_accuracy_pct",
        )?,
        field_macro_f1_pct: parse_required_f64(&tokens, "field_macro_f1_pct")?,
        field_f1_newline_pct: parse_required_f64(&tokens, "field_f1_newline_pct")?,
        field_pred_dominant_label: parse_required_string(&tokens, "field_pred_dominant_label")?,
        field_pred_dominant_share_pct: parse_required_f64(
            &tokens,
            "field_pred_dominant_share_pct",
        )?,
        field_pred_collapse_90_flag: parse_required_bool(
            &tokens,
            "field_pred_collapse_90_flag",
        )?,
        field_pred_newline_delta: parse_required_i64(&tokens, "field_pred_newline_delta")?,
        field_newline_demoted: parse_required_usize(&tokens, "field_newline_demoted")?,
        field_newline_after_demote: parse_required_usize(
            &tokens,
            "field_newline_after_demote",
        )?,
        field_newline_floor_used: parse_required_usize(&tokens, "field_newline_floor_used")?,
        field_newline_extinct_flag: parse_required_bool(
            &tokens,
            "field_newline_extinct_flag",
        )?,
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
                weighted_payload_sum += law.mean_compact_field_total_payload_exact * law.window_count as f64;
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
                            window.chunk_bytes, window.chunk_search_objective, window.chunk_raw_slack
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

    per_law.sort_by(|a, b| {
        b.path_hits
            .cmp(&a.path_hits)
            .then_with(|| a.global_law_id.cmp(&b.global_law_id))
    });
    per_law
}

fn select_target_profile<'a>(
    profiles: &'a [LawProfile],
    requested: Option<&str>,
) -> Result<&'a LawProfile> {
    if let Some(global_law_id) = requested {
        return profiles
            .iter()
            .find(|profile| profile.global_law_id == global_law_id)
            .ok_or_else(|| anyhow!("unknown global_law_id {}", global_law_id));
    }
    profiles
        .iter()
        .max_by_key(|profile| profile.path_hits)
        .ok_or_else(|| anyhow!("no law profiles available"))
}

fn select_chunk_candidates(
    args: &ApexLaneLawLocalProfileArgs,
    reports: &[FileReport],
    shared_law_ids: &BTreeMap<ReplayLawTuple, String>,
    target_profile: &LawProfile,
    dominant_chunk_bytes: usize,
) -> Result<Vec<usize>> {
    if let Some(raw) = args.local_chunk_sweep.as_deref() {
        let parsed = parse_csv_usize(raw, "local_chunk_sweep")?;
        if parsed.is_empty() {
            return Err(anyhow!("local_chunk_sweep requires at least one value"));
        }
        return Ok(parsed);
    }

    let mut values = BTreeSet::<usize>::new();
    values.insert(dominant_chunk_bytes);
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
                .map(|g| g == &target_profile.global_law_id)
                .unwrap_or(false)
            {
                values.insert(window.chunk_bytes);
            }
        }
    }
    Ok(values.into_iter().collect())
}

fn select_best_eval(rows: &[FrozenEvalRow]) -> Option<FrozenEvalRow> {
    rows.iter().cloned().min_by(|a, b| {
        a.compact_field_total_payload_exact
            .cmp(&b.compact_field_total_payload_exact)
            .then_with(|| b.field_match_pct.total_cmp(&a.field_match_pct))
            .then_with(|| a.field_patch_bytes.cmp(&b.field_patch_bytes))
            .then_with(|| a.search.chunk_bytes.cmp(&b.search.chunk_bytes))
    })
}

fn build_candidate_summary(
    chunk_bytes: usize,
    rows: &[CandidateEvalRow],
    searched_total_piecewise_payload_exact: usize,
    best_for_window_count: usize,
) -> CandidateSummary {
    let eval_window_count = rows.len();
    let improved_window_count = rows.iter().filter(|row| row.delta_payload_exact < 0).count();
    let equal_window_count = rows.iter().filter(|row| row.delta_payload_exact == 0).count();
    let worsened_window_count = rows.iter().filter(|row| row.delta_payload_exact > 0).count();
    let total_delta_payload_exact = rows.iter().map(|row| row.delta_payload_exact).sum::<i64>();
    let total_delta_patch_bytes = rows.iter().map(|row| row.delta_patch_bytes).sum::<i64>();
    let mean_payload_exact = mean_usize(rows.iter().map(|row| row.candidate_payload_exact));
    let mean_patch_bytes = mean_usize(rows.iter().map(|row| row.candidate_patch_bytes));
    let mean_delta_payload_exact = mean_i64(rows.iter().map(|row| row.delta_payload_exact));
    let mean_delta_patch_bytes = mean_i64(rows.iter().map(|row| row.delta_patch_bytes));
    let mean_match_pct = mean_f64(rows.iter().map(|row| row.candidate_match_pct));
    let mean_match_vs_majority_pct = mean_f64(
        rows.iter()
            .map(|row| row.candidate_match_vs_majority_pct),
    );
    let mean_balanced_accuracy_pct = mean_f64(
        rows.iter()
            .map(|row| row.candidate_balanced_accuracy_pct),
    );
    let mean_macro_f1_pct = mean_f64(rows.iter().map(|row| row.candidate_macro_f1_pct));
    let mean_f1_newline_pct = mean_f64(rows.iter().map(|row| row.candidate_f1_newline_pct));
    let mean_pred_dominant_share_pct = mean_f64(
        rows.iter()
            .map(|row| row.candidate_pred_dominant_share_pct),
    );
    let collapse_90_count = rows.iter().filter(|row| row.candidate_collapse_90_flag).count();
    let newline_extinct_count = rows
        .iter()
        .filter(|row| row.candidate_newline_extinct_flag)
        .count();
    let mean_abs_newline_delta = mean_f64(
        rows.iter()
            .map(|row| row.candidate_newline_delta.unsigned_abs() as f64),
    );

    CandidateSummary {
        chunk_bytes,
        eval_window_count,
        best_for_window_count,
        improved_window_count,
        equal_window_count,
        worsened_window_count,
        total_delta_payload_exact,
        total_delta_patch_bytes,
        mean_payload_exact,
        mean_patch_bytes,
        mean_delta_payload_exact,
        mean_delta_patch_bytes,
        mean_match_pct,
        mean_match_vs_majority_pct,
        mean_balanced_accuracy_pct,
        mean_macro_f1_pct,
        mean_f1_newline_pct,
        mean_pred_dominant_share_pct,
        collapse_90_count,
        newline_extinct_count,
        mean_abs_newline_delta,
        projected_total_piecewise_payload_exact: (searched_total_piecewise_payload_exact as isize)
            + (total_delta_payload_exact as isize),
    }
}

fn render_txt(
    summary: &LocalProfileSummary,
    files: &[FileSummary],
    candidates: &[CandidateSummary],
    best_rows: &[WindowBestRow],
    top_rows: usize,
) -> String {
    let mut out = String::new();
    macro_rules! line {
        ($key:expr, $value:expr) => {
            out.push_str(&format!("{}={}\n", $key, $value));
        };
    }

    line!("recipe", summary.recipe.clone());
    line!("file_count", summary.file_count);
    line!("honest_file_count", summary.honest_file_count);
    line!("union_law_count", summary.union_law_count);
    line!("target_global_law_id", summary.target_global_law_id.clone());
    line!("target_global_law_path_hits", summary.target_global_law_path_hits);
    line!("target_global_law_file_count", summary.target_global_law_file_count);
    line!("target_global_law_total_window_count", summary.target_global_law_total_window_count);
    line!("target_global_law_total_segment_count", summary.target_global_law_total_segment_count);
    line!("target_global_law_total_covered_bytes", summary.target_global_law_total_covered_bytes);
    line!(
        "target_global_law_weighted_mean_compact_field_total_payload_exact",
        format!(
            "{:.6}",
            summary.target_global_law_weighted_mean_compact_field_total_payload_exact
        )
    );
    line!(
        "target_global_law_dominant_knob_signature",
        summary.target_global_law_dominant_knob_signature.replace(' ', "|")
    );
    line!("eval_boundary_band", summary.eval_boundary_band);
    line!("eval_field_margin", summary.eval_field_margin);
    line!("eval_newline_demote_margin", summary.eval_newline_demote_margin);
    line!(
        "eval_chunk_search_objective",
        summary.eval_chunk_search_objective.clone()
    );
    line!("eval_chunk_raw_slack", summary.eval_chunk_raw_slack);
    line!("eval_chunk_candidates", summary.eval_chunk_candidates.clone());
    line!("eval_chunk_candidate_count", summary.eval_chunk_candidate_count);
    line!(
        "searched_total_piecewise_payload_exact",
        summary.searched_total_piecewise_payload_exact
    );
    line!(
        "projected_best_mix_total_piecewise_payload_exact",
        summary.projected_best_mix_total_piecewise_payload_exact
    );
    line!(
        "delta_best_mix_total_piecewise_payload_exact",
        summary.delta_best_mix_total_piecewise_payload_exact
    );
    line!("target_window_count", summary.target_window_count);
    line!(
        "searched_target_window_payload_exact",
        summary.searched_target_window_payload_exact
    );
    line!(
        "best_mix_target_window_payload_exact",
        summary.best_mix_target_window_payload_exact
    );
    line!(
        "delta_best_mix_target_window_payload_exact",
        summary.delta_best_mix_target_window_payload_exact
    );
    line!("improved_target_window_count", summary.improved_target_window_count);
    line!("equal_target_window_count", summary.equal_target_window_count);
    line!("worsened_target_window_count", summary.worsened_target_window_count);
    line!("dominant_best_chunk_bytes", summary.dominant_best_chunk_bytes);
    line!("dominant_best_chunk_count", summary.dominant_best_chunk_count);
    line!("best_gain_input", summary.best_gain_input.clone());
    line!("best_gain_window_idx", summary.best_gain_window_idx);
    line!("best_gain_delta_payload_exact", summary.best_gain_delta_payload_exact);
    line!("worst_loss_input", summary.worst_loss_input.clone());
    line!("worst_loss_window_idx", summary.worst_loss_window_idx);
    line!("worst_loss_delta_payload_exact", summary.worst_loss_delta_payload_exact);

    out.push_str("\n--- file_summaries ---\n");
    for row in files {
        out.push_str(&format!(
            "input={} searched_total_piecewise_payload_exact={} projected_total_piecewise_payload_exact={} delta_total_piecewise_payload_exact={} target_window_count={} searched_target_window_payload_exact={} best_mix_target_window_payload_exact={} delta_best_mix_target_window_payload_exact={} improved_target_window_count={} equal_target_window_count={} worsened_target_window_count={}\n",
            row.input,
            row.searched_total_piecewise_payload_exact,
            row.projected_total_piecewise_payload_exact,
            row.delta_total_piecewise_payload_exact,
            row.target_window_count,
            row.searched_target_window_payload_exact,
            row.best_mix_target_window_payload_exact,
            row.delta_best_mix_target_window_payload_exact,
            row.improved_target_window_count,
            row.equal_target_window_count,
            row.worsened_target_window_count,
        ));
    }

    out.push_str("\n--- candidate_summaries ---\n");
    for row in candidates {
        out.push_str(&format!(
            "chunk_bytes={} eval_window_count={} best_for_window_count={} improved_window_count={} equal_window_count={} worsened_window_count={} total_delta_payload_exact={} total_delta_patch_bytes={} mean_payload_exact={:.6} mean_patch_bytes={:.6} mean_delta_payload_exact={:.6} mean_delta_patch_bytes={:.6} mean_match_pct={:.6} mean_match_vs_majority_pct={:.6} mean_balanced_accuracy_pct={:.6} mean_macro_f1_pct={:.6} mean_f1_newline_pct={:.6} mean_pred_dominant_share_pct={:.6} collapse_90_count={} newline_extinct_count={} mean_abs_newline_delta={:.6} projected_total_piecewise_payload_exact={}\n",
            row.chunk_bytes,
            row.eval_window_count,
            row.best_for_window_count,
            row.improved_window_count,
            row.equal_window_count,
            row.worsened_window_count,
            row.total_delta_payload_exact,
            row.total_delta_patch_bytes,
            row.mean_payload_exact,
            row.mean_patch_bytes,
            row.mean_delta_payload_exact,
            row.mean_delta_patch_bytes,
            row.mean_match_pct,
            row.mean_match_vs_majority_pct,
            row.mean_balanced_accuracy_pct,
            row.mean_macro_f1_pct,
            row.mean_f1_newline_pct,
            row.mean_pred_dominant_share_pct,
            row.collapse_90_count,
            row.newline_extinct_count,
            row.mean_abs_newline_delta,
            row.projected_total_piecewise_payload_exact,
        ));
    }

    out.push_str("\n--- best_windows ---\n");
    for row in best_rows.iter().take(top_rows) {
        out.push_str(&format!(
            "input={} window_idx={} start={} end={} span_bytes={} searched_local_law_id={} searched_global_law_id={} searched_chunk_bytes={} searched_chunk_search_objective={} searched_chunk_raw_slack={} searched_payload_exact={} searched_patch_bytes={} searched_match_pct={:.6} searched_match_vs_majority_pct={:.6} searched_balanced_accuracy_pct={:.6} searched_macro_f1_pct={:.6} searched_f1_newline_pct={:.6} best_chunk_bytes={} best_payload_exact={} best_patch_bytes={} delta_payload_exact={} delta_patch_bytes={} best_match_pct={:.6} best_match_vs_majority_pct={:.6} best_balanced_accuracy_pct={:.6} best_macro_f1_pct={:.6} best_f1_newline_pct={:.6} best_pred_dominant_label={} best_pred_dominant_share_pct={:.6} best_collapse_90_flag={} best_newline_delta={} best_newline_demoted={} best_newline_after_demote={} best_newline_floor_used={} best_newline_extinct_flag={}\n",
            row.input,
            row.window_idx,
            row.start,
            row.end,
            row.span_bytes,
            row.searched_local_law_id,
            row.searched_global_law_id,
            row.searched_chunk_bytes,
            row.searched_chunk_search_objective,
            row.searched_chunk_raw_slack,
            row.searched_payload_exact,
            row.searched_patch_bytes,
            row.searched_match_pct,
            row.searched_match_vs_majority_pct,
            row.searched_balanced_accuracy_pct,
            row.searched_macro_f1_pct,
            row.searched_f1_newline_pct,
            row.best_chunk_bytes,
            row.best_payload_exact,
            row.best_patch_bytes,
            row.delta_payload_exact,
            row.delta_patch_bytes,
            row.best_match_pct,
            row.best_match_vs_majority_pct,
            row.best_balanced_accuracy_pct,
            row.best_macro_f1_pct,
            row.best_f1_newline_pct,
            row.best_pred_dominant_label,
            row.best_pred_dominant_share_pct,
            row.best_collapse_90_flag,
            row.best_newline_delta,
            row.best_newline_demoted,
            row.best_newline_after_demote,
            row.best_newline_floor_used,
            row.best_newline_extinct_flag,
        ));
    }

    out
}

fn render_csv(
    summary: &LocalProfileSummary,
    files: &[FileSummary],
    candidates: &[CandidateSummary],
    best_rows: &[WindowBestRow],
) -> String {
    let headers = [
        "row_type",
        "recipe",
        "target_global_law_id",
        "input",
        "window_idx",
        "chunk_bytes",
        "searched_total_piecewise_payload_exact",
        "projected_total_piecewise_payload_exact",
        "delta_total_piecewise_payload_exact",
        "searched_target_window_payload_exact",
        "best_mix_target_window_payload_exact",
        "delta_best_mix_target_window_payload_exact",
        "improved_target_window_count",
        "equal_target_window_count",
        "worsened_target_window_count",
        "best_for_window_count",
        "mean_payload_exact",
        "mean_delta_payload_exact",
        "mean_patch_bytes",
        "mean_match_pct",
        "mean_match_vs_majority_pct",
        "mean_balanced_accuracy_pct",
        "mean_macro_f1_pct",
        "mean_f1_newline_pct",
        "mean_pred_dominant_share_pct",
        "collapse_90_count",
        "newline_extinct_count",
        "mean_abs_newline_delta",
        "searched_chunk_bytes",
        "best_chunk_bytes",
        "searched_payload_exact",
        "best_payload_exact",
        "delta_payload_exact",
        "searched_patch_bytes",
        "best_patch_bytes",
        "delta_patch_bytes",
        "searched_match_pct",
        "best_match_pct",
        "searched_match_vs_majority_pct",
        "best_match_vs_majority_pct",
        "searched_balanced_accuracy_pct",
        "best_balanced_accuracy_pct",
        "searched_macro_f1_pct",
        "best_macro_f1_pct",
        "searched_f1_newline_pct",
        "best_f1_newline_pct",
        "best_pred_dominant_label",
        "best_pred_dominant_share_pct",
        "best_collapse_90_flag",
        "best_newline_delta",
        "best_newline_demoted",
        "best_newline_after_demote",
        "best_newline_floor_used",
        "best_newline_extinct_flag",
    ];

    let mut out = String::new();
    out.push_str(&headers.join(","));
    out.push('\n');

    out.push_str(&csv_line(vec![
        "summary".to_string(),
        summary.recipe.clone(),
        summary.target_global_law_id.clone(),
        String::new(),
        String::new(),
        summary.dominant_best_chunk_bytes.to_string(),
        summary.searched_total_piecewise_payload_exact.to_string(),
        summary.projected_best_mix_total_piecewise_payload_exact.to_string(),
        summary.delta_best_mix_total_piecewise_payload_exact.to_string(),
        summary.searched_target_window_payload_exact.to_string(),
        summary.best_mix_target_window_payload_exact.to_string(),
        summary.delta_best_mix_target_window_payload_exact.to_string(),
        summary.improved_target_window_count.to_string(),
        summary.equal_target_window_count.to_string(),
        summary.worsened_target_window_count.to_string(),
        summary.dominant_best_chunk_count.to_string(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
    ]));

    for row in files {
        out.push_str(&csv_line(vec![
            "file_summary".to_string(),
            summary.recipe.clone(),
            summary.target_global_law_id.clone(),
            row.input.clone(),
            String::new(),
            String::new(),
            row.searched_total_piecewise_payload_exact.to_string(),
            row.projected_total_piecewise_payload_exact.to_string(),
            row.delta_total_piecewise_payload_exact.to_string(),
            row.searched_target_window_payload_exact.to_string(),
            row.best_mix_target_window_payload_exact.to_string(),
            row.delta_best_mix_target_window_payload_exact.to_string(),
            row.improved_target_window_count.to_string(),
            row.equal_target_window_count.to_string(),
            row.worsened_target_window_count.to_string(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
        ]));
    }

    for row in candidates {
        out.push_str(&csv_line(vec![
            "candidate_summary".to_string(),
            summary.recipe.clone(),
            summary.target_global_law_id.clone(),
            String::new(),
            String::new(),
            row.chunk_bytes.to_string(),
            summary.searched_total_piecewise_payload_exact.to_string(),
            row.projected_total_piecewise_payload_exact.to_string(),
            row.total_delta_payload_exact.to_string(),
            summary.searched_target_window_payload_exact.to_string(),
            String::new(),
            String::new(),
            row.improved_window_count.to_string(),
            row.equal_window_count.to_string(),
            row.worsened_window_count.to_string(),
            row.best_for_window_count.to_string(),
            format!("{:.6}", row.mean_payload_exact),
            format!("{:.6}", row.mean_delta_payload_exact),
            format!("{:.6}", row.mean_patch_bytes),
            format!("{:.6}", row.mean_match_pct),
            format!("{:.6}", row.mean_match_vs_majority_pct),
            format!("{:.6}", row.mean_balanced_accuracy_pct),
            format!("{:.6}", row.mean_macro_f1_pct),
            format!("{:.6}", row.mean_f1_newline_pct),
            format!("{:.6}", row.mean_pred_dominant_share_pct),
            row.collapse_90_count.to_string(),
            row.newline_extinct_count.to_string(),
            format!("{:.6}", row.mean_abs_newline_delta),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
        ]));
    }

    for row in best_rows {
        out.push_str(&csv_line(vec![
            "best_window".to_string(),
            summary.recipe.clone(),
            summary.target_global_law_id.clone(),
            row.input.clone(),
            row.window_idx.to_string(),
            row.best_chunk_bytes.to_string(),
            summary.searched_total_piecewise_payload_exact.to_string(),
            summary.projected_best_mix_total_piecewise_payload_exact.to_string(),
            summary.delta_best_mix_total_piecewise_payload_exact.to_string(),
            summary.searched_target_window_payload_exact.to_string(),
            summary.best_mix_target_window_payload_exact.to_string(),
            summary.delta_best_mix_target_window_payload_exact.to_string(),
            summary.improved_target_window_count.to_string(),
            summary.equal_target_window_count.to_string(),
            summary.worsened_target_window_count.to_string(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            row.searched_chunk_bytes.to_string(),
            row.best_chunk_bytes.to_string(),
            row.searched_payload_exact.to_string(),
            row.best_payload_exact.to_string(),
            row.delta_payload_exact.to_string(),
            row.searched_patch_bytes.to_string(),
            row.best_patch_bytes.to_string(),
            row.delta_patch_bytes.to_string(),
            format!("{:.6}", row.searched_match_pct),
            format!("{:.6}", row.best_match_pct),
            format!("{:.6}", row.searched_match_vs_majority_pct),
            format!("{:.6}", row.best_match_vs_majority_pct),
            format!("{:.6}", row.searched_balanced_accuracy_pct),
            format!("{:.6}", row.best_balanced_accuracy_pct),
            format!("{:.6}", row.searched_macro_f1_pct),
            format!("{:.6}", row.best_macro_f1_pct),
            format!("{:.6}", row.searched_f1_newline_pct),
            format!("{:.6}", row.best_f1_newline_pct),
            row.best_pred_dominant_label.clone(),
            format!("{:.6}", row.best_pred_dominant_share_pct),
            row.best_collapse_90_flag.to_string(),
            row.best_newline_delta.to_string(),
            row.best_newline_demoted.to_string(),
            row.best_newline_after_demote.to_string(),
            row.best_newline_floor_used.to_string(),
            row.best_newline_extinct_flag.to_string(),
        ]));
    }

    out
}

fn csv_line(values: Vec<String>) -> String {
    let mut out = String::new();
    for (idx, value) in values.into_iter().enumerate() {
        if idx != 0 {
            out.push(',');
        }
        if value.contains(',') || value.contains('"') || value.contains('\n') {
            out.push('"');
            for ch in value.chars() {
                if ch == '"' {
                    out.push('"');
                }
                out.push(ch);
            }
            out.push('"');
        } else {
            out.push_str(&value);
        }
    }
    out.push('\n');
    out
}

fn parse_knob_signature(raw: &str) -> Result<ParsedKnobSignature> {
    let tokens = tokenize_kv_line(raw);
    Ok(ParsedKnobSignature {
        chunk_bytes: parse_required_usize(&tokens, "chunk_bytes")?,
        chunk_search_objective: parse_required_string(&tokens, "chunk_search_objective")?,
        chunk_raw_slack: parse_required_u64(&tokens, "chunk_raw_slack")?,
    })
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

    let mut map = BTreeMap::<String, String>::new();
    for token in payload.split_whitespace() {
        if let Some((k, v)) = token.split_once('=') {
            map.insert(k.to_string(), v.to_string());
        }
    }

    Ok(FrozenEvalRow {
        search: SearchKnobTuple {
            chunk_bytes: parse_required_usize(&map, "chunk_bytes")?,
            chunk_search_objective: parse_required_string(&map, "chunk_search_objective")?,
            chunk_raw_slack: parse_required_u64(&map, "chunk_raw_slack")?,
        },
        compact_field_total_payload_exact: parse_required_usize(
            &map,
            "compact_field_total_payload_exact",
        )?,
        field_patch_bytes: parse_required_usize(&map, "field_patch_bytes")?,
        field_match_pct: parse_required_f64(&map, "field_match_pct")?,
        field_match_vs_majority_pct: parse_required_f64(
            &map,
            "field_match_vs_majority_pct",
        )?,
        field_balanced_accuracy_pct: parse_required_f64(
            &map,
            "field_balanced_accuracy_pct",
        )?,
        field_macro_f1_pct: parse_required_f64(&map, "field_macro_f1_pct")?,
        field_f1_newline_pct: parse_required_f64(&map, "field_f1_newline_pct")?,
        field_pred_dominant_label: parse_required_string(&map, "field_pred_dominant_label")?,
        field_pred_dominant_share_pct: parse_required_f64(
            &map,
            "field_pred_dominant_share_pct",
        )?,
        field_pred_collapse_90_flag: parse_required_bool(
            &map,
            "field_pred_collapse_90_flag",
        )?,
        field_pred_newline_delta: parse_required_i64(&map, "field_pred_newline_delta")?,
        field_newline_demoted: parse_required_usize(&map, "field_newline_demoted")?,
        field_newline_after_demote: parse_required_usize(
            &map,
            "field_newline_after_demote",
        )?,
        field_newline_floor_used: parse_required_usize(&map, "field_newline_floor_used")?,
        field_newline_extinct_flag: parse_required_bool(
            &map,
            "field_newline_extinct_flag",
        )?,
    })
}

fn empty_best_row() -> WindowBestRow {
    WindowBestRow {
        input: String::new(),
        window_idx: 0,
        start: 0,
        end: 0,
        span_bytes: 0,
        searched_local_law_id: String::new(),
        searched_global_law_id: String::new(),
        searched_chunk_bytes: 0,
        searched_chunk_search_objective: String::new(),
        searched_chunk_raw_slack: 0,
        searched_payload_exact: 0,
        searched_patch_bytes: 0,
        searched_match_pct: 0.0,
        searched_match_vs_majority_pct: 0.0,
        searched_balanced_accuracy_pct: 0.0,
        searched_macro_f1_pct: 0.0,
        searched_f1_newline_pct: 0.0,
        best_chunk_bytes: 0,
        best_payload_exact: 0,
        best_patch_bytes: 0,
        delta_payload_exact: 0,
        delta_patch_bytes: 0,
        best_match_pct: 0.0,
        best_match_vs_majority_pct: 0.0,
        best_balanced_accuracy_pct: 0.0,
        best_macro_f1_pct: 0.0,
        best_f1_newline_pct: 0.0,
        best_pred_dominant_label: String::new(),
        best_pred_dominant_share_pct: 0.0,
        best_collapse_90_flag: false,
        best_newline_delta: 0,
        best_newline_demoted: 0,
        best_newline_after_demote: 0,
        best_newline_floor_used: 0,
        best_newline_extinct_flag: false,
    }
}

fn parse_csv_usize(raw: &str, label: &str) -> Result<Vec<usize>> {
    let mut out = raw
        .split(',')
        .map(|token| token.trim())
        .filter(|token| !token.is_empty())
        .map(|token| {
            token
                .parse::<usize>()
                .with_context(|| format!("parse {} token {} as usize", label, token))
        })
        .collect::<Result<Vec<_>>>()?;
    out.sort_unstable();
    out.dedup();
    Ok(out)
}

fn join_usize_csv(values: &[usize]) -> String {
    values
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn mean_usize<I>(iter: I) -> f64
where
    I: Iterator<Item = usize>,
{
    let mut sum = 0usize;
    let mut count = 0usize;
    for value in iter {
        sum = sum.saturating_add(value);
        count += 1;
    }
    if count == 0 {
        0.0
    } else {
        sum as f64 / count as f64
    }
}

fn mean_i64<I>(iter: I) -> f64
where
    I: Iterator<Item = i64>,
{
    let mut sum = 0i64;
    let mut count = 0usize;
    for value in iter {
        sum = sum.saturating_add(value);
        count += 1;
    }
    if count == 0 {
        0.0
    } else {
        sum as f64 / count as f64
    }
}

fn mean_f64<I>(iter: I) -> f64
where
    I: Iterator<Item = f64>,
{
    let mut sum = 0.0f64;
    let mut count = 0usize;
    for value in iter {
        sum += value;
        count += 1;
    }
    if count == 0 {
        0.0
    } else {
        sum / count as f64
    }
}

fn chunk_search_objective_name(value: ChunkSearchObjective) -> &'static str {
    match value {
        ChunkSearchObjective::Raw => "raw",
        ChunkSearchObjective::RawGuarded => "raw-guarded",
        ChunkSearchObjective::Honest => "honest",
        ChunkSearchObjective::Newline => "newline",
    }
}

fn bool_name(value: bool) -> &'static str {
    if value {
        "true"
    } else {
        "false"
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

fn make_temp_dir(label: &str) -> Result<PathBuf> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("clock before unix epoch")?
        .as_nanos();
    let dir = env::temp_dir().join(format!("{}_{}", label, nanos));
    fs::create_dir_all(&dir)
        .with_context(|| format!("create temp dir {}", dir.display()))?;
    Ok(dir)
}

fn sanitize_file_stem(path: &str) -> String {
    path.chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        build_candidate_summary, parse_knob_signature, parse_window_row, select_best_eval,
        CandidateEvalRow, FrozenEvalRow, SearchKnobTuple,
    };

    #[test]
    fn knob_signature_parses() {
        let parsed = parse_knob_signature(
            "chunk_bytes=64 chunk_search_objective=raw chunk_raw_slack=1",
        )
        .expect("parse knob signature");
        assert_eq!(parsed.chunk_bytes, 64);
        assert_eq!(parsed.chunk_search_objective, "raw");
        assert_eq!(parsed.chunk_raw_slack, 1);
    }

    #[test]
    fn manifest_window_row_parses_payload_fields() {
        let row = parse_window_row("window_idx=2 law_id=L1 start=512 end=768 span_bytes=256 chunk_bytes=32 boundary_band=8 field_margin=4 newline_demote_margin=4 chunk_search_objective=raw chunk_raw_slack=1 compact_field_total_payload_exact=180 field_patch_bytes=96 field_match_pct=68.000000 majority_baseline_match_pct=89.000000 field_match_vs_majority_pct=-21.000000 field_balanced_accuracy_pct=50.000000 field_macro_f1_pct=45.000000 field_f1_newline_pct=10.000000 field_pred_dominant_label=space field_pred_dominant_share_pct=90.000000 field_pred_collapse_90_flag=true field_pred_newline_delta=-3 field_newline_demoted=1 field_newline_after_demote=5 field_newline_floor_used=1 field_newline_extinct_flag=false").expect("parse window row");
        assert_eq!(row.window_idx, 2);
        assert_eq!(row.chunk_bytes, 32);
        assert_eq!(row.compact_field_total_payload_exact, 180);
        assert_eq!(row.field_patch_bytes, 96);
        assert!(row.field_pred_collapse_90_flag);
        assert_eq!(row.field_pred_newline_delta, -3);
    }

    #[test]
    fn select_best_eval_prefers_payload_then_match() {
        let rows = vec![
            FrozenEvalRow {
                search: SearchKnobTuple {
                    chunk_bytes: 64,
                    chunk_search_objective: "raw".to_string(),
                    chunk_raw_slack: 1,
                },
                compact_field_total_payload_exact: 180,
                field_patch_bytes: 90,
                field_match_pct: 71.0,
                field_match_vs_majority_pct: -18.0,
                field_balanced_accuracy_pct: 50.0,
                field_macro_f1_pct: 45.0,
                field_f1_newline_pct: 10.0,
                field_pred_dominant_label: "space".to_string(),
                field_pred_dominant_share_pct: 90.0,
                field_pred_collapse_90_flag: true,
                field_pred_newline_delta: -3,
                field_newline_demoted: 1,
                field_newline_after_demote: 5,
                field_newline_floor_used: 1,
                field_newline_extinct_flag: false,
            },
            FrozenEvalRow {
                search: SearchKnobTuple {
                    chunk_bytes: 32,
                    chunk_search_objective: "raw".to_string(),
                    chunk_raw_slack: 1,
                },
                compact_field_total_payload_exact: 176,
                field_patch_bytes: 92,
                field_match_pct: 70.0,
                field_match_vs_majority_pct: -19.0,
                field_balanced_accuracy_pct: 49.0,
                field_macro_f1_pct: 44.0,
                field_f1_newline_pct: 9.0,
                field_pred_dominant_label: "space".to_string(),
                field_pred_dominant_share_pct: 91.0,
                field_pred_collapse_90_flag: true,
                field_pred_newline_delta: -2,
                field_newline_demoted: 1,
                field_newline_after_demote: 4,
                field_newline_floor_used: 1,
                field_newline_extinct_flag: false,
            },
        ];
        let best = select_best_eval(&rows).expect("select best");
        assert_eq!(best.search.chunk_bytes, 32);
        assert_eq!(best.compact_field_total_payload_exact, 176);
    }

    #[test]
    fn candidate_summary_projects_total() {
        let rows = vec![
            CandidateEvalRow {
                input: "a".to_string(),
                window_idx: 0,
                candidate_chunk_bytes: 32,
                searched_payload_exact: 180,
                candidate_payload_exact: 176,
                delta_payload_exact: -4,
                searched_patch_bytes: 96,
                candidate_patch_bytes: 92,
                delta_patch_bytes: -4,
                searched_match_pct: 68.0,
                candidate_match_pct: 70.0,
                searched_match_vs_majority_pct: -21.0,
                candidate_match_vs_majority_pct: -19.0,
                searched_balanced_accuracy_pct: 50.0,
                candidate_balanced_accuracy_pct: 52.0,
                searched_macro_f1_pct: 45.0,
                candidate_macro_f1_pct: 47.0,
                searched_f1_newline_pct: 10.0,
                candidate_f1_newline_pct: 12.0,
                candidate_pred_dominant_label: "space".to_string(),
                candidate_pred_dominant_share_pct: 90.0,
                candidate_collapse_90_flag: true,
                candidate_newline_delta: -3,
                candidate_newline_demoted: 1,
                candidate_newline_after_demote: 5,
                candidate_newline_floor_used: 1,
                candidate_newline_extinct_flag: false,
            },
            CandidateEvalRow {
                input: "a".to_string(),
                window_idx: 1,
                candidate_chunk_bytes: 32,
                searched_payload_exact: 177,
                candidate_payload_exact: 179,
                delta_payload_exact: 2,
                searched_patch_bytes: 90,
                candidate_patch_bytes: 91,
                delta_patch_bytes: 1,
                searched_match_pct: 70.0,
                candidate_match_pct: 69.0,
                searched_match_vs_majority_pct: -19.0,
                candidate_match_vs_majority_pct: -20.0,
                searched_balanced_accuracy_pct: 51.0,
                candidate_balanced_accuracy_pct: 50.0,
                searched_macro_f1_pct: 46.0,
                candidate_macro_f1_pct: 45.0,
                searched_f1_newline_pct: 11.0,
                candidate_f1_newline_pct: 10.0,
                candidate_pred_dominant_label: "space".to_string(),
                candidate_pred_dominant_share_pct: 91.0,
                candidate_collapse_90_flag: true,
                candidate_newline_delta: -2,
                candidate_newline_demoted: 1,
                candidate_newline_after_demote: 4,
                candidate_newline_floor_used: 1,
                candidate_newline_extinct_flag: false,
            },
        ];
        let summary = build_candidate_summary(32, &rows, 1000, 1);
        assert_eq!(summary.total_delta_payload_exact, -2);
        assert_eq!(summary.projected_total_piecewise_payload_exact, 998);
        assert_eq!(summary.best_for_window_count, 1);
        assert_eq!(summary.improved_window_count, 1);
        assert_eq!(summary.worsened_window_count, 1);
    }
}
