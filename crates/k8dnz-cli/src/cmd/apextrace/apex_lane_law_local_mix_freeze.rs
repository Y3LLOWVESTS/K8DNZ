use anyhow::{anyhow, Context, Result};
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::cmd::apextrace::{ApexLaneLawLocalMixFreezeArgs, ChunkSearchObjective, RenderFormat};

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
struct EvalConfig {
    law: ReplayLawTuple,
    search: SearchKnobTuple,
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
struct WindowEvalRow {
    input: String,
    window_idx: usize,
    target_ordinal: usize,
    start: usize,
    end: usize,
    span_bytes: usize,
    searched_local_law_id: String,
    searched_global_law_id: String,
    searched_chunk_bytes: usize,
    searched_payload_exact: usize,
    default_payload_exact: usize,
    best_chunk_bytes: usize,
    best_payload_exact: usize,
    selected_chunk_bytes: usize,
    selected_payload_exact: usize,
    default_gain_exact: i64,
    best_gain_exact: i64,
    selected_gain_exact: i64,
}

#[derive(Clone, Debug)]
struct OverrideCandidate {
    input: String,
    window_idx: usize,
    target_ordinal: usize,
    best_chunk_bytes: usize,
    default_payload_exact: usize,
    best_payload_exact: usize,
    gain_exact: usize,
}

#[derive(Clone, Debug)]
struct FileSummary {
    input: String,
    searched_total_piecewise_payload_exact: usize,
    projected_default_total_piecewise_payload_exact: isize,
    delta_default_total_piecewise_payload_exact: i64,
    projected_unpriced_best_mix_total_piecewise_payload_exact: isize,
    delta_unpriced_best_mix_total_piecewise_payload_exact: i64,
    selected_total_piecewise_payload_exact: isize,
    delta_selected_total_piecewise_payload_exact: i64,
    target_window_count: usize,
    searched_target_window_payload_exact: usize,
    default_target_window_payload_exact: usize,
    best_mix_target_window_payload_exact: usize,
    selected_target_window_payload_exact: usize,
    delta_selected_target_window_payload_exact: i64,
    override_path_bytes_exact: usize,
    selected_override_window_count: usize,
    improved_target_window_count: usize,
    equal_target_window_count: usize,
    worsened_target_window_count: usize,
}

#[derive(Clone, Debug)]
struct LocalMixSummary {
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
    eval_boundary_band: usize,
    eval_field_margin: u64,
    eval_newline_demote_margin: u64,
    eval_chunk_search_objective: String,
    eval_chunk_raw_slack: u64,
    eval_chunk_candidates: String,
    eval_chunk_candidate_count: usize,
    default_local_chunk_bytes: usize,
    default_local_chunk_window_wins: usize,
    searched_total_piecewise_payload_exact: usize,
    projected_default_total_piecewise_payload_exact: isize,
    delta_default_total_piecewise_payload_exact: i64,
    projected_unpriced_best_mix_total_piecewise_payload_exact: isize,
    delta_unpriced_best_mix_total_piecewise_payload_exact: i64,
    selected_total_piecewise_payload_exact: isize,
    delta_selected_total_piecewise_payload_exact: i64,
    target_window_count: usize,
    searched_target_window_payload_exact: usize,
    default_target_window_payload_exact: usize,
    best_mix_target_window_payload_exact: usize,
    selected_target_window_payload_exact: usize,
    delta_selected_target_window_payload_exact: i64,
    override_path_bytes_exact: usize,
    selected_override_window_count: usize,
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

pub fn run_apex_lane_law_local_mix_freeze(args: ApexLaneLawLocalMixFreezeArgs) -> Result<()> {
    if args.inputs.is_empty() {
        return Err(anyhow!(
            "apex-lane-law-local-mix-freeze requires at least one --in input"
        ));
    }

    let exe = env::current_exe()
        .context("resolve current executable for apex-lane-law-local-mix-freeze")?;
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
    let dominant = parse_knob_signature(&target_profile.dominant_knob_signature).with_context(|| {
        format!(
            "parse dominant knob signature {} for {}",
            target_profile.dominant_knob_signature, target_profile.global_law_id
        )
    })?;

    let eval_config = build_eval_config(&args, target_profile, &dominant)?;
    let chunk_candidates = select_chunk_candidates(&args, &reports, &shared_law_ids, target_profile, dominant.chunk_bytes)?;

    let temp_dir = make_temp_dir("apex_lane_law_local_mix_freeze")?;
    let mut file_summaries = Vec::<FileSummary>::new();
    let mut all_window_rows = Vec::<WindowEvalRow>::new();
    let mut global_best_chunk_counts = BTreeMap::<usize, usize>::new();
    let mut override_candidates_all = Vec::<OverrideCandidate>::new();
    let mut override_selected_all = Vec::<OverrideCandidate>::new();

    let local_to_global_maps = reports
        .iter()
        .map(|report| {
            report
                .laws
                .iter()
                .map(|law| {
                    let mapped = shared_law_ids
                        .get(&law.law)
                        .cloned()
                        .unwrap_or_else(|| "G?".to_string());
                    (law.local_law_id.clone(), mapped)
                })
                .collect::<BTreeMap<_, _>>()
        })
        .collect::<Vec<_>>();

    let mut default_chunk_scores = BTreeMap::<usize, (usize, usize, usize)>::new();
    let mut eval_cache = BTreeMap::<(String, usize, usize), FrozenEvalRow>::new();

    for (report_idx, report) in reports.iter().enumerate() {
        let input_bytes = fs::read(&report.input)
            .with_context(|| format!("read input for local mix eval {}", report.input))?;
        let local_to_global = &local_to_global_maps[report_idx];
        let mut target_ordinal = 0usize;
        for window in &report.windows {
            let global_law_id = local_to_global
                .get(&window.local_law_id)
                .cloned()
                .unwrap_or_else(|| "G?".to_string());
            if global_law_id != target_profile.global_law_id {
                continue;
            }
            let mut best_payload = usize::MAX;
            let mut best_chunk = dominant.chunk_bytes;
            for &chunk_bytes in &chunk_candidates {
                let eval = eval_window(
                    &exe,
                    &args,
                    &eval_config,
                    &report.input,
                    &input_bytes,
                    window,
                    chunk_bytes,
                    &temp_dir,
                    &mut eval_cache,
                )?;
                if eval.compact_field_total_payload_exact < best_payload
                    || (eval.compact_field_total_payload_exact == best_payload
                        && chunk_bytes < best_chunk)
                {
                    best_payload = eval.compact_field_total_payload_exact;
                    best_chunk = chunk_bytes;
                }
            }
            *global_best_chunk_counts.entry(best_chunk).or_default() += 1;
        }
    }

    let default_chunk_bytes = choose_default_chunk(&args, &chunk_candidates, &global_best_chunk_counts, &reports, &local_to_global_maps, &eval_cache, &target_profile.global_law_id, &temp_dir, &exe, &eval_config, &args)?;

    for (report_idx, report) in reports.iter().enumerate() {
        let input_bytes = fs::read(&report.input)
            .with_context(|| format!("read input for local mix eval {}", report.input))?;
        let local_to_global = &local_to_global_maps[report_idx];
        let mut target_rows = Vec::<WindowEvalRow>::new();
        let mut target_ordinal = 0usize;
        let mut searched_target_window_payload_exact = 0usize;
        let mut default_target_window_payload_exact = 0usize;
        let mut best_mix_target_window_payload_exact = 0usize;

        for window in &report.windows {
            let global_law_id = local_to_global
                .get(&window.local_law_id)
                .cloned()
                .unwrap_or_else(|| "G?".to_string());
            if global_law_id != target_profile.global_law_id {
                continue;
            }
            let default_eval = eval_window(
                &exe,
                &args,
                &eval_config,
                &report.input,
                &input_bytes,
                window,
                default_chunk_bytes,
                &temp_dir,
                &mut eval_cache,
            )?;

            let mut best_eval = default_eval.clone();
            let mut best_chunk_bytes = default_chunk_bytes;
            for &chunk_bytes in &chunk_candidates {
                let eval = eval_window(
                    &exe,
                    &args,
                    &eval_config,
                    &report.input,
                    &input_bytes,
                    window,
                    chunk_bytes,
                    &temp_dir,
                    &mut eval_cache,
                )?;
                if eval.compact_field_total_payload_exact < best_eval.compact_field_total_payload_exact
                    || (eval.compact_field_total_payload_exact == best_eval.compact_field_total_payload_exact
                        && chunk_bytes < best_chunk_bytes)
                {
                    best_eval = eval;
                    best_chunk_bytes = chunk_bytes;
                }
            }

            searched_target_window_payload_exact = searched_target_window_payload_exact
                .saturating_add(window.compact_field_total_payload_exact);
            default_target_window_payload_exact = default_target_window_payload_exact
                .saturating_add(default_eval.compact_field_total_payload_exact);
            best_mix_target_window_payload_exact = best_mix_target_window_payload_exact
                .saturating_add(best_eval.compact_field_total_payload_exact);

            target_rows.push(WindowEvalRow {
                input: report.input.clone(),
                window_idx: window.window_idx,
                target_ordinal,
                start: window.start,
                end: window.end,
                span_bytes: window.span_bytes,
                searched_local_law_id: window.local_law_id.clone(),
                searched_global_law_id: global_law_id,
                searched_chunk_bytes: window.chunk_bytes,
                searched_payload_exact: window.compact_field_total_payload_exact,
                default_payload_exact: default_eval.compact_field_total_payload_exact,
                best_chunk_bytes,
                best_payload_exact: best_eval.compact_field_total_payload_exact,
                selected_chunk_bytes: default_chunk_bytes,
                selected_payload_exact: default_eval.compact_field_total_payload_exact,
                default_gain_exact: (window.compact_field_total_payload_exact as i64)
                    - (default_eval.compact_field_total_payload_exact as i64),
                best_gain_exact: (window.compact_field_total_payload_exact as i64)
                    - (best_eval.compact_field_total_payload_exact as i64),
                selected_gain_exact: (window.compact_field_total_payload_exact as i64)
                    - (default_eval.compact_field_total_payload_exact as i64),
            });
            target_ordinal += 1;
        }

        let override_candidates = target_rows
            .iter()
            .filter_map(|row| {
                if row.best_chunk_bytes != default_chunk_bytes && row.best_payload_exact < row.default_payload_exact {
                    Some(OverrideCandidate {
                        input: row.input.clone(),
                        window_idx: row.window_idx,
                        target_ordinal: row.target_ordinal,
                        best_chunk_bytes: row.best_chunk_bytes,
                        default_payload_exact: row.default_payload_exact,
                        best_payload_exact: row.best_payload_exact,
                        gain_exact: row.default_payload_exact.saturating_sub(row.best_payload_exact),
                    })
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let selected_override_indices = select_override_subset(
            &override_candidates,
            args.exact_subset_limit,
            args.min_override_gain_exact,
        );
        let selected_overrides = selected_override_indices
            .iter()
            .map(|&idx| override_candidates[idx].clone())
            .collect::<Vec<_>>();
        let selected_override_map = selected_overrides
            .iter()
            .map(|row| (row.window_idx, row.best_chunk_bytes))
            .collect::<BTreeMap<_, _>>();

        let override_path_bytes_exact = override_path_bytes(&selected_overrides);
        let mut selected_target_window_payload_exact = 0usize;
        let mut improved_target_window_count = 0usize;
        let mut equal_target_window_count = 0usize;
        let mut worsened_target_window_count = 0usize;

        for row in &mut target_rows {
            if let Some(chunk) = selected_override_map.get(&row.window_idx).copied() {
                row.selected_chunk_bytes = chunk;
                if chunk == row.best_chunk_bytes {
                    row.selected_payload_exact = row.best_payload_exact;
                    row.selected_gain_exact = row.best_gain_exact;
                }
            }
            selected_target_window_payload_exact = selected_target_window_payload_exact
                .saturating_add(row.selected_payload_exact);
            let delta = (row.selected_payload_exact as i64) - (row.searched_payload_exact as i64);
            match delta.cmp(&0) {
                std::cmp::Ordering::Less => improved_target_window_count += 1,
                std::cmp::Ordering::Equal => equal_target_window_count += 1,
                std::cmp::Ordering::Greater => worsened_target_window_count += 1,
            }
            all_window_rows.push(row.clone());
        }

        override_candidates_all.extend(override_candidates.clone());
        override_selected_all.extend(selected_overrides.clone());

        let projected_default_total_piecewise_payload_exact =
            (report.total_piecewise_payload_exact as isize)
                + (default_target_window_payload_exact as isize)
                - (searched_target_window_payload_exact as isize);
        let projected_unpriced_best_mix_total_piecewise_payload_exact =
            (report.total_piecewise_payload_exact as isize)
                + (best_mix_target_window_payload_exact as isize)
                - (searched_target_window_payload_exact as isize);
        let selected_total_piecewise_payload_exact =
            projected_unpriced_best_mix_total_piecewise_payload_exact
                + (selected_target_window_payload_exact as isize)
                - (best_mix_target_window_payload_exact as isize)
                + (override_path_bytes_exact as isize);

        file_summaries.push(FileSummary {
            input: report.input.clone(),
            searched_total_piecewise_payload_exact: report.total_piecewise_payload_exact,
            projected_default_total_piecewise_payload_exact,
            delta_default_total_piecewise_payload_exact: projected_default_total_piecewise_payload_exact as i64 - report.total_piecewise_payload_exact as i64,
            projected_unpriced_best_mix_total_piecewise_payload_exact,
            delta_unpriced_best_mix_total_piecewise_payload_exact: projected_unpriced_best_mix_total_piecewise_payload_exact as i64 - report.total_piecewise_payload_exact as i64,
            selected_total_piecewise_payload_exact,
            delta_selected_total_piecewise_payload_exact: selected_total_piecewise_payload_exact as i64 - report.total_piecewise_payload_exact as i64,
            target_window_count: target_rows.len(),
            searched_target_window_payload_exact,
            default_target_window_payload_exact,
            best_mix_target_window_payload_exact,
            selected_target_window_payload_exact,
            delta_selected_target_window_payload_exact: selected_target_window_payload_exact as i64 - searched_target_window_payload_exact as i64,
            override_path_bytes_exact,
            selected_override_window_count: selected_overrides.len(),
            improved_target_window_count,
            equal_target_window_count,
            worsened_target_window_count,
        });
    }

    if !args.keep_temp_dir {
        if let Err(err) = fs::remove_dir_all(&temp_dir) {
            eprintln!(
                "apex-lane-law-local-mix-freeze: warning could not remove temp_dir={} err={}",
                temp_dir.display(),
                err
            );
        }
    } else {
        eprintln!(
            "apex-lane-law-local-mix-freeze: temp_dir={}",
            temp_dir.display()
        );
    }

    let best_gain = all_window_rows
        .iter()
        .filter(|row| row.selected_gain_exact > 0)
        .max_by_key(|row| row.selected_gain_exact);
    let worst_loss = all_window_rows
        .iter()
        .filter(|row| row.selected_gain_exact < 0)
        .min_by_key(|row| row.selected_gain_exact);

    let default_local_chunk_window_wins = *global_best_chunk_counts.get(&default_chunk_bytes).unwrap_or(&0);
    let summary = LocalMixSummary {
        recipe: reports.first().map(|r| r.recipe.clone()).unwrap_or_else(|| args.recipe.clone()),
        file_count: reports.len(),
        honest_file_count: reports.iter().filter(|r| r.honest_non_overlapping).count(),
        union_law_count: shared_law_ids.len(),
        target_global_law_id: target_profile.global_law_id.clone(),
        target_global_law_path_hits: target_profile.path_hits,
        target_global_law_file_count: target_profile.file_count,
        target_global_law_total_window_count: target_profile.total_window_count,
        target_global_law_total_segment_count: target_profile.total_segment_count,
        target_global_law_total_covered_bytes: target_profile.total_covered_bytes,
        target_global_law_dominant_knob_signature: target_profile.dominant_knob_signature.clone(),
        eval_boundary_band: eval_config.law.boundary_band,
        eval_field_margin: eval_config.law.field_margin,
        eval_newline_demote_margin: eval_config.law.newline_demote_margin,
        eval_chunk_search_objective: eval_config.search.chunk_search_objective.clone(),
        eval_chunk_raw_slack: eval_config.search.chunk_raw_slack,
        eval_chunk_candidates: chunk_candidates.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(","),
        eval_chunk_candidate_count: chunk_candidates.len(),
        default_local_chunk_bytes: default_chunk_bytes,
        default_local_chunk_window_wins,
        searched_total_piecewise_payload_exact: file_summaries.iter().map(|f| f.searched_total_piecewise_payload_exact).sum(),
        projected_default_total_piecewise_payload_exact: file_summaries.iter().map(|f| f.projected_default_total_piecewise_payload_exact).sum(),
        delta_default_total_piecewise_payload_exact: file_summaries.iter().map(|f| f.delta_default_total_piecewise_payload_exact).sum(),
        projected_unpriced_best_mix_total_piecewise_payload_exact: file_summaries.iter().map(|f| f.projected_unpriced_best_mix_total_piecewise_payload_exact).sum(),
        delta_unpriced_best_mix_total_piecewise_payload_exact: file_summaries.iter().map(|f| f.delta_unpriced_best_mix_total_piecewise_payload_exact).sum(),
        selected_total_piecewise_payload_exact: file_summaries.iter().map(|f| f.selected_total_piecewise_payload_exact).sum(),
        delta_selected_total_piecewise_payload_exact: file_summaries.iter().map(|f| f.delta_selected_total_piecewise_payload_exact).sum(),
        target_window_count: file_summaries.iter().map(|f| f.target_window_count).sum(),
        searched_target_window_payload_exact: file_summaries.iter().map(|f| f.searched_target_window_payload_exact).sum(),
        default_target_window_payload_exact: file_summaries.iter().map(|f| f.default_target_window_payload_exact).sum(),
        best_mix_target_window_payload_exact: file_summaries.iter().map(|f| f.best_mix_target_window_payload_exact).sum(),
        selected_target_window_payload_exact: file_summaries.iter().map(|f| f.selected_target_window_payload_exact).sum(),
        delta_selected_target_window_payload_exact: file_summaries.iter().map(|f| f.delta_selected_target_window_payload_exact).sum(),
        override_path_bytes_exact: file_summaries.iter().map(|f| f.override_path_bytes_exact).sum(),
        selected_override_window_count: file_summaries.iter().map(|f| f.selected_override_window_count).sum(),
        improved_target_window_count: file_summaries.iter().map(|f| f.improved_target_window_count).sum(),
        equal_target_window_count: file_summaries.iter().map(|f| f.equal_target_window_count).sum(),
        worsened_target_window_count: file_summaries.iter().map(|f| f.worsened_target_window_count).sum(),
        best_gain_input: best_gain.map(|row| row.input.clone()).unwrap_or_default(),
        best_gain_window_idx: best_gain.map(|row| row.window_idx).unwrap_or(0),
        best_gain_delta_payload_exact: best_gain.map(|row| row.selected_gain_exact).unwrap_or(0),
        worst_loss_input: worst_loss.map(|row| row.input.clone()).unwrap_or_default(),
        worst_loss_window_idx: worst_loss.map(|row| row.window_idx).unwrap_or(0),
        worst_loss_delta_payload_exact: worst_loss.map(|row| row.selected_gain_exact).unwrap_or(0),
    };

    let body = match args.format {
        RenderFormat::Txt => render_txt(&summary, &file_summaries, &all_window_rows, &override_candidates_all, &override_selected_all, args.top_rows),
        RenderFormat::Csv => render_csv(&summary, &file_summaries, &all_window_rows, &override_candidates_all, &override_selected_all),
    };
    write_or_print(args.out.as_deref(), &body)?;

    if let Some(path) = args.out.as_deref() {
        eprintln!(
            "apex-lane-law-local-mix-freeze: out={} files={} target={} searched_total_piecewise_payload_exact={} selected_total_piecewise_payload_exact={} delta_selected_total_piecewise_payload_exact={}",
            path,
            summary.file_count,
            summary.target_global_law_id,
            summary.searched_total_piecewise_payload_exact,
            summary.selected_total_piecewise_payload_exact,
            summary.delta_selected_total_piecewise_payload_exact,
        );
    } else {
        eprintln!(
            "apex-lane-law-local-mix-freeze: files={} target={} searched_total_piecewise_payload_exact={} selected_total_piecewise_payload_exact={} delta_selected_total_piecewise_payload_exact={}",
            summary.file_count,
            summary.target_global_law_id,
            summary.searched_total_piecewise_payload_exact,
            summary.selected_total_piecewise_payload_exact,
            summary.delta_selected_total_piecewise_payload_exact,
        );
    }

    Ok(())
}

fn select_target_profile<'a>(profiles: &'a [LawProfile], global_law_id: Option<&str>) -> Result<&'a LawProfile> {
    if profiles.is_empty() {
        return Err(anyhow!("apex-lane-law-local-mix-freeze found no shared laws to evaluate"));
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
        .ok_or_else(|| anyhow!("apex-lane-law-local-mix-freeze could not select dominant law"))
}

fn build_eval_config(
    args: &ApexLaneLawLocalMixFreezeArgs,
    target: &LawProfile,
    dominant: &SearchKnobTuple,
) -> Result<EvalConfig> {
    let search = SearchKnobTuple {
        chunk_bytes: dominant.chunk_bytes,
        chunk_search_objective: args
            .local_chunk_search_objective
            .map(chunk_search_objective_name)
            .unwrap_or(dominant.chunk_search_objective.as_str())
            .to_string(),
        chunk_raw_slack: args.local_chunk_raw_slack.unwrap_or(dominant.chunk_raw_slack),
    };
    let law = ReplayLawTuple {
        boundary_band: args.freeze_boundary_band.unwrap_or(target.law.boundary_band),
        field_margin: args.freeze_field_margin.unwrap_or(target.law.field_margin),
        newline_demote_margin: args
            .freeze_newline_demote_margin
            .unwrap_or(target.law.newline_demote_margin),
    };
    Ok(EvalConfig { law, search })
}

fn parse_usize_list(raw: &str) -> Result<Vec<usize>> {
    let mut out = raw
        .split(',')
        .filter_map(|part| {
            let t = part.trim();
            if t.is_empty() { None } else { Some(t) }
        })
        .map(|part| part.parse::<usize>().with_context(|| format!("parse usize from {}", part)))
        .collect::<Result<Vec<_>>>()?;
    out.sort_unstable();
    out.dedup();
    if out.is_empty() {
        return Err(anyhow!("empty usize list"));
    }
    Ok(out)
}

fn select_chunk_candidates(
    args: &ApexLaneLawLocalMixFreezeArgs,
    reports: &[FileReport],
    shared_law_ids: &BTreeMap<ReplayLawTuple, String>,
    target: &LawProfile,
    dominant_chunk_bytes: usize,
) -> Result<Vec<usize>> {
    let mut out = parse_usize_list(&args.local_chunk_sweep)?;
    out.push(dominant_chunk_bytes);
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
                .map(|id| id == &target.global_law_id)
                .unwrap_or(false)
            {
                out.push(window.chunk_bytes);
            }
        }
    }
    out.sort_unstable();
    out.dedup();
    Ok(out)
}

fn eval_window(
    exe: &Path,
    args: &ApexLaneLawLocalMixFreezeArgs,
    eval_config: &EvalConfig,
    input_name: &str,
    input_bytes: &[u8],
    window: &ManifestWindowRow,
    chunk_bytes: usize,
    temp_dir: &Path,
    cache: &mut BTreeMap<(String, usize, usize), FrozenEvalRow>,
) -> Result<FrozenEvalRow> {
    let key = (input_name.to_string(), window.window_idx, chunk_bytes);
    if let Some(row) = cache.get(&key) {
        return Ok(row.clone());
    }
    let slice = &input_bytes[window.start..window.end];
    let window_path = temp_dir.join(format!(
        "local_mix_{}_window_{:04}_{:08}_{:08}_chunk_{}.bin",
        sanitize_file_stem(input_name),
        window.window_idx,
        window.start,
        window.end,
        chunk_bytes
    ));
    fs::write(&window_path, slice)
        .with_context(|| format!("write local mix slice {}", window_path.display()))?;
    let mut per_chunk = eval_config.clone();
    per_chunk.search.chunk_bytes = chunk_bytes;
    let row = run_child_frozen_apex_map_lane(exe, args, &per_chunk, &window_path).with_context(|| {
        format!(
            "run local mix frozen apex-map-lane input={} window_idx={} chunk_bytes={}",
            input_name, window.window_idx, chunk_bytes
        )
    })?;
    cache.insert(key, row.clone());
    Ok(row)
}

fn choose_default_chunk(
    args: &ApexLaneLawLocalMixFreezeArgs,
    chunk_candidates: &[usize],
    best_chunk_counts: &BTreeMap<usize, usize>,
    reports: &[FileReport],
    local_to_global_maps: &[BTreeMap<String, String>],
    eval_cache: &BTreeMap<(String, usize, usize), FrozenEvalRow>,
    target_global_law_id: &str,
    temp_dir: &Path,
    exe: &Path,
    eval_config: &EvalConfig,
    eval_args: &ApexLaneLawLocalMixFreezeArgs,
) -> Result<usize> {
    if let Some(explicit) = args.default_local_chunk_bytes {
        return Ok(explicit);
    }
    let mut best_choice: Option<(usize, usize, usize, usize)> = None;
    for &chunk_bytes in chunk_candidates {
        let mut total_payload = 0usize;
        let mut improved = 0usize;
        let mut wins = *best_chunk_counts.get(&chunk_bytes).unwrap_or(&0);
        for (report_idx, report) in reports.iter().enumerate() {
            let input_bytes = fs::read(&report.input)
                .with_context(|| format!("read input for default chunk scan {}", report.input))?;
            let local_to_global = &local_to_global_maps[report_idx];
            for window in &report.windows {
                if local_to_global
                    .get(&window.local_law_id)
                    .map(|id| id == target_global_law_id)
                    .unwrap_or(false)
                {
                    let eval = if let Some(cached) = eval_cache.get(&(report.input.clone(), window.window_idx, chunk_bytes)) {
                        cached.clone()
                    } else {
                        eval_window(exe, eval_args, eval_config, &report.input, &input_bytes, window, chunk_bytes, temp_dir, &mut BTreeMap::new())?
                    };
                    total_payload = total_payload.saturating_add(eval.compact_field_total_payload_exact);
                    if eval.compact_field_total_payload_exact < window.compact_field_total_payload_exact {
                        improved += 1;
                    }
                }
            }
        }
        let candidate = (wins, usize::MAX - improved, usize::MAX - total_payload, usize::MAX - chunk_bytes);
        match best_choice {
            None => best_choice = Some((chunk_bytes, wins, improved, total_payload)),
            Some((cur_chunk, cur_wins, cur_improved, cur_total)) => {
                if wins > cur_wins
                    || (wins == cur_wins && total_payload < cur_total)
                    || (wins == cur_wins && total_payload == cur_total && improved > cur_improved)
                    || (wins == cur_wins && total_payload == cur_total && improved == cur_improved && chunk_bytes < cur_chunk)
                {
                    best_choice = Some((chunk_bytes, wins, improved, total_payload));
                }
            }
        }
        let _ = candidate;
    }
    best_choice
        .map(|v| v.0)
        .ok_or_else(|| anyhow!("could not choose default local chunk"))
}

fn select_override_subset(
    candidates: &[OverrideCandidate],
    exact_subset_limit: usize,
    min_override_gain_exact: usize,
) -> Vec<usize> {
    let filtered = candidates
        .iter()
        .enumerate()
        .filter(|(_, row)| row.gain_exact >= min_override_gain_exact)
        .map(|(idx, _)| idx)
        .collect::<Vec<_>>();

    if filtered.len() <= exact_subset_limit && filtered.len() <= 20 {
        return select_override_subset_exact(candidates, &filtered);
    }
    select_override_subset_greedy(candidates, &filtered)
}

fn select_override_subset_exact(candidates: &[OverrideCandidate], filtered: &[usize]) -> Vec<usize> {
    let mut best = Vec::<usize>::new();
    let mut best_total = usize::MAX;
    let total_masks = 1usize.checked_shl(filtered.len() as u32).unwrap_or(0);
    for mask in 0..total_masks {
        let mut subset = Vec::<usize>::new();
        let mut gain = 0usize;
        for (bit, idx) in filtered.iter().enumerate() {
            if ((mask >> bit) & 1) == 1 {
                subset.push(*idx);
                gain = gain.saturating_add(candidates[*idx].gain_exact);
            }
        }
        let chosen = subset.iter().map(|idx| candidates[*idx].clone()).collect::<Vec<_>>();
        let path_bytes = override_path_bytes(&chosen);
        let default_payload = candidates.iter().map(|row| row.default_payload_exact).sum::<usize>();
        let total = default_payload.saturating_sub(gain).saturating_add(path_bytes);
        if total < best_total || (total == best_total && subset.len() < best.len()) {
            best_total = total;
            best = subset;
        }
    }
    best.sort_unstable();
    best
}

fn select_override_subset_greedy(candidates: &[OverrideCandidate], filtered: &[usize]) -> Vec<usize> {
    let mut order = filtered.to_vec();
    order.sort_by_key(|idx| std::cmp::Reverse(candidates[*idx].gain_exact));
    let mut chosen = Vec::<usize>::new();
    let default_payload = candidates.iter().map(|row| row.default_payload_exact).sum::<usize>();
    let mut current_total = default_payload;
    for idx in order {
        let mut trial = chosen.clone();
        trial.push(idx);
        let chosen_rows = trial.iter().map(|i| candidates[*i].clone()).collect::<Vec<_>>();
        let gain = trial.iter().map(|i| candidates[*i].gain_exact).sum::<usize>();
        let total = default_payload.saturating_sub(gain).saturating_add(override_path_bytes(&chosen_rows));
        if total < current_total {
            chosen = trial;
            current_total = total;
        }
    }
    chosen.sort_unstable();
    chosen
}

fn override_path_bytes(rows: &[OverrideCandidate]) -> usize {
    if rows.is_empty() {
        return 0;
    }
    let mut bytes = Vec::new();
    let mut prev = 0usize;
    let mut ordered = rows.to_vec();
    ordered.sort_by_key(|row| row.window_idx);
    for row in ordered {
        let delta = row.window_idx.saturating_sub(prev);
        put_varint(delta as u64, &mut bytes);
        put_varint(row.best_chunk_bytes as u64, &mut bytes);
        prev = row.window_idx;
    }
    bytes.len()
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
    args: &ApexLaneLawLocalMixFreezeArgs,
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
    args: &ApexLaneLawLocalMixFreezeArgs,
    freeze: &EvalConfig,
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
        .arg(freeze.search.chunk_bytes.to_string())
        .arg("--chunk-sweep")
        .arg(freeze.search.chunk_bytes.to_string())
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
    summary: &LocalMixSummary,
    file_summaries: &[FileSummary],
    window_rows: &[WindowEvalRow],
    override_candidates: &[OverrideCandidate],
    override_selected: &[OverrideCandidate],
    top_rows: usize,
) -> String {
    let mut out = String::new();
    push_line(&mut out, "recipe", &summary.recipe);
    push_line(&mut out, "file_count", summary.file_count);
    push_line(&mut out, "honest_file_count", summary.honest_file_count);
    push_line(&mut out, "union_law_count", summary.union_law_count);
    push_line(&mut out, "target_global_law_id", &summary.target_global_law_id);
    push_line(&mut out, "target_global_law_path_hits", summary.target_global_law_path_hits);
    push_line(&mut out, "target_global_law_file_count", summary.target_global_law_file_count);
    push_line(&mut out, "target_global_law_total_window_count", summary.target_global_law_total_window_count);
    push_line(&mut out, "target_global_law_total_segment_count", summary.target_global_law_total_segment_count);
    push_line(&mut out, "target_global_law_total_covered_bytes", summary.target_global_law_total_covered_bytes);
    push_line(&mut out, "target_global_law_dominant_knob_signature", summary.target_global_law_dominant_knob_signature.replace(' ', "|"));
    push_line(&mut out, "eval_boundary_band", summary.eval_boundary_band);
    push_line(&mut out, "eval_field_margin", summary.eval_field_margin);
    push_line(&mut out, "eval_newline_demote_margin", summary.eval_newline_demote_margin);
    push_line(&mut out, "eval_chunk_search_objective", &summary.eval_chunk_search_objective);
    push_line(&mut out, "eval_chunk_raw_slack", summary.eval_chunk_raw_slack);
    push_line(&mut out, "eval_chunk_candidates", &summary.eval_chunk_candidates);
    push_line(&mut out, "eval_chunk_candidate_count", summary.eval_chunk_candidate_count);
    push_line(&mut out, "default_local_chunk_bytes", summary.default_local_chunk_bytes);
    push_line(&mut out, "default_local_chunk_window_wins", summary.default_local_chunk_window_wins);
    push_line(&mut out, "searched_total_piecewise_payload_exact", summary.searched_total_piecewise_payload_exact);
    push_line(&mut out, "projected_default_total_piecewise_payload_exact", summary.projected_default_total_piecewise_payload_exact);
    push_line(&mut out, "delta_default_total_piecewise_payload_exact", summary.delta_default_total_piecewise_payload_exact);
    push_line(&mut out, "projected_unpriced_best_mix_total_piecewise_payload_exact", summary.projected_unpriced_best_mix_total_piecewise_payload_exact);
    push_line(&mut out, "delta_unpriced_best_mix_total_piecewise_payload_exact", summary.delta_unpriced_best_mix_total_piecewise_payload_exact);
    push_line(&mut out, "selected_total_piecewise_payload_exact", summary.selected_total_piecewise_payload_exact);
    push_line(&mut out, "delta_selected_total_piecewise_payload_exact", summary.delta_selected_total_piecewise_payload_exact);
    push_line(&mut out, "target_window_count", summary.target_window_count);
    push_line(&mut out, "searched_target_window_payload_exact", summary.searched_target_window_payload_exact);
    push_line(&mut out, "default_target_window_payload_exact", summary.default_target_window_payload_exact);
    push_line(&mut out, "best_mix_target_window_payload_exact", summary.best_mix_target_window_payload_exact);
    push_line(&mut out, "selected_target_window_payload_exact", summary.selected_target_window_payload_exact);
    push_line(&mut out, "delta_selected_target_window_payload_exact", summary.delta_selected_target_window_payload_exact);
    push_line(&mut out, "override_path_bytes_exact", summary.override_path_bytes_exact);
    push_line(&mut out, "selected_override_window_count", summary.selected_override_window_count);
    push_line(&mut out, "improved_target_window_count", summary.improved_target_window_count);
    push_line(&mut out, "equal_target_window_count", summary.equal_target_window_count);
    push_line(&mut out, "worsened_target_window_count", summary.worsened_target_window_count);
    push_line(&mut out, "best_gain_input", &summary.best_gain_input);
    push_line(&mut out, "best_gain_window_idx", summary.best_gain_window_idx);
    push_line(&mut out, "best_gain_delta_payload_exact", summary.best_gain_delta_payload_exact);
    push_line(&mut out, "worst_loss_input", &summary.worst_loss_input);
    push_line(&mut out, "worst_loss_window_idx", summary.worst_loss_window_idx);
    push_line(&mut out, "worst_loss_delta_payload_exact", summary.worst_loss_delta_payload_exact);

    out.push_str("
--- files ---
");
    for file in file_summaries {
        out.push_str(&format!(
            "input={} searched_total_piecewise_payload_exact={} projected_default_total_piecewise_payload_exact={} delta_default_total_piecewise_payload_exact={} projected_unpriced_best_mix_total_piecewise_payload_exact={} delta_unpriced_best_mix_total_piecewise_payload_exact={} selected_total_piecewise_payload_exact={} delta_selected_total_piecewise_payload_exact={} target_window_count={} searched_target_window_payload_exact={} default_target_window_payload_exact={} best_mix_target_window_payload_exact={} selected_target_window_payload_exact={} delta_selected_target_window_payload_exact={} override_path_bytes_exact={} selected_override_window_count={} improved_target_window_count={} equal_target_window_count={} worsened_target_window_count={}
",
            file.input,
            file.searched_total_piecewise_payload_exact,
            file.projected_default_total_piecewise_payload_exact,
            file.delta_default_total_piecewise_payload_exact,
            file.projected_unpriced_best_mix_total_piecewise_payload_exact,
            file.delta_unpriced_best_mix_total_piecewise_payload_exact,
            file.selected_total_piecewise_payload_exact,
            file.delta_selected_total_piecewise_payload_exact,
            file.target_window_count,
            file.searched_target_window_payload_exact,
            file.default_target_window_payload_exact,
            file.best_mix_target_window_payload_exact,
            file.selected_target_window_payload_exact,
            file.delta_selected_target_window_payload_exact,
            file.override_path_bytes_exact,
            file.selected_override_window_count,
            file.improved_target_window_count,
            file.equal_target_window_count,
            file.worsened_target_window_count,
        ));
    }

    let mut candidate_rows = override_candidates.to_vec();
    candidate_rows.sort_by_key(|row| std::cmp::Reverse(row.gain_exact));
    out.push_str("
--- top-override-candidates ---
");
    for row in candidate_rows.into_iter().take(top_rows) {
        out.push_str(&format!(
            "input={} window_idx={} target_ordinal={} best_chunk_bytes={} default_payload_exact={} best_payload_exact={} gain_exact={}
",
            row.input, row.window_idx, row.target_ordinal, row.best_chunk_bytes, row.default_payload_exact, row.best_payload_exact, row.gain_exact
        ));
    }

    let mut selected_rows = override_selected.to_vec();
    selected_rows.sort_by_key(|row| row.window_idx);
    out.push_str("
--- selected-overrides ---
");
    for row in selected_rows {
        out.push_str(&format!(
            "input={} window_idx={} target_ordinal={} best_chunk_bytes={} default_payload_exact={} best_payload_exact={} gain_exact={}
",
            row.input, row.window_idx, row.target_ordinal, row.best_chunk_bytes, row.default_payload_exact, row.best_payload_exact, row.gain_exact
        ));
    }

    let mut deltas = window_rows.to_vec();
    deltas.sort_by_key(|row| (std::cmp::Reverse(row.selected_gain_exact), row.window_idx));
    out.push_str("
--- top-window-gains ---
");
    for row in deltas.into_iter().filter(|row| row.selected_gain_exact > 0).take(top_rows) {
        out.push_str(&format!(
            "input={} window_idx={} target_ordinal={} searched_payload_exact={} default_payload_exact={} best_payload_exact={} selected_payload_exact={} searched_chunk_bytes={} default_chunk_bytes={} best_chunk_bytes={} selected_chunk_bytes={} selected_gain_exact={}
",
            row.input, row.window_idx, row.target_ordinal, row.searched_payload_exact, row.default_payload_exact, row.best_payload_exact, row.selected_payload_exact, row.searched_chunk_bytes, summary.default_local_chunk_bytes, row.best_chunk_bytes, row.selected_chunk_bytes, row.selected_gain_exact
        ));
    }
    out
}

fn render_csv(
    summary: &LocalMixSummary,
    file_summaries: &[FileSummary],
    window_rows: &[WindowEvalRow],
    override_candidates: &[OverrideCandidate],
    override_selected: &[OverrideCandidate],
) -> String {
    let mut out = String::new();
    push_csv_row(&mut out, &[
        "section","recipe","file_count","honest_file_count","union_law_count","target_global_law_id","default_local_chunk_bytes","searched_total_piecewise_payload_exact","projected_default_total_piecewise_payload_exact","projected_unpriced_best_mix_total_piecewise_payload_exact","selected_total_piecewise_payload_exact","override_path_bytes_exact"
    ]);
    push_csv_row(&mut out, &[
        "summary",
        &summary.recipe,
        &summary.file_count.to_string(),
        &summary.honest_file_count.to_string(),
        &summary.union_law_count.to_string(),
        &summary.target_global_law_id,
        &summary.default_local_chunk_bytes.to_string(),
        &summary.searched_total_piecewise_payload_exact.to_string(),
        &summary.projected_default_total_piecewise_payload_exact.to_string(),
        &summary.projected_unpriced_best_mix_total_piecewise_payload_exact.to_string(),
        &summary.selected_total_piecewise_payload_exact.to_string(),
        &summary.override_path_bytes_exact.to_string(),
    ]);
    push_csv_row(&mut out, &["section","input","searched_total_piecewise_payload_exact","projected_default_total_piecewise_payload_exact","projected_unpriced_best_mix_total_piecewise_payload_exact","selected_total_piecewise_payload_exact","target_window_count","override_path_bytes_exact","selected_override_window_count"]);
    for file in file_summaries {
        push_csv_row(&mut out, &[
            "file",
            &file.input,
            &file.searched_total_piecewise_payload_exact.to_string(),
            &file.projected_default_total_piecewise_payload_exact.to_string(),
            &file.projected_unpriced_best_mix_total_piecewise_payload_exact.to_string(),
            &file.selected_total_piecewise_payload_exact.to_string(),
            &file.target_window_count.to_string(),
            &file.override_path_bytes_exact.to_string(),
            &file.selected_override_window_count.to_string(),
        ]);
    }
    push_csv_row(&mut out, &["section","input","window_idx","target_ordinal","searched_payload_exact","default_payload_exact","best_payload_exact","selected_payload_exact","searched_chunk_bytes","best_chunk_bytes","selected_chunk_bytes","selected_gain_exact"]);
    for row in window_rows {
        push_csv_row(&mut out, &[
            "window",
            &row.input,
            &row.window_idx.to_string(),
            &row.target_ordinal.to_string(),
            &row.searched_payload_exact.to_string(),
            &row.default_payload_exact.to_string(),
            &row.best_payload_exact.to_string(),
            &row.selected_payload_exact.to_string(),
            &row.searched_chunk_bytes.to_string(),
            &row.best_chunk_bytes.to_string(),
            &row.selected_chunk_bytes.to_string(),
            &row.selected_gain_exact.to_string(),
        ]);
    }
    push_csv_row(&mut out, &["section","input","window_idx","target_ordinal","best_chunk_bytes","default_payload_exact","best_payload_exact","gain_exact"]);
    for row in override_candidates {
        push_csv_row(&mut out, &[
            "override_candidate",
            &row.input,
            &row.window_idx.to_string(),
            &row.target_ordinal.to_string(),
            &row.best_chunk_bytes.to_string(),
            &row.default_payload_exact.to_string(),
            &row.best_payload_exact.to_string(),
            &row.gain_exact.to_string(),
        ]);
    }
    for row in override_selected {
        push_csv_row(&mut out, &[
            "override_selected",
            &row.input,
            &row.window_idx.to_string(),
            &row.target_ordinal.to_string(),
            &row.best_chunk_bytes.to_string(),
            &row.default_payload_exact.to_string(),
            &row.best_payload_exact.to_string(),
            &row.gain_exact.to_string(),
        ]);
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
    use super::{override_path_bytes, select_override_subset_exact, OverrideCandidate};

    #[test]
    fn override_path_bytes_is_deterministic() {
        let rows = vec![
            OverrideCandidate { input: "a".into(), window_idx: 3, target_ordinal: 1, best_chunk_bytes: 96, default_payload_exact: 100, best_payload_exact: 90, gain_exact: 10 },
            OverrideCandidate { input: "a".into(), window_idx: 8, target_ordinal: 2, best_chunk_bytes: 64, default_payload_exact: 100, best_payload_exact: 95, gain_exact: 5 },
        ];
        assert_eq!(override_path_bytes(&rows), override_path_bytes(&rows));
        assert!(override_path_bytes(&rows) > 0);
    }

    #[test]
    fn exact_subset_skips_small_gain_when_path_cost_eats_it() {
        let rows = vec![
            OverrideCandidate { input: "a".into(), window_idx: 1, target_ordinal: 0, best_chunk_bytes: 96, default_payload_exact: 100, best_payload_exact: 97, gain_exact: 3 },
            OverrideCandidate { input: "a".into(), window_idx: 1000, target_ordinal: 1, best_chunk_bytes: 128, default_payload_exact: 100, best_payload_exact: 99, gain_exact: 1 },
        ];
        let picked = select_override_subset_exact(&rows, &[0,1]);
        assert!(picked.contains(&0));
        assert!(!picked.contains(&1));
    }
}
