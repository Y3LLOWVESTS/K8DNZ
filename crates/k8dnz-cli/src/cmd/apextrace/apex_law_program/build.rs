use anyhow::{anyhow, bail, Context, Result};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use super::args::BuildArgs;
use super::exec::{run_local_mix, run_manifest_txt};
use super::parse::{
    parse_csv_sections, parse_csv_usize_list, parse_manifest_positions, parse_required_i64,
    parse_required_string, parse_required_u64, parse_required_usize, parse_txt_summary,
};
use super::plan::select_override_plan;
use super::replay::canonicalize_artifact_selected_payloads;
use super::types::{
    BodyCandidateScore, BodySelectObjective, BuildMaterialized, LawProgramArtifact,
    ManifestWindowPos, OverrideCandidateRef, ProgramFile, ProgramOverride, ProgramSummary,
    ProgramWindow, ReplayConfig,
};

pub(crate) fn materialize_build(cli_exe: &Path, args: &BuildArgs) -> Result<BuildMaterialized> {
    let objective = BodySelectObjective::parse(&args.body_select_objective)?;
    let body_candidates = select_body_candidates(args)?;
    let manifest_positions = load_manifest_positions(cli_exe, args)?;
    let mut materials = Vec::<BuildMaterialized>::new();

    for (idx, chunk_bytes) in body_candidates.iter().enumerate() {
        let mut trial_args = args.clone();
        trial_args.default_local_chunk_bytes = Some(*chunk_bytes);

        eprintln!(
            "apex-law-program build: body candidate {}/{} chunk_bytes={} objective={}",
            idx + 1,
            body_candidates.len(),
            chunk_bytes,
            args.body_select_objective,
        );

        let txt_out = run_local_mix(cli_exe, &trial_args, "txt")?;
        let csv_out = run_local_mix(cli_exe, &trial_args, "csv")?;
        let txt_summary = parse_txt_summary(&txt_out.stdout)?;
        let csv_sections = parse_csv_sections(&csv_out.stdout)?;

        let mut artifact = build_artifact_from_outputs(
            &trial_args,
            objective,
            &txt_summary,
            &csv_sections,
            &manifest_positions,
        )?;

        eprintln!(
            "apex-law-program build: canonical replay for chunk_bytes={} windows={}",
            chunk_bytes,
            artifact.windows.len()
        );
        canonicalize_artifact_selected_payloads(cli_exe, &mut artifact).with_context(|| {
            format!(
                "canonicalize artifact selected payloads chunk_bytes={}",
                chunk_bytes
            )
        })?;

        materials.push(BuildMaterialized {
            body_scores: vec![body_score_from_artifact(&artifact)],
            artifact,
        });
    }

    let best_idx = select_best_materialized_index(&materials, objective)?;
    let mut best = materials.remove(best_idx);

    let mut body_scores = materials
        .into_iter()
        .map(|m| body_score_from_artifact(&m.artifact))
        .collect::<Vec<_>>();

    body_scores.push(body_score_from_artifact(&best.artifact));
    body_scores.sort_by_key(|row| row.chunk_bytes);
    best.body_scores = body_scores;

    Ok(best)
}

fn body_score_from_artifact(artifact: &LawProgramArtifact) -> BodyCandidateScore {
    let summary = &artifact.summary;

    BodyCandidateScore {
        chunk_bytes: summary.default_local_chunk_bytes,
        selected_total_piecewise_payload_exact: summary.selected_total_piecewise_payload_exact,
        closure_total_exact: summary.closure_total_exact,
        closure_penalty_exact: summary.closure_penalty_exact,
        mode_penalty_exact: summary.closure_mode_penalty_exact,
        selected_target_window_payload_exact: summary.selected_target_window_payload_exact,
        selected_override_window_count: summary.selected_override_window_count,
        override_run_count: summary.closure_override_run_count,
        max_override_run_length: summary.closure_max_override_run_length,
        override_path_bytes_exact: summary.override_path_bytes_exact,
        projected_default_total_piecewise_payload_exact: summary
            .projected_default_total_piecewise_payload_exact,
        target_window_count: summary.target_window_count,
        untouched_window_count: summary.closure_untouched_window_count,
        override_density_ppm: summary.closure_override_density_ppm,
        untouched_window_pct_ppm: summary.closure_untouched_window_pct_ppm,
    }
}

pub(crate) fn select_body_candidates(args: &BuildArgs) -> Result<Vec<usize>> {
    if let Some(chunk) = args.default_local_chunk_bytes {
        return Ok(vec![chunk]);
    }

    if args.tune_default_body {
        let source = args
            .default_body_chunk_sweep
            .as_deref()
            .unwrap_or(&args.local_chunk_sweep);

        let mut out = parse_csv_usize_list(source)
            .with_context(|| format!("parse body candidate sweep {}", source))?;
        out.sort_unstable();
        out.dedup();

        if out.is_empty() {
            bail!("default body candidate sweep resolved to empty set");
        }

        return Ok(out);
    }

    parse_csv_usize_list(&args.local_chunk_sweep)
        .with_context(|| format!("parse local chunk sweep {}", args.local_chunk_sweep))
        .map(|mut v| {
            v.sort_unstable();
            v.dedup();
            v.truncate(1);
            v
        })
}

fn load_manifest_positions(
    cli_exe: &Path,
    args: &BuildArgs,
) -> Result<BTreeMap<(String, usize), ManifestWindowPos>> {
    let mut manifest_positions = BTreeMap::<(String, usize), ManifestWindowPos>::new();

    for input in &args.inputs {
        eprintln!("apex-law-program build: manifest positions for {}", input);
        let manifest_out = run_manifest_txt(cli_exe, args, input)?;
        let positions = parse_manifest_positions(&manifest_out.stdout)?;
        for (window_idx, pos) in positions {
            manifest_positions.insert((input.clone(), window_idx), pos);
        }
    }

    Ok(manifest_positions)
}

fn select_best_materialized_index(
    materials: &[BuildMaterialized],
    objective: BodySelectObjective,
) -> Result<usize> {
    if materials.is_empty() {
        bail!("cannot select best body from empty materialized set");
    }

    materials
        .iter()
        .enumerate()
        .min_by(|(_, a), (_, b)| compare_materialized(a, b, objective))
        .map(|(idx, _)| idx)
        .ok_or_else(|| anyhow!("failed to select best body"))
}

fn compare_materialized(
    a: &BuildMaterialized,
    b: &BuildMaterialized,
    objective: BodySelectObjective,
) -> std::cmp::Ordering {
    let asu = &a.artifact.summary;
    let bsu = &b.artifact.summary;
    let ash = body_score_from_artifact(&a.artifact);
    let bsh = body_score_from_artifact(&b.artifact);

    match objective {
        BodySelectObjective::ClosureTotal => (
            ash.closure_total_exact,
            asu.selected_total_piecewise_payload_exact,
            ash.selected_override_window_count,
            ash.override_run_count,
            ash.override_path_bytes_exact,
            std::cmp::Reverse(ash.max_override_run_length),
            asu.default_local_chunk_bytes,
        )
            .cmp(&(
                bsh.closure_total_exact,
                bsu.selected_total_piecewise_payload_exact,
                bsh.selected_override_window_count,
                bsh.override_run_count,
                bsh.override_path_bytes_exact,
                std::cmp::Reverse(bsh.max_override_run_length),
                bsu.default_local_chunk_bytes,
            )),
        BodySelectObjective::SelectedTarget => (
            asu.selected_target_window_payload_exact,
            ash.closure_total_exact,
            asu.projected_default_total_piecewise_payload_exact,
            asu.selected_total_piecewise_payload_exact,
            ash.selected_override_window_count,
            asu.default_local_chunk_bytes,
        )
            .cmp(&(
                bsu.selected_target_window_payload_exact,
                bsh.closure_total_exact,
                bsu.projected_default_total_piecewise_payload_exact,
                bsu.selected_total_piecewise_payload_exact,
                bsh.selected_override_window_count,
                bsu.default_local_chunk_bytes,
            )),
        BodySelectObjective::SelectedTotal => (
            asu.selected_total_piecewise_payload_exact,
            asu.projected_default_total_piecewise_payload_exact,
            ash.closure_total_exact,
            asu.selected_target_window_payload_exact,
            ash.selected_override_window_count,
            ash.override_run_count,
            asu.default_local_chunk_bytes,
        )
            .cmp(&(
                bsu.selected_total_piecewise_payload_exact,
                bsu.projected_default_total_piecewise_payload_exact,
                bsh.closure_total_exact,
                bsu.selected_target_window_payload_exact,
                bsh.selected_override_window_count,
                bsh.override_run_count,
                bsu.default_local_chunk_bytes,
            )),
    }
}

fn build_artifact_from_outputs(
    args: &BuildArgs,
    objective: BodySelectObjective,
    txt_summary: &BTreeMap<String, String>,
    csv_sections: &super::types::ParsedCsvSections,
    manifest_positions: &BTreeMap<(String, usize), ManifestWindowPos>,
) -> Result<LawProgramArtifact> {
    if csv_sections.summary_rows.len() != 1 {
        bail!(
            "expected exactly one summary row in local mix csv, got {}",
            csv_sections.summary_rows.len()
        );
    }

    let input_order = args
        .inputs
        .iter()
        .enumerate()
        .map(|(idx, path)| (path.clone(), idx))
        .collect::<BTreeMap<_, _>>();

    let mut files = csv_sections
        .file_rows
        .iter()
        .map(|row| {
            Ok(ProgramFile {
                input: parse_required_string(row, "input")?,
                searched_total_piecewise_payload_exact: parse_required_i64(
                    row,
                    "searched_total_piecewise_payload_exact",
                )?,
                projected_default_total_piecewise_payload_exact: parse_required_i64(
                    row,
                    "projected_default_total_piecewise_payload_exact",
                )?,
                projected_unpriced_best_mix_total_piecewise_payload_exact: parse_required_i64(
                    row,
                    "projected_unpriced_best_mix_total_piecewise_payload_exact",
                )?,
                selected_total_piecewise_payload_exact: parse_required_i64(
                    row,
                    "selected_total_piecewise_payload_exact",
                )?,
                target_window_count: parse_required_usize(row, "target_window_count")?,
                override_path_mode: "none".to_string(),
                override_path_bytes_exact: parse_required_usize(row, "override_path_bytes_exact")?,
                selected_override_window_count: parse_required_usize(
                    row,
                    "selected_override_window_count",
                )?,
                closure_override_count: 0,
                closure_override_run_count: 0,
                closure_max_override_run_length: 0,
                closure_untouched_window_count: 0,
                closure_override_density_ppm: 0,
                closure_untouched_window_pct_ppm: 0,
                closure_mode_penalty_exact: 0,
                closure_penalty_exact: 0,
                closure_total_exact: 0,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let default_local_chunk_bytes = parse_required_usize(txt_summary, "default_local_chunk_bytes")?;

    let mut windows = csv_sections
        .window_rows
        .iter()
        .map(|row| {
            let input = parse_required_string(row, "input")?;
            let input_index = *input_order
                .get(&input)
                .ok_or_else(|| anyhow!("window row references unknown input {}", input))?;
            let window_idx = parse_required_usize(row, "window_idx")?;
            let pos = *manifest_positions.get(&(input.clone(), window_idx)).ok_or_else(|| {
                anyhow!(
                    "missing manifest position for input={} window_idx={}",
                    input,
                    window_idx
                )
            })?;

            Ok(ProgramWindow {
                input_index,
                input,
                window_idx,
                target_ordinal: parse_required_usize(row, "target_ordinal")?,
                start: pos.start,
                end: pos.end,
                span_bytes: pos.span_bytes,
                searched_payload_exact: parse_required_usize(row, "searched_payload_exact")?,
                default_payload_exact: parse_required_usize(row, "default_payload_exact")?,
                best_payload_exact: parse_required_usize(row, "best_payload_exact")?,
                selected_payload_exact: parse_required_usize(row, "selected_payload_exact")?,
                searched_chunk_bytes: parse_required_usize(row, "searched_chunk_bytes")?,
                best_chunk_bytes: parse_required_usize(row, "best_chunk_bytes")?,
                selected_chunk_bytes: parse_required_usize(row, "selected_chunk_bytes")?,
                selected_gain_exact: parse_required_i64(row, "selected_gain_exact")?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    files.sort_by_key(|row| input_order.get(&row.input).copied().unwrap_or(usize::MAX));
    windows.sort_by_key(|row| (row.input_index, row.target_ordinal, row.window_idx));

    let mut windows_by_input = BTreeMap::<String, Vec<usize>>::new();
    for (idx, row) in windows.iter().enumerate() {
        windows_by_input.entry(row.input.clone()).or_default().push(idx);
    }

    let searched_total_piecewise_payload_exact = files
        .iter()
        .map(|row| row.searched_total_piecewise_payload_exact)
        .sum::<i64>();

    let mut projected_default_total_piecewise_payload_exact = 0i64;
    let mut projected_unpriced_best_mix_total_piecewise_payload_exact = 0i64;
    let mut selected_total_piecewise_payload_exact = 0i64;
    let mut searched_target_window_payload_exact = 0usize;
    let mut default_target_window_payload_exact = 0usize;
    let mut best_mix_target_window_payload_exact = 0usize;
    let mut selected_target_window_payload_exact = 0usize;
    let mut override_path_bytes_exact = 0usize;
    let mut selected_override_window_count = 0usize;
    let mut improved_target_window_count = 0usize;
    let mut equal_target_window_count = 0usize;
    let mut worsened_target_window_count = 0usize;
    let mut closure_override_count = 0usize;
    let mut closure_override_run_count = 0usize;
    let mut closure_max_override_run_length = 0usize;
    let mut closure_untouched_window_count = 0usize;
    let mut closure_mode_penalty_exact = 0usize;
    let mut closure_penalty_exact = 0usize;
    let mut overrides = Vec::<ProgramOverride>::new();

    for file in &mut files {
        let window_indexes = windows_by_input.get(&file.input).cloned().unwrap_or_default();

        let searched_target = window_indexes
            .iter()
            .map(|idx| windows[*idx].searched_payload_exact)
            .sum::<usize>();
        let default_target = window_indexes
            .iter()
            .map(|idx| windows[*idx].default_payload_exact)
            .sum::<usize>();
        let best_target = window_indexes
            .iter()
            .map(|idx| windows[*idx].best_payload_exact)
            .sum::<usize>();

        let base_default_total = file.searched_total_piecewise_payload_exact
            + default_target as i64
            - searched_target as i64;
        let base_best_mix_total = file.searched_total_piecewise_payload_exact
            + best_target as i64
            - searched_target as i64;

        let candidates = window_indexes
            .iter()
            .filter_map(|idx| {
                let window = &windows[*idx];
                let gain_exact = window
                    .default_payload_exact
                    .saturating_sub(window.best_payload_exact);
                if gain_exact >= args.min_override_gain_exact {
                    Some(OverrideCandidateRef {
                        window_idx: *idx,
                        target_ordinal: window.target_ordinal,
                        gain_exact,
                    })
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let selected_plan = select_override_plan(
            &candidates,
            args.exact_subset_limit,
            objective,
            window_indexes.len(),
        );

        let selected_ordinals = selected_plan
            .selected_window_ordinals
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();

        let mut file_selected_target = 0usize;
        for idx in &window_indexes {
            let window = &mut windows[*idx];
            let gain_exact = window
                .default_payload_exact
                .saturating_sub(window.best_payload_exact);

            if selected_ordinals.contains(&window.target_ordinal) && gain_exact > 0 {
                window.selected_payload_exact = window.best_payload_exact;
                window.selected_chunk_bytes = window.best_chunk_bytes;
                window.selected_gain_exact = gain_exact as i64;
                overrides.push(ProgramOverride {
                    input_index: window.input_index,
                    input: window.input.clone(),
                    window_idx: window.window_idx,
                    target_ordinal: window.target_ordinal,
                    best_chunk_bytes: window.best_chunk_bytes,
                    default_payload_exact: window.default_payload_exact,
                    best_payload_exact: window.best_payload_exact,
                    gain_exact,
                });
            } else {
                window.selected_payload_exact = window.default_payload_exact;
                window.selected_chunk_bytes = default_local_chunk_bytes;
                window.selected_gain_exact = 0;
            }

            file_selected_target += window.selected_payload_exact;
            match window.selected_payload_exact.cmp(&window.searched_payload_exact) {
                std::cmp::Ordering::Less => improved_target_window_count += 1,
                std::cmp::Ordering::Equal => equal_target_window_count += 1,
                std::cmp::Ordering::Greater => worsened_target_window_count += 1,
            }
        }

        let file_selected_total = file.searched_total_piecewise_payload_exact
            + file_selected_target as i64
            - searched_target as i64
            + selected_plan.path_bytes_exact as i64;

        let shape = selected_plan.closure_shape;
        let file_closure_total_exact =
            file_selected_total.saturating_add(shape.closure_penalty_exact as i64);

        file.projected_default_total_piecewise_payload_exact = base_default_total;
        file.projected_unpriced_best_mix_total_piecewise_payload_exact = base_best_mix_total;
        file.selected_total_piecewise_payload_exact = file_selected_total;
        file.target_window_count = window_indexes.len();
        file.override_path_mode = selected_plan.mode.as_str().to_string();
        file.override_path_bytes_exact = selected_plan.path_bytes_exact;
        file.selected_override_window_count = selected_plan.selected_window_ordinals.len();
        file.closure_override_count = shape.override_count;
        file.closure_override_run_count = shape.override_run_count;
        file.closure_max_override_run_length = shape.max_override_run_length;
        file.closure_untouched_window_count = shape.untouched_window_count;
        file.closure_override_density_ppm = shape.override_density_ppm;
        file.closure_untouched_window_pct_ppm = shape.untouched_window_pct_ppm;
        file.closure_mode_penalty_exact = shape.mode_penalty_exact;
        file.closure_penalty_exact = shape.closure_penalty_exact;
        file.closure_total_exact = file_closure_total_exact;

        projected_default_total_piecewise_payload_exact += base_default_total;
        projected_unpriced_best_mix_total_piecewise_payload_exact += base_best_mix_total;
        selected_total_piecewise_payload_exact += file_selected_total;
        searched_target_window_payload_exact += searched_target;
        default_target_window_payload_exact += default_target;
        best_mix_target_window_payload_exact += best_target;
        selected_target_window_payload_exact += file_selected_target;
        override_path_bytes_exact += selected_plan.path_bytes_exact;
        selected_override_window_count += selected_plan.selected_window_ordinals.len();
        closure_override_count += shape.override_count;
        closure_override_run_count += shape.override_run_count;
        closure_max_override_run_length =
            closure_max_override_run_length.max(shape.max_override_run_length);
        closure_untouched_window_count += shape.untouched_window_count;
        closure_mode_penalty_exact += shape.mode_penalty_exact;
        closure_penalty_exact += shape.closure_penalty_exact;
    }

    overrides.sort_by_key(|row| (row.input_index, row.target_ordinal, row.window_idx));

    let closure_override_density_ppm = if windows.is_empty() {
        0
    } else {
        scaled_ppm(closure_override_count, windows.len())
    };
    let closure_untouched_window_pct_ppm = if windows.is_empty() {
        1_000_000
    } else {
        scaled_ppm(closure_untouched_window_count, windows.len())
    };
    let closure_total_exact =
        selected_total_piecewise_payload_exact.saturating_add(closure_penalty_exact as i64);

    let summary = ProgramSummary {
        recipe: parse_required_string(txt_summary, "recipe")?,
        file_count: parse_required_usize(txt_summary, "file_count")?,
        honest_file_count: parse_required_usize(txt_summary, "honest_file_count")?,
        union_law_count: parse_required_usize(txt_summary, "union_law_count")?,
        target_global_law_id: parse_required_string(txt_summary, "target_global_law_id")?,
        target_global_law_path_hits: parse_required_usize(txt_summary, "target_global_law_path_hits")?,
        target_global_law_file_count: parse_required_usize(txt_summary, "target_global_law_file_count")?,
        target_global_law_total_window_count: parse_required_usize(
            txt_summary,
            "target_global_law_total_window_count",
        )?,
        target_global_law_total_segment_count: parse_required_usize(
            txt_summary,
            "target_global_law_total_segment_count",
        )?,
        target_global_law_total_covered_bytes: parse_required_usize(
            txt_summary,
            "target_global_law_total_covered_bytes",
        )?,
        target_global_law_dominant_knob_signature: parse_required_string(
            txt_summary,
            "target_global_law_dominant_knob_signature",
        )?,
        eval_boundary_band: parse_required_usize(txt_summary, "eval_boundary_band")?,
        eval_field_margin: parse_required_u64(txt_summary, "eval_field_margin")?,
        eval_newline_demote_margin: parse_required_u64(txt_summary, "eval_newline_demote_margin")?,
        eval_chunk_search_objective: parse_required_string(txt_summary, "eval_chunk_search_objective")?,
        eval_chunk_raw_slack: parse_required_u64(txt_summary, "eval_chunk_raw_slack")?,
        eval_chunk_candidates: parse_required_string(txt_summary, "eval_chunk_candidates")?,
        eval_chunk_candidate_count: parse_required_usize(txt_summary, "eval_chunk_candidate_count")?,
        default_local_chunk_bytes,
        default_local_chunk_window_wins: parse_required_usize(
            txt_summary,
            "default_local_chunk_window_wins",
        )?,
        searched_total_piecewise_payload_exact,
        projected_default_total_piecewise_payload_exact,
        delta_default_total_piecewise_payload_exact: projected_default_total_piecewise_payload_exact
            - searched_total_piecewise_payload_exact,
        projected_unpriced_best_mix_total_piecewise_payload_exact,
        delta_unpriced_best_mix_total_piecewise_payload_exact:
            projected_unpriced_best_mix_total_piecewise_payload_exact
                - searched_total_piecewise_payload_exact,
        selected_total_piecewise_payload_exact,
        delta_selected_total_piecewise_payload_exact: selected_total_piecewise_payload_exact
            - searched_total_piecewise_payload_exact,
        target_window_count: windows.len(),
        searched_target_window_payload_exact,
        default_target_window_payload_exact,
        best_mix_target_window_payload_exact,
        selected_target_window_payload_exact,
        delta_selected_target_window_payload_exact: selected_target_window_payload_exact as i64
            - searched_target_window_payload_exact as i64,
        override_path_mode: aggregate_override_path_mode(&files),
        override_path_bytes_exact,
        selected_override_window_count,
        improved_target_window_count,
        equal_target_window_count,
        worsened_target_window_count,
        closure_override_count,
        closure_override_run_count,
        closure_max_override_run_length,
        closure_untouched_window_count,
        closure_override_density_ppm,
        closure_untouched_window_pct_ppm,
        closure_mode_penalty_exact,
        closure_penalty_exact,
        closure_total_exact,
    };

    Ok(LawProgramArtifact {
        config: ReplayConfig {
            recipe: args.recipe.clone(),
            inputs: args.inputs.clone(),
            max_ticks: args.max_ticks,
            window_bytes: args.window_bytes,
            step_bytes: args.step_bytes,
            max_windows: args.max_windows,
            seed_from: args.seed_from,
            seed_count: args.seed_count,
            seed_step: args.seed_step,
            recipe_seed: args.recipe_seed,
            chunk_sweep: args.chunk_sweep.clone(),
            chunk_search_objective: args.chunk_search_objective.clone(),
            chunk_raw_slack: args.chunk_raw_slack,
            map_max_depth: args.map_max_depth,
            map_depth_shift: args.map_depth_shift,
            boundary_band_sweep: args.boundary_band_sweep.clone(),
            boundary_delta: args.boundary_delta,
            field_margin_sweep: args.field_margin_sweep.clone(),
            newline_margin_add: args.newline_margin_add,
            space_to_newline_margin_add: args.space_to_newline_margin_add,
            newline_share_ppm_min: args.newline_share_ppm_min,
            newline_override_budget: args.newline_override_budget,
            newline_demote_margin_sweep: args.newline_demote_margin_sweep.clone(),
            newline_demote_keep_ppm_min: args.newline_demote_keep_ppm_min,
            newline_demote_keep_min: args.newline_demote_keep_min,
            newline_only_from_spacelike: args.newline_only_from_spacelike,
            merge_gap_bytes: args.merge_gap_bytes,
            allow_overlap_scout: args.allow_overlap_scout,
            freeze_boundary_band: args.freeze_boundary_band,
            freeze_field_margin: args.freeze_field_margin,
            freeze_newline_demote_margin: args.freeze_newline_demote_margin,
            local_chunk_sweep: args.local_chunk_sweep.clone(),
            local_chunk_search_objective: args.local_chunk_search_objective.clone(),
            local_chunk_raw_slack: args.local_chunk_raw_slack,
            default_local_chunk_bytes_arg: args.default_local_chunk_bytes,
            tune_default_body: args.tune_default_body,
            default_body_chunk_sweep: args.default_body_chunk_sweep.clone(),
            body_select_objective: args.body_select_objective.clone(),
            emit_body_scoreboard: args.emit_body_scoreboard,
            min_override_gain_exact: args.min_override_gain_exact,
            exact_subset_limit: args.exact_subset_limit,
            global_law_id_arg: args.global_law_id.clone(),
        },
        summary,
        files,
        windows,
        overrides,
    })
}

fn scaled_ppm(num: usize, den: usize) -> u32 {
    if den == 0 {
        return 0;
    }
    (((num as u128) * 1_000_000u128) / den as u128) as u32
}

fn aggregate_override_path_mode(files: &[ProgramFile]) -> String {
    let mut modes = files
        .iter()
        .map(|row| row.override_path_mode.as_str())
        .filter(|mode| *mode != "none")
        .collect::<Vec<_>>();

    modes.sort_unstable();
    modes.dedup();

    match modes.len() {
        0 => "none".to_string(),
        1 => modes[0].to_string(),
        _ => format!("mixed({})", modes.join(",")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::plan::compute_closure_shape_metrics;
    use super::super::types::OverridePathMode;

    fn override_path_mode_from_str(raw: &str) -> OverridePathMode {
        match raw {
            "none" => OverridePathMode::None,
            "delta" => OverridePathMode::Delta,
            "runs" => OverridePathMode::Runs,
            "ordinals" => OverridePathMode::Ordinals,
            _ => OverridePathMode::Ordinals,
        }
    }

    fn make_test_materialized(
        chunk: usize,
        selected_total: i64,
        default_total: i64,
        selected_target: usize,
        override_path: usize,
        override_mode: &str,
        target_window_count: usize,
        override_ordinals: &[usize],
    ) -> BuildMaterialized {
        let input = "text/Genesis1.txt".to_string();
        let shape = compute_closure_shape_metrics(
            override_path_mode_from_str(override_mode),
            override_path,
            override_ordinals,
            target_window_count,
        );

        BuildMaterialized {
            body_scores: Vec::new(),
            artifact: LawProgramArtifact {
                config: ReplayConfig {
                    recipe: String::new(),
                    inputs: vec![input.clone()],
                    max_ticks: 0,
                    window_bytes: 0,
                    step_bytes: 0,
                    max_windows: 0,
                    seed_from: 0,
                    seed_count: 0,
                    seed_step: 0,
                    recipe_seed: 0,
                    chunk_sweep: String::new(),
                    chunk_search_objective: String::new(),
                    chunk_raw_slack: 0,
                    map_max_depth: 0,
                    map_depth_shift: 0,
                    boundary_band_sweep: String::new(),
                    boundary_delta: 0,
                    field_margin_sweep: String::new(),
                    newline_margin_add: 0,
                    space_to_newline_margin_add: 0,
                    newline_share_ppm_min: 0,
                    newline_override_budget: 0,
                    newline_demote_margin_sweep: String::new(),
                    newline_demote_keep_ppm_min: 0,
                    newline_demote_keep_min: 0,
                    newline_only_from_spacelike: false,
                    merge_gap_bytes: 0,
                    allow_overlap_scout: false,
                    freeze_boundary_band: None,
                    freeze_field_margin: None,
                    freeze_newline_demote_margin: None,
                    local_chunk_sweep: String::new(),
                    local_chunk_search_objective: None,
                    local_chunk_raw_slack: None,
                    default_local_chunk_bytes_arg: Some(chunk),
                    tune_default_body: true,
                    default_body_chunk_sweep: None,
                    body_select_objective: "selected-total".to_string(),
                    emit_body_scoreboard: false,
                    min_override_gain_exact: 0,
                    exact_subset_limit: 0,
                    global_law_id_arg: None,
                },
                summary: ProgramSummary {
                    recipe: String::new(),
                    file_count: 1,
                    honest_file_count: 1,
                    union_law_count: 1,
                    target_global_law_id: String::new(),
                    target_global_law_path_hits: 0,
                    target_global_law_file_count: 1,
                    target_global_law_total_window_count: target_window_count,
                    target_global_law_total_segment_count: 0,
                    target_global_law_total_covered_bytes: 0,
                    target_global_law_dominant_knob_signature: String::new(),
                    eval_boundary_band: 0,
                    eval_field_margin: 0,
                    eval_newline_demote_margin: 0,
                    eval_chunk_search_objective: String::new(),
                    eval_chunk_raw_slack: 0,
                    eval_chunk_candidates: String::new(),
                    eval_chunk_candidate_count: 0,
                    default_local_chunk_bytes: chunk,
                    default_local_chunk_window_wins: 0,
                    searched_total_piecewise_payload_exact: 0,
                    projected_default_total_piecewise_payload_exact: default_total,
                    delta_default_total_piecewise_payload_exact: 0,
                    projected_unpriced_best_mix_total_piecewise_payload_exact: 0,
                    delta_unpriced_best_mix_total_piecewise_payload_exact: 0,
                    selected_total_piecewise_payload_exact: selected_total,
                    delta_selected_total_piecewise_payload_exact: 0,
                    target_window_count,
                    searched_target_window_payload_exact: 0,
                    default_target_window_payload_exact: 0,
                    best_mix_target_window_payload_exact: 0,
                    selected_target_window_payload_exact: selected_target,
                    delta_selected_target_window_payload_exact: 0,
                    override_path_mode: override_mode.to_string(),
                    override_path_bytes_exact: override_path,
                    selected_override_window_count: override_ordinals.len(),
                    improved_target_window_count: 0,
                    equal_target_window_count: 0,
                    worsened_target_window_count: 0,
                    closure_override_count: shape.override_count,
                    closure_override_run_count: shape.override_run_count,
                    closure_max_override_run_length: shape.max_override_run_length,
                    closure_untouched_window_count: shape.untouched_window_count,
                    closure_override_density_ppm: shape.override_density_ppm,
                    closure_untouched_window_pct_ppm: shape.untouched_window_pct_ppm,
                    closure_mode_penalty_exact: shape.mode_penalty_exact,
                    closure_penalty_exact: shape.closure_penalty_exact,
                    closure_total_exact: selected_total + shape.closure_penalty_exact as i64,
                },
                files: vec![ProgramFile {
                    input: input.clone(),
                    searched_total_piecewise_payload_exact: 0,
                    projected_default_total_piecewise_payload_exact: default_total,
                    projected_unpriced_best_mix_total_piecewise_payload_exact: 0,
                    selected_total_piecewise_payload_exact: selected_total,
                    target_window_count,
                    override_path_mode: override_mode.to_string(),
                    override_path_bytes_exact: override_path,
                    selected_override_window_count: override_ordinals.len(),
                    closure_override_count: shape.override_count,
                    closure_override_run_count: shape.override_run_count,
                    closure_max_override_run_length: shape.max_override_run_length,
                    closure_untouched_window_count: shape.untouched_window_count,
                    closure_override_density_ppm: shape.override_density_ppm,
                    closure_untouched_window_pct_ppm: shape.untouched_window_pct_ppm,
                    closure_mode_penalty_exact: shape.mode_penalty_exact,
                    closure_penalty_exact: shape.closure_penalty_exact,
                    closure_total_exact: selected_total + shape.closure_penalty_exact as i64,
                }],
                windows: Vec::new(),
                overrides: override_ordinals
                    .iter()
                    .copied()
                    .enumerate()
                    .map(|(idx, target_ordinal)| ProgramOverride {
                        input_index: 0,
                        input: input.clone(),
                        window_idx: idx,
                        target_ordinal,
                        best_chunk_bytes: chunk,
                        default_payload_exact: 0,
                        best_payload_exact: 0,
                        gain_exact: 1,
                    })
                    .collect(),
            },
        }
    }

    #[test]
    fn selected_total_breaks_ties_toward_better_default_body() {
        let items = vec![
            make_test_materialized(
                32,
                2123,
                2400,
                507,
                3,
                "runs",
                12,
                &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            ),
            make_test_materialized(
                64,
                2123,
                2200,
                507,
                3,
                "runs",
                12,
                &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            ),
            make_test_materialized(96, 2124, 2128, 508, 4, "delta", 12, &[2, 9]),
        ];

        let idx =
            select_best_materialized_index(&items, BodySelectObjective::SelectedTotal).unwrap();
        assert_eq!(idx, 1);
    }

    #[test]
    fn closure_total_prefers_sparse_body_over_dense_body() {
        let items = vec![
            make_test_materialized(
                64,
                2123,
                2200,
                507,
                3,
                "runs",
                12,
                &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            ),
            make_test_materialized(96, 2124, 2128, 508, 4, "delta", 12, &[2, 9]),
        ];

        let idx =
            select_best_materialized_index(&items, BodySelectObjective::ClosureTotal).unwrap();
        assert_eq!(idx, 1);
    }

    #[test]
    fn selected_target_breaks_ties_toward_better_default_body() {
        let items = vec![
            make_test_materialized(
                32,
                2123,
                2400,
                507,
                3,
                "runs",
                12,
                &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            ),
            make_test_materialized(
                64,
                2123,
                2200,
                507,
                3,
                "runs",
                12,
                &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            ),
        ];

        let idx =
            select_best_materialized_index(&items, BodySelectObjective::SelectedTarget).unwrap();
        assert_eq!(idx, 1);
    }

    #[test]
    fn select_body_candidates_uses_tuned_sweep() {
        let args = BuildArgs {
            recipe: "r".to_string(),
            inputs: vec!["text/Genesis1.txt".to_string()],
            max_ticks: 1,
            window_bytes: 256,
            step_bytes: 256,
            max_windows: 1,
            seed_from: 0,
            seed_count: 1,
            seed_step: 1,
            recipe_seed: 1,
            chunk_sweep: "32,64".to_string(),
            chunk_search_objective: "raw".to_string(),
            chunk_raw_slack: 1,
            map_max_depth: 0,
            map_depth_shift: 1,
            boundary_band_sweep: "8,12".to_string(),
            boundary_delta: 1,
            field_margin_sweep: "4,8".to_string(),
            newline_margin_add: 0,
            space_to_newline_margin_add: 0,
            newline_share_ppm_min: 0,
            newline_override_budget: 0,
            newline_demote_margin_sweep: "0,4".to_string(),
            newline_demote_keep_ppm_min: 0,
            newline_demote_keep_min: 0,
            newline_only_from_spacelike: true,
            merge_gap_bytes: 0,
            allow_overlap_scout: false,
            freeze_boundary_band: None,
            freeze_field_margin: None,
            freeze_newline_demote_margin: None,
            local_chunk_sweep: "32,64,96,128".to_string(),
            local_chunk_search_objective: None,
            local_chunk_raw_slack: None,
            default_local_chunk_bytes: None,
            tune_default_body: true,
            default_body_chunk_sweep: Some("128,64,64,96".to_string()),
            body_select_objective: "selected-total".to_string(),
            emit_body_scoreboard: false,
            min_override_gain_exact: 1,
            exact_subset_limit: 20,
            global_law_id: None,
            top_rows: 10,
            out: "/tmp/out.aklp".to_string(),
            out_report: None,
        };

        assert_eq!(select_body_candidates(&args).unwrap(), vec![64, 96, 128]);
    }
}