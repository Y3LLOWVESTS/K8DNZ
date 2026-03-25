use anyhow::{anyhow, bail, Context, Result};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use tempfile::tempdir;

use super::parse::parse_best_line;
use super::types::{
    FrozenEvalRow, LawProgramArtifact, ProgramBridgeSegment, ProgramOverride, ReplayEvalRow,
    ReplayFileSummary,
};

pub(crate) fn replay_artifact(
    cli_exe: &Path,
    artifact: &LawProgramArtifact,
) -> Result<(Vec<ReplayEvalRow>, Vec<ReplayFileSummary>)> {
    let total_windows = artifact.windows.len();
    eprintln!(
        "apex-law-program replay: start windows={} inputs={}",
        total_windows,
        artifact.config.inputs.len()
    );

    let temp_dir = tempdir().context("create replay temp dir")?;
    let input_data = artifact
        .config
        .inputs
        .iter()
        .map(|path| fs::read(path).with_context(|| format!("read replay input {}", path)))
        .collect::<Result<Vec<_>>>()?;

    let jobs = resolve_replay_jobs(total_windows);
    eprintln!(
        "apex-law-program replay: jobs={} temp_dir={}",
        jobs,
        temp_dir.path().display()
    );

    let rows = if jobs <= 1 || total_windows <= 1 {
        replay_rows_serial(cli_exe, artifact, temp_dir.path(), &input_data)?
    } else {
        replay_rows_parallel(cli_exe, artifact, temp_dir.path(), input_data, jobs)?
    };

    let file_summaries = build_file_summaries(artifact, &rows)?;
    eprintln!(
        "apex-law-program replay: done windows={} files={}",
        rows.len(),
        file_summaries.len()
    );

    Ok((rows, file_summaries))
}


pub(crate) fn canonicalize_artifact_selected_payloads(
    cli_exe: &Path,
    artifact: &mut LawProgramArtifact,
) -> Result<()> {
    let (rows, file_summaries) = replay_artifact(cli_exe, artifact)?;
    apply_replay_rows_to_artifact(artifact, &rows, &file_summaries)
}

pub(crate) fn apply_replay_rows_to_artifact(
    artifact: &mut LawProgramArtifact,
    rows: &[ReplayEvalRow],
    file_summaries: &[ReplayFileSummary],
) -> Result<()> {
    let row_map = rows
        .iter()
        .map(|row| ((row.input_index, row.target_ordinal, row.window_idx), row))
        .collect::<BTreeMap<_, _>>();

    for window in &mut artifact.windows {
        let row = row_map
            .get(&(window.input_index, window.target_ordinal, window.window_idx))
            .ok_or_else(|| {
                anyhow!(
                    "missing replay row for input={} input_index={} window_idx={} target_ordinal={}",
                    window.input,
                    window.input_index,
                    window.window_idx,
                    window.target_ordinal
                )
            })?;
        window.selected_payload_exact = row.replay_payload_exact;
        window.selected_gain_exact =
            window.default_payload_exact as i64 - row.replay_payload_exact as i64;
    }

    let mut file_summary_by_input = BTreeMap::<String, &ReplayFileSummary>::new();
    for summary in file_summaries {
        file_summary_by_input.insert(summary.input.clone(), summary);
    }

    for file in &mut artifact.files {
        let summary = file_summary_by_input
            .get(&file.input)
            .copied()
            .ok_or_else(|| anyhow!("missing replay file summary for {}", file.input))?;
        file.selected_total_piecewise_payload_exact =
            summary.replay_selected_total_piecewise_payload_exact;
        file.target_window_count = summary.target_window_count;
        file.closure_total_exact = summary
            .replay_selected_total_piecewise_payload_exact
            .saturating_add(file.closure_penalty_exact as i64);
    }

    let selected_target_window_payload_exact =
        rows.iter().map(|row| row.replay_payload_exact).sum::<usize>();
    let replay_selected_total_piecewise_payload_exact = file_summaries
        .iter()
        .map(|row| row.replay_selected_total_piecewise_payload_exact)
        .sum::<i64>();

    let mut improved_target_window_count = 0usize;
    let mut equal_target_window_count = 0usize;
    let mut worsened_target_window_count = 0usize;

    for row in rows {
        match row.delta_vs_searched_exact.cmp(&0) {
            std::cmp::Ordering::Less => improved_target_window_count += 1,
            std::cmp::Ordering::Equal => equal_target_window_count += 1,
            std::cmp::Ordering::Greater => worsened_target_window_count += 1,
        }
    }

    artifact.summary.selected_target_window_payload_exact = selected_target_window_payload_exact;
    artifact.summary.delta_selected_target_window_payload_exact =
        selected_target_window_payload_exact as i64
            - artifact.summary.searched_target_window_payload_exact as i64;
    artifact.summary.selected_total_piecewise_payload_exact =
        replay_selected_total_piecewise_payload_exact;
    artifact.summary.delta_selected_total_piecewise_payload_exact =
        replay_selected_total_piecewise_payload_exact
            - artifact.summary.searched_total_piecewise_payload_exact;
    artifact.summary.improved_target_window_count = improved_target_window_count;
    artifact.summary.equal_target_window_count = equal_target_window_count;
    artifact.summary.worsened_target_window_count = worsened_target_window_count;
    artifact.summary.closure_total_exact = replay_selected_total_piecewise_payload_exact
        .saturating_add(artifact.summary.closure_penalty_exact as i64);
    artifact.bridge_segments = derive_bridge_segments_from_overrides(&artifact.overrides);

    Ok(())
}

fn derive_bridge_segments_from_overrides(
    overrides: &[ProgramOverride],
) -> Vec<ProgramBridgeSegment> {
    let mut grouped = BTreeMap::<(usize, String), Vec<&ProgramOverride>>::new();
    for row in overrides {
        grouped
            .entry((row.input_index, row.input.clone()))
            .or_default()
            .push(row);
    }

    let mut out = Vec::<ProgramBridgeSegment>::new();
    for ((input_index, input), mut rows) in grouped {
        rows.sort_by_key(|row| (row.target_ordinal, row.window_idx));

        let mut segment_idx = 0usize;
        let mut cursor = 0usize;
        while cursor < rows.len() {
            let first = rows[cursor];
            let mut end = cursor;
            while end + 1 < rows.len()
                && rows[end + 1].target_ordinal == rows[end].target_ordinal + 1
            {
                end += 1;
            }

            let chunk = &rows[cursor..=end];
            let last = rows[end];
            let default_payload_exact = chunk
                .iter()
                .map(|row| row.default_payload_exact)
                .sum::<usize>();
            let best_payload_exact = chunk
                .iter()
                .map(|row| row.best_payload_exact)
                .sum::<usize>();

            out.push(ProgramBridgeSegment {
                input_index,
                input: input.clone(),
                segment_idx,
                start_window_idx: first.window_idx,
                end_window_idx: last.window_idx,
                start_target_ordinal: first.target_ordinal,
                end_target_ordinal: last.target_ordinal,
                window_count: chunk.len(),
                default_payload_exact,
                best_payload_exact,
                gain_exact: default_payload_exact.saturating_sub(best_payload_exact),
            });

            segment_idx += 1;
            cursor = end + 1;
        }
    }

    out.sort_by_key(|row| {
        (
            row.input_index,
            row.start_target_ordinal,
            row.end_target_ordinal,
            row.segment_idx,
        )
    });
    out
}

fn replay_rows_serial(
    cli_exe: &Path,
    artifact: &LawProgramArtifact,
    temp_root: &Path,
    input_data: &[Vec<u8>],
) -> Result<Vec<ReplayEvalRow>> {
    let total = artifact.windows.len();
    let mut rows = Vec::with_capacity(total);

    for (idx, window) in artifact.windows.iter().enumerate() {
        let row = replay_single_window(
            cli_exe,
            artifact,
            temp_root,
            input_data,
            window,
            idx,
        )?;
        rows.push(row);

        let done = idx + 1;
        if should_log_progress(done, total) {
            eprintln!("apex-law-program replay: completed={}/{}", done, total);
        }
    }

    rows.sort_by_key(|row| (row.input_index, row.target_ordinal, row.window_idx));
    Ok(rows)
}

fn replay_rows_parallel(
    cli_exe: &Path,
    artifact: &LawProgramArtifact,
    temp_root: &Path,
    input_data: Vec<Vec<u8>>,
    jobs: usize,
) -> Result<Vec<ReplayEvalRow>> {
    let total = artifact.windows.len();
    let artifact = Arc::new(artifact.clone());
    let input_data = Arc::new(input_data);
    let cli_exe = Arc::new(cli_exe.to_path_buf());
    let temp_root = Arc::new(temp_root.to_path_buf());
    let next_idx = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = mpsc::channel::<Result<ReplayEvalRow>>();

    let mut handles = Vec::with_capacity(jobs);
    for worker_id in 0..jobs {
        let artifact = Arc::clone(&artifact);
        let input_data = Arc::clone(&input_data);
        let cli_exe = Arc::clone(&cli_exe);
        let temp_root = Arc::clone(&temp_root);
        let next_idx = Arc::clone(&next_idx);
        let tx = tx.clone();

        handles.push(thread::spawn(move || {
            loop {
                let idx = next_idx.fetch_add(1, Ordering::Relaxed);
                if idx >= artifact.windows.len() {
                    break;
                }

                let window = artifact.windows[idx].clone();
                let result = replay_single_window(
                    cli_exe.as_path(),
                    artifact.as_ref(),
                    temp_root.as_path(),
                    input_data.as_ref(),
                    &window,
                    idx,
                )
                .with_context(|| {
                    format!(
                        "parallel replay worker={} input={} window_idx={} target_ordinal={}",
                        worker_id,
                        window.input,
                        window.window_idx,
                        window.target_ordinal
                    )
                });

                if tx.send(result).is_err() {
                    break;
                }
            }
        }));
    }
    drop(tx);

    let mut rows = Vec::with_capacity(total);
    let mut first_err = None::<anyhow::Error>;

    for done in 1..=total {
        match rx.recv() {
            Ok(Ok(row)) => {
                rows.push(row);
                if should_log_progress(done, total) {
                    eprintln!("apex-law-program replay: completed={}/{}", done, total);
                }
            }
            Ok(Err(err)) => {
                if first_err.is_none() {
                    first_err = Some(err);
                }
                if should_log_progress(done, total) {
                    eprintln!(
                        "apex-law-program replay: completed={}/{} (with error)",
                        done, total
                    );
                }
            }
            Err(err) => {
                if first_err.is_none() {
                    first_err = Some(anyhow!("replay worker channel failed: {}", err));
                }
                break;
            }
        }
    }

    for handle in handles {
        match handle.join() {
            Ok(()) => {}
            Err(_) => {
                if first_err.is_none() {
                    first_err = Some(anyhow!("replay worker thread panicked"));
                }
            }
        }
    }

    if let Some(err) = first_err {
        return Err(err);
    }

    rows.sort_by_key(|row| (row.input_index, row.target_ordinal, row.window_idx));
    Ok(rows)
}

fn replay_single_window(
    cli_exe: &Path,
    artifact: &LawProgramArtifact,
    temp_root: &Path,
    input_data: &[Vec<u8>],
    window: &super::types::ProgramWindow,
    ordinal: usize,
) -> Result<ReplayEvalRow> {
    let input_bytes = input_data.get(window.input_index).ok_or_else(|| {
        anyhow!(
            "artifact window references missing input_index={} input={}",
            window.input_index,
            window.input
        )
    })?;

    if window.end > input_bytes.len() || window.start > window.end {
        bail!(
            "artifact window out of range input={} start={} end={} len={}",
            artifact
                .config
                .inputs
                .get(window.input_index)
                .map(String::as_str)
                .unwrap_or(&window.input),
            window.start,
            window.end,
            input_bytes.len()
        );
    }

    let slice = &input_bytes[window.start..window.end];
    let slice_path = temp_root.join(format!(
        "window_{:02}_{:04}_{:08}_{:08}_{:04}.bin",
        window.input_index,
        window.window_idx,
        window.start,
        window.end,
        ordinal
    ));

    fs::write(&slice_path, slice)
        .with_context(|| format!("write replay slice {}", slice_path.display()))?;

    let eval = eval_window_fixed(cli_exe, artifact, &slice_path, window.selected_chunk_bytes)?;

    Ok(ReplayEvalRow {
        input_index: window.input_index,
        input: window.input.clone(),
        window_idx: window.window_idx,
        target_ordinal: window.target_ordinal,
        start: window.start,
        end: window.end,
        selected_chunk_bytes: window.selected_chunk_bytes,
        searched_payload_exact: window.searched_payload_exact,
        artifact_selected_payload_exact: window.selected_payload_exact,
        replay_payload_exact: eval.compact_field_total_payload_exact,
        delta_vs_artifact_exact: eval.compact_field_total_payload_exact as i64
            - window.selected_payload_exact as i64,
        delta_vs_searched_exact: eval.compact_field_total_payload_exact as i64
            - window.searched_payload_exact as i64,
        field_match_pct: eval.field_match_pct,
        collapse_90_flag: eval.field_pred_collapse_90_flag,
        newline_extinct_flag: eval.field_newline_extinct_flag,
        newline_floor_used: eval.field_newline_floor_used,
    })
}

fn build_file_summaries(
    artifact: &LawProgramArtifact,
    rows: &[ReplayEvalRow],
) -> Result<Vec<ReplayFileSummary>> {
    let mut by_file = BTreeMap::<usize, Vec<&super::types::ProgramWindow>>::new();
    for window in &artifact.windows {
        by_file.entry(window.input_index).or_default().push(window);
    }

    let mut file_summaries = Vec::new();
    for (input_index, windows) in by_file {
        let input_path = artifact
            .config
            .inputs
            .get(input_index)
            .ok_or_else(|| anyhow!("missing artifact input for input_index={}", input_index))?;

        let file = artifact
            .files
            .iter()
            .find(|row| row.input == *input_path)
            .ok_or_else(|| anyhow!("missing file summary for {}", input_path))?;

        let file_rows = rows
            .iter()
            .filter(|row| row.input_index == input_index)
            .collect::<Vec<_>>();

        let searched_target_window_payload_exact =
            windows.iter().map(|row| row.searched_payload_exact).sum::<usize>();
        let artifact_selected_target_window_payload_exact =
            windows.iter().map(|row| row.selected_payload_exact).sum::<usize>();
        let replay_target_window_payload_exact =
            file_rows.iter().map(|row| row.replay_payload_exact).sum::<usize>();

        let replay_selected_total_piecewise_payload_exact =
            file.searched_total_piecewise_payload_exact
                + replay_target_window_payload_exact as i64
                - searched_target_window_payload_exact as i64
                + file.override_path_bytes_exact as i64;

        let mut improved_vs_searched_count = 0usize;
        let mut equal_vs_searched_count = 0usize;
        let mut worsened_vs_searched_count = 0usize;

        for row in &file_rows {
            match row.delta_vs_searched_exact.cmp(&0) {
                std::cmp::Ordering::Less => improved_vs_searched_count += 1,
                std::cmp::Ordering::Equal => equal_vs_searched_count += 1,
                std::cmp::Ordering::Greater => worsened_vs_searched_count += 1,
            }
        }

        file_summaries.push(ReplayFileSummary {
            input: file.input.clone(),
            searched_total_piecewise_payload_exact: file.searched_total_piecewise_payload_exact,
            artifact_selected_total_piecewise_payload_exact: file.selected_total_piecewise_payload_exact,
            replay_selected_total_piecewise_payload_exact,
            searched_target_window_payload_exact,
            artifact_selected_target_window_payload_exact,
            replay_target_window_payload_exact,
            override_path_bytes_exact: file.override_path_bytes_exact,
            target_window_count: file.target_window_count,
            drift_exact: replay_selected_total_piecewise_payload_exact
                - file.selected_total_piecewise_payload_exact,
            improved_vs_searched_count,
            equal_vs_searched_count,
            worsened_vs_searched_count,
        });
    }

    file_summaries.sort_by(|a, b| a.input.cmp(&b.input));
    Ok(file_summaries)
}

fn resolve_replay_jobs(total_windows: usize) -> usize {
    let env_jobs = std::env::var("K8DNZ_REPLAY_JOBS")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        });

    env_jobs.clamp(1, total_windows.max(1)).min(4)
}

fn should_log_progress(done: usize, total: usize) -> bool {
    done == 1 || done == total || done % 4 == 0 || total <= 8
}

fn eval_window_fixed(
    cli_exe: &Path,
    artifact: &LawProgramArtifact,
    slice_path: &Path,
    chunk_bytes: usize,
) -> Result<FrozenEvalRow> {
    let mut cmd = Command::new(cli_exe);
    cmd.arg("apextrace")
        .arg("apex-map-lane")
        .arg("--recipe")
        .arg(&artifact.config.recipe)
        .arg("--in")
        .arg(slice_path)
        .arg("--max-ticks")
        .arg(artifact.config.max_ticks.to_string())
        .arg("--seed-from")
        .arg(artifact.config.seed_from.to_string())
        .arg("--seed-count")
        .arg(artifact.config.seed_count.to_string())
        .arg("--seed-step")
        .arg(artifact.config.seed_step.to_string())
        .arg("--recipe-seed")
        .arg(artifact.config.recipe_seed.to_string())
        .arg("--chunk-bytes")
        .arg(chunk_bytes.to_string())
        .arg("--chunk-search-objective")
        .arg(&artifact.summary.eval_chunk_search_objective)
        .arg("--chunk-raw-slack")
        .arg(artifact.summary.eval_chunk_raw_slack.to_string())
        .arg("--map-max-depth")
        .arg(artifact.config.map_max_depth.to_string())
        .arg("--map-depth-shift")
        .arg(artifact.config.map_depth_shift.to_string())
        .arg("--boundary-band")
        .arg(artifact.summary.eval_boundary_band.to_string())
        .arg("--boundary-delta")
        .arg(artifact.config.boundary_delta.to_string())
        .arg("--field-margin")
        .arg(artifact.summary.eval_field_margin.to_string())
        .arg("--newline-margin-add")
        .arg(artifact.config.newline_margin_add.to_string())
        .arg("--space-to-newline-margin-add")
        .arg(artifact.config.space_to_newline_margin_add.to_string())
        .arg("--newline-share-ppm-min")
        .arg(artifact.config.newline_share_ppm_min.to_string())
        .arg("--newline-override-budget")
        .arg(artifact.config.newline_override_budget.to_string())
        .arg("--newline-demote-margin")
        .arg(artifact.summary.eval_newline_demote_margin.to_string())
        .arg("--newline-demote-keep-ppm-min")
        .arg(artifact.config.newline_demote_keep_ppm_min.to_string())
        .arg("--newline-demote-keep-min")
        .arg(artifact.config.newline_demote_keep_min.to_string())
        .arg(format!(
            "--newline-only-from-spacelike={}",
            artifact.config.newline_only_from_spacelike
        ))
        .arg("--format")
        .arg("txt");

    let output = cmd.output().with_context(|| {
        format!(
            "run fixed apex-map-lane slice={} chunk_bytes={}",
            slice_path.display(),
            chunk_bytes
        )
    })?;

    if !output.status.success() {
        bail!(
            "fixed apex-map-lane failed status={}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    parse_best_line(&output.stderr)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_artifact() -> LawProgramArtifact {
        LawProgramArtifact {
            config: super::super::types::ReplayConfig {
                recipe: "configs/tuned_validated.k8r".to_string(),
                inputs: vec!["text/Genesis1.txt".to_string()],
                max_ticks: 20,
                window_bytes: 256,
                step_bytes: 256,
                max_windows: 2,
                seed_from: 0,
                seed_count: 64,
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
                freeze_boundary_band: Some(12),
                freeze_field_margin: Some(4),
                freeze_newline_demote_margin: Some(0),
                local_chunk_sweep: "32,64,96".to_string(),
                local_chunk_search_objective: None,
                local_chunk_raw_slack: None,
                default_local_chunk_bytes_arg: Some(96),
                tune_default_body: true,
                default_body_chunk_sweep: Some("64,96".to_string()),
                body_select_objective: "default-total".to_string(),
                emit_body_scoreboard: false,
                min_override_gain_exact: 1,
                exact_subset_limit: 8,
                global_law_id_arg: None,
            },
            summary: super::super::types::ProgramSummary {
                recipe: "configs/tuned_validated.k8r".to_string(),
                file_count: 1,
                honest_file_count: 1,
                union_law_count: 1,
                target_global_law_id: "G1".to_string(),
                target_global_law_path_hits: 1,
                target_global_law_file_count: 1,
                target_global_law_total_window_count: 2,
                target_global_law_total_segment_count: 0,
                target_global_law_total_covered_bytes: 512,
                target_global_law_dominant_knob_signature: "sig".to_string(),
                eval_boundary_band: 12,
                eval_field_margin: 4,
                eval_newline_demote_margin: 0,
                eval_chunk_search_objective: "raw".to_string(),
                eval_chunk_raw_slack: 1,
                eval_chunk_candidates: "32,64".to_string(),
                eval_chunk_candidate_count: 2,
                default_local_chunk_bytes: 96,
                default_local_chunk_window_wins: 2,
                searched_total_piecewise_payload_exact: 1000,
                projected_default_total_piecewise_payload_exact: 980,
                delta_default_total_piecewise_payload_exact: -20,
                projected_unpriced_best_mix_total_piecewise_payload_exact: 975,
                delta_unpriced_best_mix_total_piecewise_payload_exact: -25,
                selected_total_piecewise_payload_exact: 985,
                delta_selected_total_piecewise_payload_exact: -15,
                target_window_count: 2,
                searched_target_window_payload_exact: 220,
                default_target_window_payload_exact: 200,
                best_mix_target_window_payload_exact: 195,
                selected_target_window_payload_exact: 200,
                delta_selected_target_window_payload_exact: -20,
                override_path_mode: "delta".to_string(),
                override_path_bytes_exact: 5,
                selected_override_window_count: 1,
                improved_target_window_count: 2,
                equal_target_window_count: 0,
                worsened_target_window_count: 0,
                closure_override_count: 1,
                closure_override_run_count: 1,
                closure_max_override_run_length: 1,
                closure_untouched_window_count: 1,
                closure_override_density_ppm: 500_000,
                closure_untouched_window_pct_ppm: 500_000,
                closure_mode_penalty_exact: 0,
                closure_penalty_exact: 106,
                closure_total_exact: 1091,
            },
            files: vec![super::super::types::ProgramFile {
                input: "text/Genesis1.txt".to_string(),
                searched_total_piecewise_payload_exact: 1000,
                projected_default_total_piecewise_payload_exact: 980,
                projected_unpriced_best_mix_total_piecewise_payload_exact: 975,
                selected_total_piecewise_payload_exact: 985,
                target_window_count: 2,
                override_path_mode: "delta".to_string(),
                override_path_bytes_exact: 5,
                selected_override_window_count: 1,
                closure_override_count: 1,
                closure_override_run_count: 1,
                closure_max_override_run_length: 1,
                closure_untouched_window_count: 1,
                closure_override_density_ppm: 500_000,
                closure_untouched_window_pct_ppm: 500_000,
                closure_mode_penalty_exact: 0,
                closure_penalty_exact: 106,
                closure_total_exact: 1091,
            }],
            windows: vec![
                super::super::types::ProgramWindow {
                    input_index: 0,
                    input: "text/Genesis1.txt".to_string(),
                    window_idx: 0,
                    target_ordinal: 0,
                    start: 0,
                    end: 256,
                    span_bytes: 256,
                    searched_payload_exact: 100,
                    default_payload_exact: 90,
                    best_payload_exact: 88,
                    selected_payload_exact: 90,
                    searched_chunk_bytes: 64,
                    best_chunk_bytes: 96,
                    selected_chunk_bytes: 96,
                    selected_gain_exact: 0,
                },
                super::super::types::ProgramWindow {
                    input_index: 0,
                    input: "text/Genesis1.txt".to_string(),
                    window_idx: 1,
                    target_ordinal: 1,
                    start: 256,
                    end: 512,
                    span_bytes: 256,
                    searched_payload_exact: 120,
                    default_payload_exact: 110,
                    best_payload_exact: 105,
                    selected_payload_exact: 110,
                    searched_chunk_bytes: 64,
                    best_chunk_bytes: 96,
                    selected_chunk_bytes: 96,
                    selected_gain_exact: 5,
                },
            ],
            overrides: vec![super::super::types::ProgramOverride {
                input_index: 0,
                input: "text/Genesis1.txt".to_string(),
                window_idx: 1,
                target_ordinal: 1,
                best_chunk_bytes: 96,
                default_payload_exact: 110,
                best_payload_exact: 105,
                gain_exact: 5,
            }],
            bridge_segments: vec![super::super::types::ProgramBridgeSegment {
                input_index: 0,
                input: "text/Genesis1.txt".to_string(),
                segment_idx: 0,
                start_window_idx: 1,
                end_window_idx: 1,
                start_target_ordinal: 1,
                end_target_ordinal: 1,
                window_count: 1,
                default_payload_exact: 110,
                best_payload_exact: 105,
                gain_exact: 5,
            }],
        }
    }

    #[test]
    fn apply_replay_rows_to_artifact_updates_canonical_selected_totals() {
        let mut artifact = fixture_artifact();
        let rows = vec![
            ReplayEvalRow {
                input_index: 0,
                input: "text/Genesis1.txt".to_string(),
                window_idx: 0,
                target_ordinal: 0,
                start: 0,
                end: 256,
                selected_chunk_bytes: 96,
                searched_payload_exact: 100,
                artifact_selected_payload_exact: 90,
                replay_payload_exact: 89,
                delta_vs_artifact_exact: -1,
                delta_vs_searched_exact: -11,
                field_match_pct: 90.0,
                collapse_90_flag: false,
                newline_extinct_flag: false,
                newline_floor_used: 0,
            },
            ReplayEvalRow {
                input_index: 0,
                input: "text/Genesis1.txt".to_string(),
                window_idx: 1,
                target_ordinal: 1,
                start: 256,
                end: 512,
                selected_chunk_bytes: 96,
                searched_payload_exact: 120,
                artifact_selected_payload_exact: 110,
                replay_payload_exact: 115,
                delta_vs_artifact_exact: 5,
                delta_vs_searched_exact: -5,
                field_match_pct: 85.0,
                collapse_90_flag: false,
                newline_extinct_flag: false,
                newline_floor_used: 0,
            },
        ];
        let file_summaries = vec![ReplayFileSummary {
            input: "text/Genesis1.txt".to_string(),
            searched_total_piecewise_payload_exact: 1000,
            artifact_selected_total_piecewise_payload_exact: 985,
            replay_selected_total_piecewise_payload_exact: 989,
            searched_target_window_payload_exact: 220,
            artifact_selected_target_window_payload_exact: 200,
            replay_target_window_payload_exact: 204,
            override_path_bytes_exact: 5,
            target_window_count: 2,
            drift_exact: 4,
            improved_vs_searched_count: 2,
            equal_vs_searched_count: 0,
            worsened_vs_searched_count: 0,
        }];

        apply_replay_rows_to_artifact(&mut artifact, &rows, &file_summaries).unwrap();

        assert_eq!(artifact.windows[0].selected_payload_exact, 89);
        assert_eq!(artifact.windows[1].selected_payload_exact, 115);
        assert_eq!(artifact.summary.selected_target_window_payload_exact, 204);
        assert_eq!(artifact.summary.selected_total_piecewise_payload_exact, 989);
        assert_eq!(artifact.summary.delta_selected_total_piecewise_payload_exact, -11);
        assert_eq!(artifact.summary.closure_total_exact, 1095);
        assert_eq!(artifact.files[0].selected_total_piecewise_payload_exact, 989);
        assert_eq!(artifact.files[0].closure_total_exact, 1095);
        assert_eq!(artifact.bridge_segments.len(), 1);
        assert_eq!(artifact.bridge_segments[0].window_count, 1);
        assert_eq!(artifact.bridge_segments[0].gain_exact, 5);
    }
}
