use anyhow::{anyhow, bail, Context, Result};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use tempfile::tempdir;

use super::parse::parse_best_line;
use super::types::{FrozenEvalRow, LawProgramArtifact, ReplayEvalRow, ReplayFileSummary};

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