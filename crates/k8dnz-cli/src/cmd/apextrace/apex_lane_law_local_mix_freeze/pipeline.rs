use anyhow::{anyhow, Context, Result};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::thread;

use crate::cmd::apextrace::ApexLaneLawLocalMixFreezeArgs;

use super::eval::eval_window;
use super::overrides::{override_path_bytes, select_override_subset};
use super::types::{
    EvalConfig, FileReport, FileSummary, FrozenEvalRow, LocalMixSummary, ManifestWindowRow,
    OverrideCandidate, WindowEvalRow,
};

#[allow(clippy::too_many_arguments)]
pub(crate) fn scan_global_best_chunk_counts(
    exe: &Path,
    args: &ApexLaneLawLocalMixFreezeArgs,
    eval_config: &EvalConfig,
    reports: &[FileReport],
    local_to_global_maps: &[BTreeMap<String, String>],
    target_global_law_id: &str,
    dominant_chunk_bytes: usize,
    chunk_candidates: &[usize],
    temp_dir: &Path,
    eval_cache: &mut BTreeMap<(String, usize, usize), FrozenEvalRow>,
    jobs: usize,
) -> Result<BTreeMap<usize, usize>> {
    let tasks = collect_target_window_tasks(reports, local_to_global_maps, target_global_law_id);
    if tasks.is_empty() {
        return Ok(BTreeMap::new());
    }

    let input_bytes_by_report = reports
        .iter()
        .map(|report| {
            fs::read(&report.input)
                .with_context(|| format!("read input for local mix eval {}", report.input))
        })
        .collect::<Result<Vec<_>>>()?;

    if jobs <= 1 || tasks.len() <= 1 {
        return scan_global_best_chunk_counts_serial(
            exe,
            args,
            eval_config,
            reports,
            &input_bytes_by_report,
            dominant_chunk_bytes,
            chunk_candidates,
            temp_dir,
            eval_cache,
            &tasks,
        );
    }

    let chunk_size = div_ceil(tasks.len(), jobs);
    let mut merged_counts = BTreeMap::<usize, usize>::new();
    let mut merged_cache_entries = Vec::<((String, usize, usize), FrozenEvalRow)>::new();

    thread::scope(|scope| -> Result<()> {
        let mut handles = Vec::new();

        for task_chunk in tasks.chunks(chunk_size) {
            let reports_ref = reports;
            let input_bytes_by_report_ref = &input_bytes_by_report;
            let exe_ref = exe;
            let args_ref = args;
            let eval_config_ref = eval_config;
            let chunk_candidates_ref = chunk_candidates;
            let temp_dir_ref = temp_dir;

            handles.push(scope.spawn(move || -> Result<_> {
                let mut local_counts = BTreeMap::<usize, usize>::new();
                let mut local_cache = BTreeMap::<(String, usize, usize), FrozenEvalRow>::new();

                for task in task_chunk {
                    let report = &reports_ref[task.report_idx];
                    let input_bytes = &input_bytes_by_report_ref[task.report_idx];
                    let mut best_payload = usize::MAX;
                    let mut best_chunk = dominant_chunk_bytes;

                    for &chunk_bytes in chunk_candidates_ref {
                        let eval = eval_window(
                            exe_ref,
                            args_ref,
                            eval_config_ref,
                            &report.input,
                            input_bytes,
                            &task.window,
                            chunk_bytes,
                            temp_dir_ref,
                            &mut local_cache,
                        )?;
                        if eval.compact_field_total_payload_exact < best_payload
                            || (eval.compact_field_total_payload_exact == best_payload
                                && chunk_bytes < best_chunk)
                        {
                            best_payload = eval.compact_field_total_payload_exact;
                            best_chunk = chunk_bytes;
                        }
                    }

                    *local_counts.entry(best_chunk).or_default() += 1;
                }

                Ok((local_counts, local_cache.into_iter().collect::<Vec<_>>()))
            }));
        }

        for handle in handles {
            let (counts, cache_entries) = handle
                .join()
                .map_err(|_| anyhow!("scan_global_best_chunk_counts worker panicked"))??;
            for (chunk_bytes, count) in counts {
                *merged_counts.entry(chunk_bytes).or_default() += count;
            }
            merged_cache_entries.extend(cache_entries);
        }

        Ok(())
    })?;

    for (key, row) in merged_cache_entries {
        eval_cache.entry(key).or_insert(row);
    }

    Ok(merged_counts)
}

fn scan_global_best_chunk_counts_serial(
    exe: &Path,
    args: &ApexLaneLawLocalMixFreezeArgs,
    eval_config: &EvalConfig,
    reports: &[FileReport],
    input_bytes_by_report: &[Vec<u8>],
    dominant_chunk_bytes: usize,
    chunk_candidates: &[usize],
    temp_dir: &Path,
    eval_cache: &mut BTreeMap<(String, usize, usize), FrozenEvalRow>,
    tasks: &[TargetWindowTask],
) -> Result<BTreeMap<usize, usize>> {
    let mut global_best_chunk_counts = BTreeMap::<usize, usize>::new();

    for task in tasks {
        let report = &reports[task.report_idx];
        let input_bytes = &input_bytes_by_report[task.report_idx];
        let mut best_payload = usize::MAX;
        let mut best_chunk = dominant_chunk_bytes;

        for &chunk_bytes in chunk_candidates {
            let eval = eval_window(
                exe,
                args,
                eval_config,
                &report.input,
                input_bytes,
                &task.window,
                chunk_bytes,
                temp_dir,
                eval_cache,
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

    Ok(global_best_chunk_counts)
}

pub(crate) struct ProcessReportOutcome {
    pub(crate) file_summary: FileSummary,
    pub(crate) target_rows: Vec<WindowEvalRow>,
    pub(crate) override_candidates: Vec<OverrideCandidate>,
    pub(crate) selected_overrides: Vec<OverrideCandidate>,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn process_report(
    exe: &Path,
    args: &ApexLaneLawLocalMixFreezeArgs,
    eval_config: &EvalConfig,
    report: &FileReport,
    local_to_global: &BTreeMap<String, String>,
    target_global_law_id: &str,
    chunk_candidates: &[usize],
    default_chunk_bytes: usize,
    temp_dir: &Path,
    eval_cache: &BTreeMap<(String, usize, usize), FrozenEvalRow>,
    jobs: usize,
) -> Result<ProcessReportOutcome> {
    let input_bytes = fs::read(&report.input)
        .with_context(|| format!("read input for local mix eval {}", report.input))?;
    let target_tasks = collect_report_target_tasks(report, local_to_global, target_global_law_id);

    let mut target_rows = if jobs <= 1 || target_tasks.len() <= 1 {
        process_report_windows_serial(
            exe,
            args,
            eval_config,
            report,
            target_global_law_id,
            &input_bytes,
            chunk_candidates,
            default_chunk_bytes,
            temp_dir,
            eval_cache,
            &target_tasks,
        )?
    } else {
        process_report_windows_parallel(
            exe,
            args,
            eval_config,
            report,
            target_global_law_id,
            &input_bytes,
            chunk_candidates,
            default_chunk_bytes,
            temp_dir,
            eval_cache,
            jobs,
            &target_tasks,
        )?
    };

    target_rows.sort_by_key(|row| row.target_ordinal);

    let override_candidates = target_rows
        .iter()
        .filter_map(|row| {
            if row.best_chunk_bytes != default_chunk_bytes
                && row.best_payload_exact < row.default_payload_exact
            {
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

    let searched_target_window_payload_exact = target_rows
        .iter()
        .map(|row| row.searched_payload_exact)
        .sum::<usize>();
    let default_target_window_payload_exact = target_rows
        .iter()
        .map(|row| row.default_payload_exact)
        .sum::<usize>();
    let best_mix_target_window_payload_exact = target_rows
        .iter()
        .map(|row| row.best_payload_exact)
        .sum::<usize>();

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

        selected_target_window_payload_exact =
            selected_target_window_payload_exact.saturating_add(row.selected_payload_exact);

        let delta = (row.selected_payload_exact as i64) - (row.searched_payload_exact as i64);
        match delta.cmp(&0) {
            std::cmp::Ordering::Less => improved_target_window_count += 1,
            std::cmp::Ordering::Equal => equal_target_window_count += 1,
            std::cmp::Ordering::Greater => worsened_target_window_count += 1,
        }
    }

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

    Ok(ProcessReportOutcome {
        file_summary: FileSummary {
            input: report.input.clone(),
            searched_total_piecewise_payload_exact: report.total_piecewise_payload_exact,
            projected_default_total_piecewise_payload_exact,
            delta_default_total_piecewise_payload_exact:
                projected_default_total_piecewise_payload_exact as i64
                    - report.total_piecewise_payload_exact as i64,
            projected_unpriced_best_mix_total_piecewise_payload_exact,
            delta_unpriced_best_mix_total_piecewise_payload_exact:
                projected_unpriced_best_mix_total_piecewise_payload_exact as i64
                    - report.total_piecewise_payload_exact as i64,
            selected_total_piecewise_payload_exact,
            delta_selected_total_piecewise_payload_exact:
                selected_total_piecewise_payload_exact as i64
                    - report.total_piecewise_payload_exact as i64,
            target_window_count: target_rows.len(),
            searched_target_window_payload_exact,
            default_target_window_payload_exact,
            best_mix_target_window_payload_exact,
            selected_target_window_payload_exact,
            delta_selected_target_window_payload_exact:
                selected_target_window_payload_exact as i64
                    - searched_target_window_payload_exact as i64,
            override_path_bytes_exact,
            selected_override_window_count: selected_overrides.len(),
            improved_target_window_count,
            equal_target_window_count,
            worsened_target_window_count,
        },
        target_rows,
        override_candidates,
        selected_overrides,
    })
}

pub(crate) fn build_summary(
    recipe_fallback: &str,
    reports: &[FileReport],
    file_summaries: &[FileSummary],
    all_window_rows: &[WindowEvalRow],
    target_profile: &super::types::LawProfile,
    eval_config: &EvalConfig,
    chunk_candidates: &[usize],
    default_chunk_bytes: usize,
    global_best_chunk_counts: &BTreeMap<usize, usize>,
    union_law_count: usize,
) -> LocalMixSummary {
    let best_gain = all_window_rows
        .iter()
        .filter(|row| row.selected_gain_exact > 0)
        .max_by_key(|row| row.selected_gain_exact);
    let worst_loss = all_window_rows
        .iter()
        .filter(|row| row.selected_gain_exact < 0)
        .min_by_key(|row| row.selected_gain_exact);

    LocalMixSummary {
        recipe: reports
            .first()
            .map(|r| r.recipe.clone())
            .unwrap_or_else(|| recipe_fallback.to_string()),
        file_count: reports.len(),
        honest_file_count: reports.iter().filter(|r| r.honest_non_overlapping).count(),
        union_law_count,
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
        eval_chunk_candidates: chunk_candidates
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(","),
        eval_chunk_candidate_count: chunk_candidates.len(),
        default_local_chunk_bytes: default_chunk_bytes,
        default_local_chunk_window_wins: *global_best_chunk_counts
            .get(&default_chunk_bytes)
            .unwrap_or(&0),
        searched_total_piecewise_payload_exact: file_summaries
            .iter()
            .map(|f| f.searched_total_piecewise_payload_exact)
            .sum(),
        projected_default_total_piecewise_payload_exact: file_summaries
            .iter()
            .map(|f| f.projected_default_total_piecewise_payload_exact)
            .sum(),
        delta_default_total_piecewise_payload_exact: file_summaries
            .iter()
            .map(|f| f.delta_default_total_piecewise_payload_exact)
            .sum(),
        projected_unpriced_best_mix_total_piecewise_payload_exact: file_summaries
            .iter()
            .map(|f| f.projected_unpriced_best_mix_total_piecewise_payload_exact)
            .sum(),
        delta_unpriced_best_mix_total_piecewise_payload_exact: file_summaries
            .iter()
            .map(|f| f.delta_unpriced_best_mix_total_piecewise_payload_exact)
            .sum(),
        selected_total_piecewise_payload_exact: file_summaries
            .iter()
            .map(|f| f.selected_total_piecewise_payload_exact)
            .sum(),
        delta_selected_total_piecewise_payload_exact: file_summaries
            .iter()
            .map(|f| f.delta_selected_total_piecewise_payload_exact)
            .sum(),
        target_window_count: file_summaries.iter().map(|f| f.target_window_count).sum(),
        searched_target_window_payload_exact: file_summaries
            .iter()
            .map(|f| f.searched_target_window_payload_exact)
            .sum(),
        default_target_window_payload_exact: file_summaries
            .iter()
            .map(|f| f.default_target_window_payload_exact)
            .sum(),
        best_mix_target_window_payload_exact: file_summaries
            .iter()
            .map(|f| f.best_mix_target_window_payload_exact)
            .sum(),
        selected_target_window_payload_exact: file_summaries
            .iter()
            .map(|f| f.selected_target_window_payload_exact)
            .sum(),
        delta_selected_target_window_payload_exact: file_summaries
            .iter()
            .map(|f| f.delta_selected_target_window_payload_exact)
            .sum(),
        override_path_bytes_exact: file_summaries
            .iter()
            .map(|f| f.override_path_bytes_exact)
            .sum(),
        selected_override_window_count: file_summaries
            .iter()
            .map(|f| f.selected_override_window_count)
            .sum(),
        improved_target_window_count: file_summaries
            .iter()
            .map(|f| f.improved_target_window_count)
            .sum(),
        equal_target_window_count: file_summaries
            .iter()
            .map(|f| f.equal_target_window_count)
            .sum(),
        worsened_target_window_count: file_summaries
            .iter()
            .map(|f| f.worsened_target_window_count)
            .sum(),
        best_gain_input: best_gain.map(|row| row.input.clone()).unwrap_or_default(),
        best_gain_window_idx: best_gain.map(|row| row.window_idx).unwrap_or(0),
        best_gain_delta_payload_exact: best_gain.map(|row| row.selected_gain_exact).unwrap_or(0),
        worst_loss_input: worst_loss.map(|row| row.input.clone()).unwrap_or_default(),
        worst_loss_window_idx: worst_loss.map(|row| row.window_idx).unwrap_or(0),
        worst_loss_delta_payload_exact: worst_loss.map(|row| row.selected_gain_exact).unwrap_or(0),
    }
}

#[derive(Clone)]
struct TargetWindowTask {
    report_idx: usize,
    target_ordinal: usize,
    window: ManifestWindowRow,
}

fn collect_target_window_tasks(
    reports: &[FileReport],
    local_to_global_maps: &[BTreeMap<String, String>],
    target_global_law_id: &str,
) -> Vec<TargetWindowTask> {
    let mut tasks = Vec::new();
    for (report_idx, report) in reports.iter().enumerate() {
        let mut ordinal = 0usize;
        for window in &report.windows {
            if local_to_global_maps[report_idx]
                .get(&window.local_law_id)
                .map(|id| id == target_global_law_id)
                .unwrap_or(false)
            {
                tasks.push(TargetWindowTask {
                    report_idx,
                    target_ordinal: ordinal,
                    window: window.clone(),
                });
                ordinal += 1;
            }
        }
    }
    tasks
}

fn collect_report_target_tasks(
    report: &FileReport,
    local_to_global: &BTreeMap<String, String>,
    target_global_law_id: &str,
) -> Vec<TargetWindowTask> {
    let mut tasks = Vec::new();
    let mut ordinal = 0usize;
    for window in &report.windows {
        if local_to_global
            .get(&window.local_law_id)
            .map(|id| id == target_global_law_id)
            .unwrap_or(false)
        {
            tasks.push(TargetWindowTask {
                report_idx: 0,
                target_ordinal: ordinal,
                window: window.clone(),
            });
            ordinal += 1;
        }
    }
    tasks
}

#[allow(clippy::too_many_arguments)]
fn process_report_windows_serial(
    exe: &Path,
    args: &ApexLaneLawLocalMixFreezeArgs,
    eval_config: &EvalConfig,
    report: &FileReport,
    target_global_law_id: &str,
    input_bytes: &[u8],
    chunk_candidates: &[usize],
    default_chunk_bytes: usize,
    temp_dir: &Path,
    eval_cache: &BTreeMap<(String, usize, usize), FrozenEvalRow>,
    target_tasks: &[TargetWindowTask],
) -> Result<Vec<WindowEvalRow>> {
    let mut local_cache = BTreeMap::<(String, usize, usize), FrozenEvalRow>::new();
    let mut rows = Vec::with_capacity(target_tasks.len());
    for task in target_tasks {
        rows.push(build_window_row(
            exe,
            args,
            eval_config,
            report,
            target_global_law_id,
            input_bytes,
            chunk_candidates,
            default_chunk_bytes,
            temp_dir,
            eval_cache,
            &mut local_cache,
            task,
        )?);
    }
    Ok(rows)
}

#[allow(clippy::too_many_arguments)]
fn process_report_windows_parallel(
    exe: &Path,
    args: &ApexLaneLawLocalMixFreezeArgs,
    eval_config: &EvalConfig,
    report: &FileReport,
    target_global_law_id: &str,
    input_bytes: &[u8],
    chunk_candidates: &[usize],
    default_chunk_bytes: usize,
    temp_dir: &Path,
    eval_cache: &BTreeMap<(String, usize, usize), FrozenEvalRow>,
    jobs: usize,
    target_tasks: &[TargetWindowTask],
) -> Result<Vec<WindowEvalRow>> {
    let chunk_size = div_ceil(target_tasks.len(), jobs);
    let mut rows = Vec::<WindowEvalRow>::with_capacity(target_tasks.len());

    thread::scope(|scope| -> Result<()> {
        let mut handles = Vec::new();

        for task_chunk in target_tasks.chunks(chunk_size) {
            let exe_ref = exe;
            let args_ref = args;
            let eval_config_ref = eval_config;
            let report_ref = report;
            let target_global_law_id_ref = target_global_law_id;
            let input_bytes_ref = input_bytes;
            let chunk_candidates_ref = chunk_candidates;
            let temp_dir_ref = temp_dir;
            let eval_cache_ref = eval_cache;

            handles.push(scope.spawn(move || -> Result<Vec<WindowEvalRow>> {
                let mut local_rows = Vec::with_capacity(task_chunk.len());
                let mut local_cache = BTreeMap::<(String, usize, usize), FrozenEvalRow>::new();

                for task in task_chunk {
                    local_rows.push(build_window_row(
                        exe_ref,
                        args_ref,
                        eval_config_ref,
                        report_ref,
                        target_global_law_id_ref,
                        input_bytes_ref,
                        chunk_candidates_ref,
                        default_chunk_bytes,
                        temp_dir_ref,
                        eval_cache_ref,
                        &mut local_cache,
                        task,
                    )?);
                }

                Ok(local_rows)
            }));
        }

        for handle in handles {
            let local_rows = handle
                .join()
                .map_err(|_| anyhow!("process_report window worker panicked"))??;
            rows.extend(local_rows);
        }

        Ok(())
    })?;

    Ok(rows)
}

#[allow(clippy::too_many_arguments)]
fn build_window_row(
    exe: &Path,
    args: &ApexLaneLawLocalMixFreezeArgs,
    eval_config: &EvalConfig,
    report: &FileReport,
    target_global_law_id: &str,
    input_bytes: &[u8],
    chunk_candidates: &[usize],
    default_chunk_bytes: usize,
    temp_dir: &Path,
    eval_cache: &BTreeMap<(String, usize, usize), FrozenEvalRow>,
    local_cache: &mut BTreeMap<(String, usize, usize), FrozenEvalRow>,
    task: &TargetWindowTask,
) -> Result<WindowEvalRow> {
    let default_eval = cached_or_eval_window(
        exe,
        args,
        eval_config,
        &report.input,
        input_bytes,
        &task.window,
        default_chunk_bytes,
        temp_dir,
        eval_cache,
        local_cache,
    )?;

    let mut best_eval = default_eval.clone();
    let mut best_chunk_bytes = default_chunk_bytes;

    for &chunk_bytes in chunk_candidates {
        let eval = cached_or_eval_window(
            exe,
            args,
            eval_config,
            &report.input,
            input_bytes,
            &task.window,
            chunk_bytes,
            temp_dir,
            eval_cache,
            local_cache,
        )?;
        if eval.compact_field_total_payload_exact < best_eval.compact_field_total_payload_exact
            || (eval.compact_field_total_payload_exact == best_eval.compact_field_total_payload_exact
                && chunk_bytes < best_chunk_bytes)
        {
            best_eval = eval;
            best_chunk_bytes = chunk_bytes;
        }
    }

    Ok(WindowEvalRow {
        input: report.input.clone(),
        window_idx: task.window.window_idx,
        target_ordinal: task.target_ordinal,
        start: task.window.start,
        end: task.window.end,
        span_bytes: task.window.span_bytes,
        searched_local_law_id: task.window.local_law_id.clone(),
        searched_global_law_id: target_global_law_id.to_string(),
        searched_chunk_bytes: task.window.chunk_bytes,
        searched_payload_exact: task.window.compact_field_total_payload_exact,
        default_payload_exact: default_eval.compact_field_total_payload_exact,
        best_chunk_bytes,
        best_payload_exact: best_eval.compact_field_total_payload_exact,
        selected_chunk_bytes: default_chunk_bytes,
        selected_payload_exact: default_eval.compact_field_total_payload_exact,
        default_gain_exact: (task.window.compact_field_total_payload_exact as i64)
            - (default_eval.compact_field_total_payload_exact as i64),
        best_gain_exact: (task.window.compact_field_total_payload_exact as i64)
            - (best_eval.compact_field_total_payload_exact as i64),
        selected_gain_exact: (task.window.compact_field_total_payload_exact as i64)
            - (default_eval.compact_field_total_payload_exact as i64),
    })
}

#[allow(clippy::too_many_arguments)]
fn cached_or_eval_window(
    exe: &Path,
    args: &ApexLaneLawLocalMixFreezeArgs,
    eval_config: &EvalConfig,
    input_name: &str,
    input_bytes: &[u8],
    window: &ManifestWindowRow,
    chunk_bytes: usize,
    temp_dir: &Path,
    eval_cache: &BTreeMap<(String, usize, usize), FrozenEvalRow>,
    local_cache: &mut BTreeMap<(String, usize, usize), FrozenEvalRow>,
) -> Result<FrozenEvalRow> {
    let key = (input_name.to_string(), window.window_idx, chunk_bytes);
    if let Some(row) = eval_cache.get(&key) {
        return Ok(row.clone());
    }
    if let Some(row) = local_cache.get(&key) {
        return Ok(row.clone());
    }
    eval_window(
        exe,
        args,
        eval_config,
        input_name,
        input_bytes,
        window,
        chunk_bytes,
        temp_dir,
        local_cache,
    )
}

fn div_ceil(n: usize, d: usize) -> usize {
    if d == 0 {
        return n.max(1);
    }
    let q = n / d;
    let r = n % d;
    if r == 0 {
        q
    } else {
        q + 1
    }
}