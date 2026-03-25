use anyhow::{anyhow, Context, Result};
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::Path;
use std::thread;

use crate::cmd::apextrace::ApexLaneLawLocalMixFreezeArgs;

use super::children::{parse_knob_signature, run_child_apex_lane_manifest};
use super::config::{build_eval_config, select_chunk_candidates, select_target_profile};
use super::eval::choose_default_chunk;
use super::parsing::parse_manifest_txt;
use super::pipeline::{
    build_summary, process_report, scan_global_best_chunk_counts, ProcessReportOutcome,
};
use super::profiles::{build_profiles, build_shared_law_ids};
use super::render::render_body;
use super::types::{FileReport, FrozenEvalRow};
use super::util::make_temp_dir;
use super::super::common::write_or_print;

pub(crate) fn run_apex_lane_law_local_mix_freeze(
    args: ApexLaneLawLocalMixFreezeArgs,
) -> Result<()> {
    if args.inputs.is_empty() {
        return Err(anyhow!(
            "apex-lane-law-local-mix-freeze requires at least one --in input"
        ));
    }

    let exe = env::current_exe()
        .context("resolve current executable for apex-lane-law-local-mix-freeze")?;
    let local_mix_jobs = resolve_local_mix_jobs();
    let reports = load_reports(&exe, &args, local_mix_jobs)?;
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
    let chunk_candidates = select_chunk_candidates(
        &args,
        &reports,
        &shared_law_ids,
        target_profile,
        dominant.chunk_bytes,
    )?;
    let local_to_global_maps = build_local_to_global_maps(&reports, &shared_law_ids);
    let temp_dir = make_temp_dir("apex_lane_law_local_mix_freeze")?;
    let mut eval_cache = BTreeMap::<(String, usize, usize), FrozenEvalRow>::new();

    eprintln!(
        "apex-lane-law-local-mix-freeze: local_mix_jobs={} inputs={} chunk_candidates={}",
        local_mix_jobs,
        reports.len(),
        chunk_candidates.len(),
    );

    let global_best_chunk_counts = scan_global_best_chunk_counts(
        &exe,
        &args,
        &eval_config,
        &reports,
        &local_to_global_maps,
        &target_profile.global_law_id,
        dominant.chunk_bytes,
        &chunk_candidates,
        &temp_dir,
        &mut eval_cache,
        local_mix_jobs,
    )?;

    let default_chunk_bytes = choose_default_chunk(
        &args,
        &chunk_candidates,
        &global_best_chunk_counts,
        &reports,
        &local_to_global_maps,
        &eval_cache,
        &target_profile.global_law_id,
        &temp_dir,
        &exe,
        &eval_config,
        &args,
    )?;

    let outcomes = process_reports_parallel(
        &exe,
        &args,
        &eval_config,
        &reports,
        &local_to_global_maps,
        &target_profile.global_law_id,
        &chunk_candidates,
        default_chunk_bytes,
        &temp_dir,
        &eval_cache,
        local_mix_jobs,
    )?;

    let mut file_summaries = Vec::with_capacity(outcomes.len());
    let mut all_window_rows = Vec::new();
    let mut override_candidates_all = Vec::new();
    let mut override_selected_all = Vec::new();

    for outcome in outcomes {
        file_summaries.push(outcome.file_summary);
        all_window_rows.extend(outcome.target_rows);
        override_candidates_all.extend(outcome.override_candidates);
        override_selected_all.extend(outcome.selected_overrides);
    }

    cleanup_temp_dir(&args, &temp_dir);

    let summary = build_summary(
        &args.recipe,
        &reports,
        &file_summaries,
        &all_window_rows,
        target_profile,
        &eval_config,
        &chunk_candidates,
        default_chunk_bytes,
        &global_best_chunk_counts,
        shared_law_ids.len(),
    );

    let body = render_body(
        args.format,
        &summary,
        &file_summaries,
        &all_window_rows,
        &override_candidates_all,
        &override_selected_all,
        args.top_rows,
    );
    write_or_print(args.out.as_deref(), &body)?;
    emit_summary_log(args.out.as_deref(), &summary);
    Ok(())
}

fn resolve_local_mix_jobs() -> usize {
    let available = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .max(1);
    let default_jobs = if available >= 8 {
        3
    } else if available >= 4 {
        2
    } else {
        1
    };
    let parsed = env::var("K8DNZ_LOCAL_MIX_JOBS")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(default_jobs);
    parsed.clamp(1, available.min(8))
}

fn load_reports(
    exe: &Path,
    args: &ApexLaneLawLocalMixFreezeArgs,
    jobs: usize,
) -> Result<Vec<FileReport>> {
    if jobs <= 1 || args.inputs.len() <= 1 {
        return load_reports_serial(exe, args);
    }

    let chunk_size = div_ceil(args.inputs.len(), jobs);
    let mut joined = Vec::<(usize, FileReport)>::new();

    thread::scope(|scope| -> Result<()> {
        let mut handles = Vec::new();
        for (chunk_idx, input_chunk) in args.inputs.chunks(chunk_size).enumerate() {
            let base_idx = chunk_idx * chunk_size;
            handles.push(scope.spawn(move || -> Result<Vec<(usize, FileReport)>> {
                let mut local = Vec::with_capacity(input_chunk.len());
                for (offset, input) in input_chunk.iter().enumerate() {
                    let output = run_child_apex_lane_manifest(exe, args, input)?;
                    let report = parse_manifest_txt(&output)
                        .with_context(|| format!("parse apex-lane-manifest output for {}", input))?;
                    local.push((base_idx + offset, report));
                }
                Ok(local)
            }));
        }

        for handle in handles {
            let local = handle
                .join()
                .map_err(|_| anyhow!("load_reports worker panicked"))??;
            joined.extend(local);
        }
        Ok(())
    })?;

    joined.sort_by_key(|(idx, _)| *idx);
    Ok(joined.into_iter().map(|(_, report)| report).collect())
}

fn load_reports_serial(exe: &Path, args: &ApexLaneLawLocalMixFreezeArgs) -> Result<Vec<FileReport>> {
    let mut reports = Vec::with_capacity(args.inputs.len());
    for input in &args.inputs {
        let output = run_child_apex_lane_manifest(exe, args, input)?;
        let report = parse_manifest_txt(&output)
            .with_context(|| format!("parse apex-lane-manifest output for {}", input))?;
        reports.push(report);
    }
    Ok(reports)
}

#[allow(clippy::too_many_arguments)]
fn process_reports_parallel(
    exe: &Path,
    args: &ApexLaneLawLocalMixFreezeArgs,
    eval_config: &super::types::EvalConfig,
    reports: &[FileReport],
    local_to_global_maps: &[BTreeMap<String, String>],
    target_global_law_id: &str,
    chunk_candidates: &[usize],
    default_chunk_bytes: usize,
    temp_dir: &Path,
    eval_cache: &BTreeMap<(String, usize, usize), FrozenEvalRow>,
    jobs: usize,
) -> Result<Vec<ProcessReportOutcome>> {
    if jobs <= 1 || reports.len() <= 1 {
        let mut outcomes = Vec::with_capacity(reports.len());
        for (report_idx, report) in reports.iter().enumerate() {
            outcomes.push(process_report(
                exe,
                args,
                eval_config,
                report,
                &local_to_global_maps[report_idx],
                target_global_law_id,
                chunk_candidates,
                default_chunk_bytes,
                temp_dir,
                eval_cache,
                jobs,
            )?);
        }
        return Ok(outcomes);
    }

    let chunk_size = div_ceil(reports.len(), jobs);
    let mut joined = Vec::<(usize, ProcessReportOutcome)>::new();

    thread::scope(|scope| -> Result<()> {
        let mut handles = Vec::new();
        for (chunk_idx, report_chunk) in reports.chunks(chunk_size).enumerate() {
            let base_idx = chunk_idx * chunk_size;
            handles.push(scope.spawn(move || -> Result<Vec<(usize, ProcessReportOutcome)>> {
                let mut local = Vec::with_capacity(report_chunk.len());
                for offset in 0..report_chunk.len() {
                    let global_idx = base_idx + offset;
                    let outcome = process_report(
                        exe,
                        args,
                        eval_config,
                        &reports[global_idx],
                        &local_to_global_maps[global_idx],
                        target_global_law_id,
                        chunk_candidates,
                        default_chunk_bytes,
                        temp_dir,
                        eval_cache,
                        jobs,
                    )?;
                    local.push((global_idx, outcome));
                }
                Ok(local)
            }));
        }

        for handle in handles {
            let local = handle
                .join()
                .map_err(|_| anyhow!("process_report worker panicked"))??;
            joined.extend(local);
        }
        Ok(())
    })?;

    joined.sort_by_key(|(idx, _)| *idx);
    Ok(joined.into_iter().map(|(_, outcome)| outcome).collect())
}

fn build_local_to_global_maps(
    reports: &[FileReport],
    shared_law_ids: &BTreeMap<super::types::ReplayLawTuple, String>,
) -> Vec<BTreeMap<String, String>> {
    reports
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
        .collect::<Vec<_>>()
}

fn cleanup_temp_dir(args: &ApexLaneLawLocalMixFreezeArgs, temp_dir: &Path) {
    if !args.keep_temp_dir {
        if let Err(err) = fs::remove_dir_all(temp_dir) {
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
}

fn emit_summary_log(out_path: Option<&str>, summary: &super::types::LocalMixSummary) {
    if let Some(path) = out_path {
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
}

fn div_ceil(n: usize, d: usize) -> usize {
    if d == 0 {
        return n.max(1);
    }
    let q = n / d;
    let r = n % d;
    if r == 0 { q } else { q + 1 }
}
