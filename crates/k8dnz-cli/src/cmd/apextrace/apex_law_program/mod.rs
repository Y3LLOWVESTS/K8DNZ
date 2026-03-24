mod args;
mod bincodec;
mod build;
mod exec;
mod parse;
mod plan;
mod replay;
mod report;
mod types;

#[cfg(test)]
mod tests;

use anyhow::{Context, Result};
use clap::Parser;
use std::fs;
use std::time::Instant;

use self::args::{BuildArgs, Cli, Cmd, InspectArgs, ReplayArgs};
use self::build::materialize_build;
use self::exec::{resolve_k8dnz_cli_exe, run_surface_scoreboard};
use self::replay::replay_artifact;
use self::report::{render_artifact_report, render_replay_report};
use self::types::LawProgramArtifact;

pub(crate) fn main_entry() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Build(args) => run_build(args),
        Cmd::Inspect(args) => run_inspect(args),
        Cmd::Replay(args) => run_replay(args),
    }
}

fn run_build(args: BuildArgs) -> Result<()> {
    let cli_exe = resolve_k8dnz_cli_exe()?;
    let materialized = materialize_build(&cli_exe, &args)?;
    let bytes = materialized.artifact.encode()?;

    fs::write(&args.out, &bytes).with_context(|| format!("write artifact {}", args.out))?;

    let report = render_artifact_report(
        &materialized.artifact,
        &bytes,
        &args.out,
        if args.emit_body_scoreboard {
            Some(&materialized.body_scores)
        } else {
            None
        },
    );

    if let Some(path) = args.out_report.as_deref() {
        fs::write(path, report).with_context(|| format!("write report {}", path))?;
        eprintln!(
            "apex-law-program build: artifact={} bytes={} windows={} overrides={} target={} body_select_objective={} override_path_mode={} override_path_bytes_exact={} selected_total_piecewise_payload_exact={} closure_total_exact={} closure_penalty_exact={}",
            args.out,
            bytes.len(),
            materialized.artifact.windows.len(),
            materialized.artifact.overrides.len(),
            materialized.artifact.summary.target_global_law_id,
            materialized.artifact.config.body_select_objective,
            materialized.artifact.summary.override_path_mode,
            materialized.artifact.summary.override_path_bytes_exact,
            materialized.artifact.summary.selected_total_piecewise_payload_exact,
            materialized.artifact.summary.closure_total_exact,
            materialized.artifact.summary.closure_penalty_exact,
        );
    } else {
        print!("{report}");
    }

    Ok(())
}

fn run_inspect(args: InspectArgs) -> Result<()> {
    let bytes =
        fs::read(&args.artifact).with_context(|| format!("read artifact {}", args.artifact))?;
    let artifact = LawProgramArtifact::decode(&bytes)?;
    let report = render_artifact_report(&artifact, &bytes, &args.artifact, None);
    print!("{report}");
    Ok(())
}

fn run_replay(args: ReplayArgs) -> Result<()> {
    let bytes =
        fs::read(&args.artifact).with_context(|| format!("read artifact {}", args.artifact))?;
    let artifact = LawProgramArtifact::decode(&bytes)?;
    let cli_exe = resolve_k8dnz_cli_exe()?;

    eprintln!(
        "apex-law-program replay: artifact={} compare_surfaces={} inputs={} windows={} target={}",
        args.artifact,
        args.compare_surfaces,
        artifact.config.inputs.len(),
        artifact.windows.len(),
        artifact.summary.target_global_law_id,
    );

    let (rows, file_summaries) = replay_artifact(&cli_exe, &artifact)?;

    let replay_selected_total_piecewise_payload_exact = file_summaries
        .iter()
        .map(|row| row.replay_selected_total_piecewise_payload_exact)
        .sum::<i64>();

    let collapse_90_failures = rows.iter().filter(|row| row.collapse_90_flag).count();
    let newline_extinct_failures = rows.iter().filter(|row| row.newline_extinct_flag).count();
    let drift_exact = replay_selected_total_piecewise_payload_exact
        - artifact.summary.selected_total_piecewise_payload_exact;

    let scoreboard = if args.compare_surfaces {
        let compare_started = Instant::now();
        eprintln!(
            "apex-law-program replay: compare-surfaces requested target={} artifact_selected_total_piecewise_payload_exact={} replay_selected_total_piecewise_payload_exact={}",
            artifact.summary.target_global_law_id,
            artifact.summary.selected_total_piecewise_payload_exact,
            replay_selected_total_piecewise_payload_exact,
        );

        let board = run_surface_scoreboard(
            &cli_exe,
            &artifact,
            replay_selected_total_piecewise_payload_exact,
        )?;

        eprintln!(
            "apex-law-program replay: compare-surfaces finished elapsed_ms={} frozen_total_piecewise_payload_exact={:?} split_total_piecewise_payload_exact={:?} bridge_total_piecewise_payload_exact={:?} best_surface={} best_total_piecewise_payload_exact={} best_delta_vs_artifact_exact={}",
            compare_started.elapsed().as_millis(),
            board.frozen_total_piecewise_payload_exact,
            board.split_total_piecewise_payload_exact,
            board.bridge_total_piecewise_payload_exact,
            board.best_surface,
            board.best_total_piecewise_payload_exact,
            board.best_delta_vs_artifact_exact,
        );

        Some(board)
    } else {
        None
    };

    let report = render_replay_report(
        &args.artifact,
        &artifact,
        &rows,
        &file_summaries,
        replay_selected_total_piecewise_payload_exact,
        drift_exact,
        collapse_90_failures,
        newline_extinct_failures,
        scoreboard.as_ref(),
    );

    if let Some(path) = args.out_report.as_deref() {
        fs::write(path, report).with_context(|| format!("write report {}", path))?;
        eprintln!(
            "apex-law-program replay: artifact={} replay_selected_total_piecewise_payload_exact={} artifact_selected_total_piecewise_payload_exact={} drift_exact={} collapse_90_failures={} newline_extinct_failures={} compare_surfaces={}",
            args.artifact,
            replay_selected_total_piecewise_payload_exact,
            artifact.summary.selected_total_piecewise_payload_exact,
            drift_exact,
            collapse_90_failures,
            newline_extinct_failures,
            args.compare_surfaces,
        );
    } else {
        print!("{report}");
    }

    Ok(())
}
