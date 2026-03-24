use anyhow::{anyhow, bail, Context, Result};
use std::env;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::thread;
use std::time::Instant;

use super::args::BuildArgs;
use super::parse::{parse_required_i64, parse_txt_summary};
use super::types::{LawProgramArtifact, SurfaceScoreboard};

const SURFACE_FREEZE: &str = "apex-lane-law-freeze";
const SURFACE_SPLIT: &str = "apex-lane-law-split-freeze";
const SURFACE_BRIDGE: &str = "apex-lane-law-bridge-freeze";

pub(crate) fn resolve_k8dnz_cli_exe() -> Result<PathBuf> {
    let exe = env::current_exe().context("resolve current executable")?;
    let parent = exe
        .parent()
        .ok_or_else(|| anyhow!("current executable has no parent: {}", exe.display()))?;
    let bin_name = if cfg!(windows) {
        "k8dnz-cli.exe"
    } else {
        "k8dnz-cli"
    };
    let path = parent.join(bin_name);
    if path.exists() {
        Ok(path)
    } else {
        bail!(
            "missing sibling executable {}; run cargo build first",
            path.display()
        )
    }
}

pub(crate) fn run_local_mix(cli_exe: &Path, args: &BuildArgs, format: &str) -> Result<Output> {
    let mut cmd = Command::new(cli_exe);
    cmd.arg("apextrace")
        .arg("apex-lane-law-local-mix-freeze")
        .arg("--recipe")
        .arg(&args.recipe);

    for input in &args.inputs {
        cmd.arg("--in").arg(input);
    }

    append_manifest_base_args(&mut cmd, args);
    append_local_mix_only_args(&mut cmd, args);
    cmd.arg("--format").arg(format);

    let output = cmd
        .output()
        .with_context(|| format!("run local mix child {}", cli_exe.display()))?;

    if !output.status.success() {
        bail!(
            "local mix child failed status={}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    Ok(output)
}

pub(crate) fn run_manifest_txt(cli_exe: &Path, args: &BuildArgs, input: &str) -> Result<Output> {
    let mut cmd = Command::new(cli_exe);
    cmd.arg("apextrace")
        .arg("apex-lane-manifest")
        .arg("--recipe")
        .arg(&args.recipe)
        .arg("--in")
        .arg(input);

    append_manifest_base_args(&mut cmd, args);
    cmd.arg("--format").arg("txt");

    let output = cmd
        .output()
        .with_context(|| format!("run manifest child {}", cli_exe.display()))?;

    if !output.status.success() {
        bail!(
            "manifest child failed status={}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    Ok(output)
}

pub(crate) fn run_surface_scoreboard(
    cli_exe: &Path,
    artifact: &LawProgramArtifact,
    replay_selected_total_piecewise_payload_exact: i64,
) -> Result<SurfaceScoreboard> {
    let jobs = resolve_surface_jobs();
    let started = Instant::now();

    eprintln!(
        "apex-law-program compare-surfaces: start jobs={} target={} inputs={}",
        jobs,
        artifact.summary.target_global_law_id,
        artifact.config.inputs.len(),
    );

    let (frozen_total, split_total, bridge_total) = match jobs {
        0 | 1 => run_surface_scoreboard_serial(cli_exe, artifact)?,
        2 => run_surface_scoreboard_parallel_two(cli_exe, artifact)?,
        _ => run_surface_scoreboard_parallel_three(cli_exe, artifact)?,
    };

    let (best_surface, best_total_piecewise_payload_exact) = select_best_surface(
        artifact.summary.selected_total_piecewise_payload_exact,
        replay_selected_total_piecewise_payload_exact,
        frozen_total,
        split_total,
        bridge_total,
    );
    let best_delta_vs_artifact_exact =
        best_total_piecewise_payload_exact - artifact.summary.selected_total_piecewise_payload_exact;

    eprintln!(
        "apex-law-program compare-surfaces: done elapsed_ms={} artifact_selected_total_piecewise_payload_exact={} replay_selected_total_piecewise_payload_exact={} frozen_total_piecewise_payload_exact={} split_total_piecewise_payload_exact={} bridge_total_piecewise_payload_exact={} best_surface={} best_total_piecewise_payload_exact={} best_delta_vs_artifact_exact={}",
        started.elapsed().as_millis(),
        artifact.summary.selected_total_piecewise_payload_exact,
        replay_selected_total_piecewise_payload_exact,
        frozen_total,
        split_total,
        bridge_total,
        best_surface,
        best_total_piecewise_payload_exact,
        best_delta_vs_artifact_exact,
    );

    Ok(SurfaceScoreboard {
        searched_total_piecewise_payload_exact: artifact
            .summary
            .searched_total_piecewise_payload_exact,
        artifact_selected_total_piecewise_payload_exact: artifact
            .summary
            .selected_total_piecewise_payload_exact,
        replay_selected_total_piecewise_payload_exact,
        frozen_total_piecewise_payload_exact: Some(frozen_total),
        split_total_piecewise_payload_exact: Some(split_total),
        bridge_total_piecewise_payload_exact: Some(bridge_total),
        best_surface,
        best_total_piecewise_payload_exact,
        best_delta_vs_artifact_exact,
    })
}

fn run_surface_scoreboard_serial(
    cli_exe: &Path,
    artifact: &LawProgramArtifact,
) -> Result<(i64, i64, i64)> {
    let frozen_total = run_surface_total_logged(cli_exe, artifact, SURFACE_FREEZE)?;
    let split_total = run_surface_total_logged(cli_exe, artifact, SURFACE_SPLIT)?;
    let bridge_total = run_surface_total_logged(cli_exe, artifact, SURFACE_BRIDGE)?;
    Ok((frozen_total, split_total, bridge_total))
}

fn run_surface_scoreboard_parallel_two(
    cli_exe: &Path,
    artifact: &LawProgramArtifact,
) -> Result<(i64, i64, i64)> {
    let (frozen_total, split_total) = thread::scope(|scope| {
        let frozen = scope.spawn(|| run_surface_total_logged(cli_exe, artifact, SURFACE_FREEZE));
        let split = scope.spawn(|| run_surface_total_logged(cli_exe, artifact, SURFACE_SPLIT));

        let frozen_total = join_surface(frozen, SURFACE_FREEZE)?;
        let split_total = join_surface(split, SURFACE_SPLIT)?;
        Ok::<(i64, i64), anyhow::Error>((frozen_total, split_total))
    })?;

    let bridge_total = run_surface_total_logged(cli_exe, artifact, SURFACE_BRIDGE)?;
    Ok((frozen_total, split_total, bridge_total))
}

fn run_surface_scoreboard_parallel_three(
    cli_exe: &Path,
    artifact: &LawProgramArtifact,
) -> Result<(i64, i64, i64)> {
    thread::scope(|scope| {
        let frozen = scope.spawn(|| run_surface_total_logged(cli_exe, artifact, SURFACE_FREEZE));
        let split = scope.spawn(|| run_surface_total_logged(cli_exe, artifact, SURFACE_SPLIT));
        let bridge = scope.spawn(|| run_surface_total_logged(cli_exe, artifact, SURFACE_BRIDGE));

        let frozen_total = join_surface(frozen, SURFACE_FREEZE)?;
        let split_total = join_surface(split, SURFACE_SPLIT)?;
        let bridge_total = join_surface(bridge, SURFACE_BRIDGE)?;
        Ok::<(i64, i64, i64), anyhow::Error>((frozen_total, split_total, bridge_total))
    })
}

fn join_surface<'scope>(
    handle: thread::ScopedJoinHandle<'scope, Result<i64>>,
    subcmd: &str,
) -> Result<i64> {
    match handle.join() {
        Ok(result) => result,
        Err(_) => Err(anyhow!("surface worker panicked for {}", subcmd)),
    }
}

fn resolve_surface_jobs() -> usize {
    let default_jobs = std::thread::available_parallelism()
        .map(|n| n.get().min(2))
        .unwrap_or(1);

    env::var("K8DNZ_SURFACE_JOBS")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(default_jobs)
        .clamp(1, 3)
}

fn run_surface_total_logged(
    cli_exe: &Path,
    artifact: &LawProgramArtifact,
    subcmd: &str,
) -> Result<i64> {
    let started = Instant::now();
    eprintln!(
        "apex-law-program compare-surfaces: surface-start subcmd={} target={} inputs={}",
        subcmd,
        artifact.summary.target_global_law_id,
        artifact.config.inputs.len(),
    );

    let total = run_surface_total(cli_exe, artifact, subcmd)?;

    eprintln!(
        "apex-law-program compare-surfaces: surface-done subcmd={} elapsed_ms={} total_piecewise_payload_exact={}",
        subcmd,
        started.elapsed().as_millis(),
        total,
    );

    Ok(total)
}

pub(crate) fn run_surface_total(
    cli_exe: &Path,
    artifact: &LawProgramArtifact,
    subcmd: &str,
) -> Result<i64> {
    let mut cmd = Command::new(cli_exe);
    cmd.arg("apextrace")
        .arg(subcmd)
        .arg("--recipe")
        .arg(&artifact.config.recipe);

    for input in &artifact.config.inputs {
        cmd.arg("--in").arg(input);
    }

    append_surface_replay_args(&mut cmd, artifact, subcmd);
    cmd.arg("--format").arg("txt");

    let output = cmd
        .output()
        .with_context(|| format!("run surface {}", subcmd))?;

    if !output.status.success() {
        bail!(
            "surface {} failed status={}\nstdout:\n{}\nstderr:\n{}",
            subcmd,
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let map = parse_txt_summary(&output.stdout)?;
    parse_required_i64(&map, "frozen_total_piecewise_payload_exact")
}

fn select_best_surface(
    artifact_selected_total_piecewise_payload_exact: i64,
    replay_selected_total_piecewise_payload_exact: i64,
    frozen_total_piecewise_payload_exact: i64,
    split_total_piecewise_payload_exact: i64,
    bridge_total_piecewise_payload_exact: i64,
) -> (String, i64) {
    let candidates = [
        ("artifact", artifact_selected_total_piecewise_payload_exact),
        ("replay", replay_selected_total_piecewise_payload_exact),
        ("freeze", frozen_total_piecewise_payload_exact),
        ("split-freeze", split_total_piecewise_payload_exact),
        ("bridge-freeze", bridge_total_piecewise_payload_exact),
    ];

    let (name, value) = candidates
        .into_iter()
        .min_by_key(|(_, total)| *total)
        .expect("surface candidates should not be empty");

    (name.to_string(), value)
}

fn append_manifest_base_args(cmd: &mut Command, args: &BuildArgs) {
    cmd.arg("--max-ticks")
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
        .arg(&args.chunk_search_objective)
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
            if args.newline_only_from_spacelike {
                "true"
            } else {
                "false"
            }
        ))
        .arg("--merge-gap-bytes")
        .arg(args.merge_gap_bytes.to_string());

    if args.allow_overlap_scout {
        cmd.arg("--allow-overlap-scout");
    }
}

fn append_local_mix_only_args(cmd: &mut Command, args: &BuildArgs) {
    cmd.arg("--local-chunk-sweep")
        .arg(&args.local_chunk_sweep)
        .arg("--min-override-gain-exact")
        .arg(args.min_override_gain_exact.to_string())
        .arg("--exact-subset-limit")
        .arg(args.exact_subset_limit.to_string())
        .arg("--top-rows")
        .arg(args.top_rows.to_string());

    if let Some(v) = args.local_chunk_search_objective.as_deref() {
        cmd.arg("--local-chunk-search-objective").arg(v);
    }
    if let Some(v) = args.local_chunk_raw_slack {
        cmd.arg("--local-chunk-raw-slack").arg(v.to_string());
    }
    if let Some(v) = args.default_local_chunk_bytes {
        cmd.arg("--default-local-chunk-bytes").arg(v.to_string());
    }
    if let Some(v) = args.freeze_boundary_band {
        cmd.arg("--freeze-boundary-band").arg(v.to_string());
    }
    if let Some(v) = args.freeze_field_margin {
        cmd.arg("--freeze-field-margin").arg(v.to_string());
    }
    if let Some(v) = args.freeze_newline_demote_margin {
        cmd.arg("--freeze-newline-demote-margin")
            .arg(v.to_string());
    }
    if let Some(v) = args.global_law_id.as_deref() {
        cmd.arg("--global-law-id").arg(v);
    }
}

fn append_surface_replay_args(cmd: &mut Command, artifact: &LawProgramArtifact, subcmd: &str) {
    cmd.arg("--max-ticks")
        .arg(artifact.config.max_ticks.to_string())
        .arg("--window-bytes")
        .arg(artifact.config.window_bytes.to_string())
        .arg("--step-bytes")
        .arg(artifact.config.step_bytes.to_string())
        .arg("--max-windows")
        .arg(artifact.config.max_windows.to_string())
        .arg("--seed-from")
        .arg(artifact.config.seed_from.to_string())
        .arg("--seed-count")
        .arg(artifact.config.seed_count.to_string())
        .arg("--seed-step")
        .arg(artifact.config.seed_step.to_string())
        .arg("--recipe-seed")
        .arg(artifact.config.recipe_seed.to_string())
        .arg("--chunk-sweep")
        .arg(&artifact.config.chunk_sweep)
        .arg("--chunk-search-objective")
        .arg(&artifact.config.chunk_search_objective)
        .arg("--chunk-raw-slack")
        .arg(artifact.config.chunk_raw_slack.to_string())
        .arg("--map-max-depth")
        .arg(artifact.config.map_max_depth.to_string())
        .arg("--map-depth-shift")
        .arg(artifact.config.map_depth_shift.to_string())
        .arg("--boundary-band-sweep")
        .arg(&artifact.config.boundary_band_sweep)
        .arg("--boundary-delta")
        .arg(artifact.config.boundary_delta.to_string())
        .arg("--field-margin-sweep")
        .arg(&artifact.config.field_margin_sweep)
        .arg("--newline-margin-add")
        .arg(artifact.config.newline_margin_add.to_string())
        .arg("--space-to-newline-margin-add")
        .arg(artifact.config.space_to_newline_margin_add.to_string())
        .arg("--newline-share-ppm-min")
        .arg(artifact.config.newline_share_ppm_min.to_string())
        .arg("--newline-override-budget")
        .arg(artifact.config.newline_override_budget.to_string())
        .arg("--newline-demote-margin-sweep")
        .arg(&artifact.config.newline_demote_margin_sweep)
        .arg("--newline-demote-keep-ppm-min")
        .arg(artifact.config.newline_demote_keep_ppm_min.to_string())
        .arg("--newline-demote-keep-min")
        .arg(artifact.config.newline_demote_keep_min.to_string())
        .arg(format!(
            "--newline-only-from-spacelike={}",
            if artifact.config.newline_only_from_spacelike {
                "true"
            } else {
                "false"
            }
        ))
        .arg("--merge-gap-bytes")
        .arg(artifact.config.merge_gap_bytes.to_string())
        .arg("--global-law-id")
        .arg(&artifact.summary.target_global_law_id)
        .arg("--freeze-boundary-band")
        .arg(artifact.summary.eval_boundary_band.to_string())
        .arg("--freeze-field-margin")
        .arg(artifact.summary.eval_field_margin.to_string())
        .arg("--freeze-newline-demote-margin")
        .arg(artifact.summary.eval_newline_demote_margin.to_string());

    match subcmd {
        SURFACE_FREEZE => {
            cmd.arg("--freeze-chunk-bytes")
                .arg(artifact.summary.default_local_chunk_bytes.to_string())
                .arg("--freeze-chunk-search-objective")
                .arg(&artifact.summary.eval_chunk_search_objective)
                .arg("--freeze-chunk-raw-slack")
                .arg(artifact.summary.eval_chunk_raw_slack.to_string());
        }
        SURFACE_SPLIT => {
            cmd.arg("--split-chunk-sweep")
                .arg(artifact.summary.default_local_chunk_bytes.to_string())
                .arg("--split-chunk-search-objective")
                .arg(&artifact.summary.eval_chunk_search_objective)
                .arg("--split-chunk-raw-slack")
                .arg(artifact.summary.eval_chunk_raw_slack.to_string());
        }
        SURFACE_BRIDGE => {
            cmd.arg("--bridge-chunk-sweep")
                .arg(artifact.summary.default_local_chunk_bytes.to_string())
                .arg("--bridge-chunk-search-objective")
                .arg(&artifact.summary.eval_chunk_search_objective)
                .arg("--bridge-chunk-raw-slack")
                .arg(artifact.summary.eval_chunk_raw_slack.to_string());
        }
        other => panic!("unsupported surface subcommand {}", other),
    }

    if artifact.config.allow_overlap_scout {
        cmd.arg("--allow-overlap-scout");
    }
}
