use anyhow::{anyhow, bail, Context, Result};
use clap::{Parser, Subcommand};
use crc32fast::Hasher;
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use tempfile::TempDir;

const ARTIFACT_MAGIC: &[u8; 4] = b"AKLP";
const ARTIFACT_VERSION: u8 = 1;

#[derive(Parser, Debug)]
#[command(name = "apex_law_program")]
#[command(about = "Build, inspect, and replay deterministic K8DNZ law-program artifacts")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    Build(BuildArgs),
    Inspect(InspectArgs),
    Replay(ReplayArgs),
}

#[derive(Parser, Debug, Clone)]
struct BuildArgs {
    #[arg(long, default_value = "configs/tuned_validated.k8r")]
    recipe: String,
    #[arg(long = "in", required = true, num_args = 1..)]
    inputs: Vec<String>,
    #[arg(long, default_value_t = 20_000_000)]
    max_ticks: u64,
    #[arg(long, default_value_t = 256)]
    window_bytes: usize,
    #[arg(long, default_value_t = 256)]
    step_bytes: usize,
    #[arg(long, default_value_t = 12)]
    max_windows: usize,
    #[arg(long, default_value_t = 0)]
    seed_from: u64,
    #[arg(long, default_value_t = 64)]
    seed_count: u64,
    #[arg(long, default_value_t = 1)]
    seed_step: u64,
    #[arg(long, default_value_t = 1)]
    recipe_seed: u64,
    #[arg(long, default_value = "32,64")]
    chunk_sweep: String,
    #[arg(long, default_value = "raw")]
    chunk_search_objective: String,
    #[arg(long, default_value_t = 1)]
    chunk_raw_slack: u64,
    #[arg(long, default_value_t = 0)]
    map_max_depth: u8,
    #[arg(long, default_value_t = 1)]
    map_depth_shift: u8,
    #[arg(long, default_value = "8,12")]
    boundary_band_sweep: String,
    #[arg(long, default_value_t = 1)]
    boundary_delta: usize,
    #[arg(long, default_value = "4,8")]
    field_margin_sweep: String,
    #[arg(long, default_value_t = 96)]
    newline_margin_add: u64,
    #[arg(long, default_value_t = 64)]
    space_to_newline_margin_add: u64,
    #[arg(long, default_value_t = 550_000)]
    newline_share_ppm_min: u32,
    #[arg(long, default_value_t = 0)]
    newline_override_budget: usize,
    #[arg(long, default_value = "0,4")]
    newline_demote_margin_sweep: String,
    #[arg(long, default_value_t = 150_000)]
    newline_demote_keep_ppm_min: u32,
    #[arg(long, default_value_t = 1)]
    newline_demote_keep_min: usize,
    #[arg(long, action = clap::ArgAction::Set, default_value_t = true)]
    newline_only_from_spacelike: bool,
    #[arg(long, default_value_t = 0)]
    merge_gap_bytes: usize,
    #[arg(long, default_value_t = false)]
    allow_overlap_scout: bool,
    #[arg(long)]
    freeze_boundary_band: Option<usize>,
    #[arg(long)]
    freeze_field_margin: Option<u64>,
    #[arg(long)]
    freeze_newline_demote_margin: Option<u64>,
    #[arg(long, default_value = "32,64,96,128")]
    local_chunk_sweep: String,
    #[arg(long)]
    local_chunk_search_objective: Option<String>,
    #[arg(long)]
    local_chunk_raw_slack: Option<u64>,
    #[arg(long)]
    default_local_chunk_bytes: Option<usize>,
    #[arg(long, default_value_t = false)]
    tune_default_body: bool,
    #[arg(long)]
    default_body_chunk_sweep: Option<String>,
    #[arg(long, default_value = "selected-total")]
    body_select_objective: String,
    #[arg(long, default_value_t = false)]
    emit_body_scoreboard: bool,
    #[arg(long, default_value_t = 1)]
    min_override_gain_exact: usize,
    #[arg(long, default_value_t = 20)]
    exact_subset_limit: usize,
    #[arg(long)]
    global_law_id: Option<String>,
    #[arg(long, default_value_t = 12)]
    top_rows: usize,
    #[arg(long)]
    out: String,
    #[arg(long)]
    out_report: Option<String>,
}

#[derive(Parser, Debug)]
struct InspectArgs {
    #[arg(long)]
    artifact: String,
}

#[derive(Parser, Debug)]
struct ReplayArgs {
    #[arg(long)]
    artifact: String,
    #[arg(long)]
    out_report: Option<String>,
    #[arg(long, default_value_t = false)]
    compare_surfaces: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ReplayConfig {
    recipe: String,
    inputs: Vec<String>,
    max_ticks: u64,
    window_bytes: usize,
    step_bytes: usize,
    max_windows: usize,
    seed_from: u64,
    seed_count: u64,
    seed_step: u64,
    recipe_seed: u64,
    chunk_sweep: String,
    chunk_search_objective: String,
    chunk_raw_slack: u64,
    map_max_depth: u8,
    map_depth_shift: u8,
    boundary_band_sweep: String,
    boundary_delta: usize,
    field_margin_sweep: String,
    newline_margin_add: u64,
    space_to_newline_margin_add: u64,
    newline_share_ppm_min: u32,
    newline_override_budget: usize,
    newline_demote_margin_sweep: String,
    newline_demote_keep_ppm_min: u32,
    newline_demote_keep_min: usize,
    newline_only_from_spacelike: bool,
    merge_gap_bytes: usize,
    allow_overlap_scout: bool,
    freeze_boundary_band: Option<usize>,
    freeze_field_margin: Option<u64>,
    freeze_newline_demote_margin: Option<u64>,
    local_chunk_sweep: String,
    local_chunk_search_objective: Option<String>,
    local_chunk_raw_slack: Option<u64>,
    default_local_chunk_bytes_arg: Option<usize>,
    tune_default_body: bool,
    default_body_chunk_sweep: Option<String>,
    body_select_objective: String,
    emit_body_scoreboard: bool,
    min_override_gain_exact: usize,
    exact_subset_limit: usize,
    global_law_id_arg: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ProgramSummary {
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
    searched_total_piecewise_payload_exact: i64,
    projected_default_total_piecewise_payload_exact: i64,
    delta_default_total_piecewise_payload_exact: i64,
    projected_unpriced_best_mix_total_piecewise_payload_exact: i64,
    delta_unpriced_best_mix_total_piecewise_payload_exact: i64,
    selected_total_piecewise_payload_exact: i64,
    delta_selected_total_piecewise_payload_exact: i64,
    target_window_count: usize,
    searched_target_window_payload_exact: usize,
    default_target_window_payload_exact: usize,
    best_mix_target_window_payload_exact: usize,
    selected_target_window_payload_exact: usize,
    delta_selected_target_window_payload_exact: i64,
    override_path_mode: String,
    override_path_bytes_exact: usize,
    selected_override_window_count: usize,
    improved_target_window_count: usize,
    equal_target_window_count: usize,
    worsened_target_window_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ProgramFile {
    input: String,
    searched_total_piecewise_payload_exact: i64,
    projected_default_total_piecewise_payload_exact: i64,
    projected_unpriced_best_mix_total_piecewise_payload_exact: i64,
    selected_total_piecewise_payload_exact: i64,
    target_window_count: usize,
    override_path_mode: String,
    override_path_bytes_exact: usize,
    selected_override_window_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ProgramWindow {
    input_index: usize,
    input: String,
    window_idx: usize,
    target_ordinal: usize,
    start: usize,
    end: usize,
    span_bytes: usize,
    searched_payload_exact: usize,
    default_payload_exact: usize,
    best_payload_exact: usize,
    selected_payload_exact: usize,
    searched_chunk_bytes: usize,
    best_chunk_bytes: usize,
    selected_chunk_bytes: usize,
    selected_gain_exact: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ProgramOverride {
    input_index: usize,
    input: String,
    window_idx: usize,
    target_ordinal: usize,
    best_chunk_bytes: usize,
    default_payload_exact: usize,
    best_payload_exact: usize,
    gain_exact: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LawProgramArtifact {
    config: ReplayConfig,
    summary: ProgramSummary,
    files: Vec<ProgramFile>,
    windows: Vec<ProgramWindow>,
    overrides: Vec<ProgramOverride>,
}

#[derive(Clone, Debug)]
struct ReplayEvalRow {
    input_index: usize,
    input: String,
    window_idx: usize,
    target_ordinal: usize,
    start: usize,
    end: usize,
    selected_chunk_bytes: usize,
    searched_payload_exact: usize,
    artifact_selected_payload_exact: usize,
    replay_payload_exact: usize,
    delta_vs_artifact_exact: i64,
    delta_vs_searched_exact: i64,
    field_match_pct: f64,
    collapse_90_flag: bool,
    newline_extinct_flag: bool,
    newline_floor_used: usize,
}

#[derive(Clone, Debug)]
struct ReplayFileSummary {
    input: String,
    searched_total_piecewise_payload_exact: i64,
    artifact_selected_total_piecewise_payload_exact: i64,
    replay_selected_total_piecewise_payload_exact: i64,
    searched_target_window_payload_exact: usize,
    artifact_selected_target_window_payload_exact: usize,
    replay_target_window_payload_exact: usize,
    override_path_bytes_exact: usize,
    target_window_count: usize,
    drift_exact: i64,
    improved_vs_searched_count: usize,
    equal_vs_searched_count: usize,
    worsened_vs_searched_count: usize,
}

#[derive(Clone, Debug)]
struct SurfaceScoreboard {
    searched_total_piecewise_payload_exact: i64,
    artifact_selected_total_piecewise_payload_exact: i64,
    replay_selected_total_piecewise_payload_exact: i64,
    frozen_total_piecewise_payload_exact: Option<i64>,
    split_total_piecewise_payload_exact: Option<i64>,
    bridge_total_piecewise_payload_exact: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ParsedCsvSections {
    summary_rows: Vec<BTreeMap<String, String>>,
    file_rows: Vec<BTreeMap<String, String>>,
    window_rows: Vec<BTreeMap<String, String>>,
    override_selected_rows: Vec<BTreeMap<String, String>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ManifestWindowPos {
    start: usize,
    end: usize,
    span_bytes: usize,
}

#[derive(Clone, Debug)]
struct FrozenEvalRow {
    compact_field_total_payload_exact: usize,
    field_match_pct: f64,
    field_pred_collapse_90_flag: bool,
    field_newline_extinct_flag: bool,
    field_newline_floor_used: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BodyCandidateScore {
    chunk_bytes: usize,
    selected_total_piecewise_payload_exact: i64,
    selected_target_window_payload_exact: usize,
    selected_override_window_count: usize,
    override_path_bytes_exact: usize,
    projected_default_total_piecewise_payload_exact: i64,
    target_window_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BuildMaterialized {
    artifact: LawProgramArtifact,
    body_scores: Vec<BodyCandidateScore>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum OverridePathMode {
    None,
    Delta,
    Runs,
    Ordinals,
}

impl OverridePathMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Delta => "delta",
            Self::Runs => "runs",
            Self::Ordinals => "ordinals",
        }
    }

    fn tie_rank(self) -> u8 {
        match self {
            Self::None => 0,
            Self::Delta => 1,
            Self::Runs => 2,
            Self::Ordinals => 3,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct OverridePathPlan {
    mode: OverridePathMode,
    bytes: usize,
    ordinals: Vec<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct OverrideCandidateRef {
    window_idx: usize,
    target_ordinal: usize,
    gain_exact: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SelectedFilePlan {
    mode: OverridePathMode,
    path_bytes_exact: usize,
    selected_window_ordinals: Vec<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct OverrideSubsetScore {
    net_total_delta_exact: i64,
    path_bytes_exact: usize,
    selected_count: usize,
    mode_rank: u8,
    ordinals: Vec<usize>,
    plan: OverridePathPlan,
}

fn main() -> Result<()> {
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
            "apex-law-program build: artifact={} bytes={} windows={} overrides={} target={} override_path_mode={} override_path_bytes_exact={} selected_total_piecewise_payload_exact={}",
            args.out,
            bytes.len(),
            materialized.artifact.windows.len(),
            materialized.artifact.overrides.len(),
            materialized.artifact.summary.target_global_law_id,
            materialized.artifact.summary.override_path_mode,
            materialized.artifact.summary.override_path_bytes_exact,
            materialized.artifact.summary.selected_total_piecewise_payload_exact,
        );
    } else {
        print!("{}", report);
    }

    Ok(())
}

fn materialize_build(cli_exe: &Path, args: &BuildArgs) -> Result<BuildMaterialized> {
    let body_candidates = select_body_candidates(args)?;
    let mut materials = Vec::<BuildMaterialized>::new();
    for chunk_bytes in &body_candidates {
        let mut trial_args = args.clone();
        trial_args.default_local_chunk_bytes = Some(*chunk_bytes);
        let txt_out = run_local_mix(cli_exe, &trial_args, "txt")?;
        let csv_out = run_local_mix(cli_exe, &trial_args, "csv")?;
        let txt_summary = parse_txt_summary(&txt_out.stdout)?;
        let csv_sections = parse_csv_sections(&csv_out.stdout)?;
        let mut manifest_positions = BTreeMap::<(String, usize), ManifestWindowPos>::new();
        for input in &trial_args.inputs {
            let manifest_out = run_manifest_txt(cli_exe, &trial_args, input)?;
            let positions = parse_manifest_positions(&manifest_out.stdout)?;
            for (window_idx, pos) in positions {
                manifest_positions.insert((input.clone(), window_idx), pos);
            }
        }
        let artifact = build_artifact_from_outputs(&trial_args, &txt_summary, &csv_sections, &manifest_positions)?;
        materials.push(BuildMaterialized {
            body_scores: vec![BodyCandidateScore {
                chunk_bytes: artifact.summary.default_local_chunk_bytes,
                selected_total_piecewise_payload_exact: artifact.summary.selected_total_piecewise_payload_exact,
                selected_target_window_payload_exact: artifact.summary.selected_target_window_payload_exact,
                selected_override_window_count: artifact.summary.selected_override_window_count,
                override_path_bytes_exact: artifact.summary.override_path_bytes_exact,
                projected_default_total_piecewise_payload_exact: artifact.summary.projected_default_total_piecewise_payload_exact,
                target_window_count: artifact.summary.target_window_count,
            }],
            artifact,
        });
    }
    let best_idx = select_best_materialized_index(&materials, &args.body_select_objective)?;
    let mut best = materials.remove(best_idx);
    let mut body_scores = materials
        .into_iter()
        .map(|m| BodyCandidateScore {
            chunk_bytes: m.artifact.summary.default_local_chunk_bytes,
            selected_total_piecewise_payload_exact: m.artifact.summary.selected_total_piecewise_payload_exact,
            selected_target_window_payload_exact: m.artifact.summary.selected_target_window_payload_exact,
            selected_override_window_count: m.artifact.summary.selected_override_window_count,
            override_path_bytes_exact: m.artifact.summary.override_path_bytes_exact,
            projected_default_total_piecewise_payload_exact: m.artifact.summary.projected_default_total_piecewise_payload_exact,
            target_window_count: m.artifact.summary.target_window_count,
        })
        .collect::<Vec<_>>();
    body_scores.push(best.body_scores[0].clone());
    body_scores.sort_by_key(|row| row.chunk_bytes);
    best.body_scores = body_scores;
    Ok(best)
}

fn select_body_candidates(args: &BuildArgs) -> Result<Vec<usize>> {
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

fn select_best_materialized_index(materials: &[BuildMaterialized], objective: &str) -> Result<usize> {
    if materials.is_empty() {
        bail!("cannot select best body from empty materialized set");
    }
    let best = materials
        .iter()
        .enumerate()
        .min_by(|(_, a), (_, b)| compare_materialized(a, b, objective))
        .map(|(idx, _)| idx)
        .ok_or_else(|| anyhow!("failed to select best body"))?;
    Ok(best)
}

fn compare_materialized(a: &BuildMaterialized, b: &BuildMaterialized, objective: &str) -> std::cmp::Ordering {
    let asu = &a.artifact.summary;
    let bsu = &b.artifact.summary;
    match objective {
        "default-total" => (
            asu.projected_default_total_piecewise_payload_exact,
            asu.override_path_bytes_exact,
            asu.selected_override_window_count,
            asu.default_local_chunk_bytes,
        )
            .cmp(&(
                bsu.projected_default_total_piecewise_payload_exact,
                bsu.override_path_bytes_exact,
                bsu.selected_override_window_count,
                bsu.default_local_chunk_bytes,
            )),
        "selected-target" => (
            asu.selected_target_window_payload_exact,
            asu.override_path_bytes_exact,
            asu.selected_override_window_count,
            asu.default_local_chunk_bytes,
        )
            .cmp(&(
                bsu.selected_target_window_payload_exact,
                bsu.override_path_bytes_exact,
                bsu.selected_override_window_count,
                bsu.default_local_chunk_bytes,
            )),
        _ => (
            asu.selected_total_piecewise_payload_exact,
            asu.override_path_bytes_exact,
            asu.selected_override_window_count,
            asu.default_local_chunk_bytes,
        )
            .cmp(&(
                bsu.selected_total_piecewise_payload_exact,
                bsu.override_path_bytes_exact,
                bsu.selected_override_window_count,
                bsu.default_local_chunk_bytes,
            )),
    }
}

fn run_inspect(args: InspectArgs) -> Result<()> {
    let bytes = fs::read(&args.artifact).with_context(|| format!("read artifact {}", args.artifact))?;
    let artifact = LawProgramArtifact::decode(&bytes)?;
    let report = render_artifact_report(&artifact, &bytes, &args.artifact, None);
    print!("{}", report);
    Ok(())
}

fn run_replay(args: ReplayArgs) -> Result<()> {
    let bytes = fs::read(&args.artifact).with_context(|| format!("read artifact {}", args.artifact))?;
    let artifact = LawProgramArtifact::decode(&bytes)?;
    let cli_exe = resolve_k8dnz_cli_exe()?;
    let temp_dir = tempfile::tempdir().context("create replay temp dir")?;
    let (rows, file_summaries) = replay_artifact(&cli_exe, &artifact, &temp_dir)?;

    let replay_selected_total_piecewise_payload_exact = file_summaries
        .iter()
        .map(|row| row.replay_selected_total_piecewise_payload_exact)
        .sum::<i64>();
    let collapse_90_failures = rows.iter().filter(|row| row.collapse_90_flag).count();
    let newline_extinct_failures = rows.iter().filter(|row| row.newline_extinct_flag).count();
    let drift_exact = replay_selected_total_piecewise_payload_exact
        - artifact.summary.selected_total_piecewise_payload_exact;

    let scoreboard = if args.compare_surfaces {
        Some(run_surface_scoreboard(
            &cli_exe,
            &artifact,
            replay_selected_total_piecewise_payload_exact,
        )?)
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
            "apex-law-program replay: artifact={} replay_selected_total_piecewise_payload_exact={} artifact_selected_total_piecewise_payload_exact={} drift_exact={} collapse_90_failures={} newline_extinct_failures={}",
            args.artifact,
            replay_selected_total_piecewise_payload_exact,
            artifact.summary.selected_total_piecewise_payload_exact,
            drift_exact,
            collapse_90_failures,
            newline_extinct_failures,
        );
    } else {
        print!("{}", report);
    }

    Ok(())
}

fn resolve_k8dnz_cli_exe() -> Result<PathBuf> {
    let exe = env::current_exe().context("resolve current executable")?;
    let parent = exe
        .parent()
        .ok_or_else(|| anyhow!("current executable has no parent: {}", exe.display()))?;
    let bin_name = if cfg!(windows) { "k8dnz-cli.exe" } else { "k8dnz-cli" };
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

fn run_local_mix(cli_exe: &Path, args: &BuildArgs, format: &str) -> Result<Output> {
    let mut cmd = Command::new(cli_exe);
    cmd.arg("apextrace")
        .arg("apex-lane-law-local-mix-freeze")
        .arg("--recipe")
        .arg(&args.recipe);
    for input in &args.inputs {
        cmd.arg("--in").arg(input);
    }
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
        .arg(format!("--newline-only-from-spacelike={}", args.newline_only_from_spacelike))
        .arg("--merge-gap-bytes")
        .arg(args.merge_gap_bytes.to_string())
        .arg("--local-chunk-sweep")
        .arg(&args.local_chunk_sweep)
        .arg("--min-override-gain-exact")
        .arg(args.min_override_gain_exact.to_string())
        .arg("--exact-subset-limit")
        .arg(args.exact_subset_limit.to_string())
        .arg("--top-rows")
        .arg(args.top_rows.to_string())
        .arg("--format")
        .arg(format);

    if args.allow_overlap_scout {
        cmd.arg("--allow-overlap-scout");
    }
    if let Some(v) = args.freeze_boundary_band {
        cmd.arg("--freeze-boundary-band").arg(v.to_string());
    }
    if let Some(v) = args.freeze_field_margin {
        cmd.arg("--freeze-field-margin").arg(v.to_string());
    }
    if let Some(v) = args.freeze_newline_demote_margin {
        cmd.arg("--freeze-newline-demote-margin").arg(v.to_string());
    }
    if let Some(v) = args.local_chunk_search_objective.as_deref() {
        cmd.arg("--local-chunk-search-objective").arg(v);
    }
    if let Some(v) = args.local_chunk_raw_slack {
        cmd.arg("--local-chunk-raw-slack").arg(v.to_string());
    }
    if let Some(v) = args.default_local_chunk_bytes {
        cmd.arg("--default-local-chunk-bytes").arg(v.to_string());
    }
    if let Some(v) = args.global_law_id.as_deref() {
        cmd.arg("--global-law-id").arg(v);
    }

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


fn build_artifact_from_outputs(
    args: &BuildArgs,
    txt_summary: &BTreeMap<String, String>,
    csv_sections: &ParsedCsvSections,
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
    let mut overrides = Vec::<ProgramOverride>::new();

    for file in &mut files {
        let window_indexes = windows_by_input
            .get(&file.input)
            .cloned()
            .unwrap_or_default();

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

        let selected_plan = select_override_plan(&candidates, args.exact_subset_limit);
        let selected_ordinals = selected_plan
            .selected_window_ordinals
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();

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

        file.projected_default_total_piecewise_payload_exact = base_default_total;
        file.projected_unpriced_best_mix_total_piecewise_payload_exact = base_best_mix_total;
        file.selected_total_piecewise_payload_exact = file_selected_total;
        file.target_window_count = window_indexes.len();
        file.override_path_mode = selected_plan.mode.as_str().to_string();
        file.override_path_bytes_exact = selected_plan.path_bytes_exact;
        file.selected_override_window_count = selected_plan.selected_window_ordinals.len();

        projected_default_total_piecewise_payload_exact += base_default_total;
        projected_unpriced_best_mix_total_piecewise_payload_exact += base_best_mix_total;
        selected_total_piecewise_payload_exact += file_selected_total;
        searched_target_window_payload_exact += searched_target;
        default_target_window_payload_exact += default_target;
        best_mix_target_window_payload_exact += best_target;
        selected_target_window_payload_exact += file_selected_target;
        override_path_bytes_exact += selected_plan.path_bytes_exact;
        selected_override_window_count += selected_plan.selected_window_ordinals.len();
    }

    overrides.sort_by_key(|row| (row.input_index, row.target_ordinal, row.window_idx));

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

fn select_override_plan(
    candidates: &[OverrideCandidateRef],
    exact_subset_limit: usize,
) -> SelectedFilePlan {
    let filtered = candidates
        .iter()
        .copied()
        .filter(|row| row.gain_exact > 0)
        .collect::<Vec<_>>();
    if filtered.is_empty() {
        return SelectedFilePlan {
            mode: OverridePathMode::None,
            path_bytes_exact: 0,
            selected_window_ordinals: Vec::new(),
        };
    }
    if filtered.len() <= exact_subset_limit && filtered.len() < 63 {
        return select_override_plan_exact(&filtered);
    }
    select_override_plan_greedy(&filtered)
}

fn select_override_plan_exact(candidates: &[OverrideCandidateRef]) -> SelectedFilePlan {
    let mut best = score_override_subset(&[]);
    let subset_count = 1u64 << candidates.len();
    for mask in 1..subset_count {
        let mut subset = Vec::new();
        for (bit, row) in candidates.iter().enumerate() {
            if (mask & (1u64 << bit)) != 0 {
                subset.push(*row);
            }
        }
        let score = score_override_subset(&subset);
        if is_better_override_score(&score, &best) {
            best = score;
        }
    }
    SelectedFilePlan {
        mode: best.plan.mode,
        path_bytes_exact: best.plan.bytes,
        selected_window_ordinals: best.plan.ordinals,
    }
}


fn select_override_plan_greedy(candidates: &[OverrideCandidateRef]) -> SelectedFilePlan {
    let mut remaining = candidates.to_vec();
    remaining.sort_by_key(|row| row.target_ordinal);
    let mut selected = Vec::<OverrideCandidateRef>::new();
    let mut best = score_override_subset(&selected);

    loop {
        let mut best_next = best.clone();
        let mut best_idx = None;
        for (idx, candidate) in remaining.iter().enumerate() {
            let mut trial = selected.clone();
            trial.push(*candidate);
            let score = score_override_subset(&trial);
            if is_better_override_score(&score, &best_next) {
                best_next = score;
                best_idx = Some(idx);
            }
        }
        match best_idx {
            Some(idx) => {
                selected.push(remaining.remove(idx));
                best = best_next;
            }
            None => break,
        }
    }

    SelectedFilePlan {
        mode: best.plan.mode,
        path_bytes_exact: best.plan.bytes,
        selected_window_ordinals: best.plan.ordinals,
    }
}

fn score_override_subset(subset: &[OverrideCandidateRef]) -> OverrideSubsetScore {
    let total_gain_exact = subset.iter().map(|row| row.gain_exact as i64).sum::<i64>();
    let ordinals = subset
        .iter()
        .map(|row| row.target_ordinal)
        .collect::<Vec<_>>();
    let plan = choose_best_override_path_plan(&ordinals);
    OverrideSubsetScore {
        net_total_delta_exact: plan.bytes as i64 - total_gain_exact,
        path_bytes_exact: plan.bytes,
        selected_count: plan.ordinals.len(),
        mode_rank: plan.mode.tie_rank(),
        ordinals: plan.ordinals.clone(),
        plan,
    }
}

fn is_better_override_score(a: &OverrideSubsetScore, b: &OverrideSubsetScore) -> bool {
    (
        a.net_total_delta_exact,
        a.path_bytes_exact,
        a.selected_count,
        a.mode_rank,
        a.ordinals.clone(),
    ) < (
        b.net_total_delta_exact,
        b.path_bytes_exact,
        b.selected_count,
        b.mode_rank,
        b.ordinals.clone(),
    )
}

fn choose_best_override_path_plan(ordinals: &[usize]) -> OverridePathPlan {
    if ordinals.is_empty() {
        return OverridePathPlan {
            mode: OverridePathMode::None,
            bytes: 0,
            ordinals: Vec::new(),
        };
    }

    let mut normalized = ordinals.to_vec();
    normalized.sort_unstable();
    normalized.dedup();

    vec![
        OverridePathPlan {
            mode: OverridePathMode::Delta,
            bytes: override_path_bytes_delta(&normalized),
            ordinals: normalized.clone(),
        },
        OverridePathPlan {
            mode: OverridePathMode::Runs,
            bytes: override_path_bytes_runs(&normalized),
            ordinals: normalized.clone(),
        },
        OverridePathPlan {
            mode: OverridePathMode::Ordinals,
            bytes: override_path_bytes_ordinals(&normalized),
            ordinals: normalized.clone(),
        },
    ]
    .into_iter()
    .min_by_key(|plan| (plan.bytes, plan.mode.tie_rank(), plan.ordinals.clone()))
    .expect("override path plans should not be empty")
}

fn override_path_bytes_ordinals(ordinals: &[usize]) -> usize {
    1 + varint_len(ordinals.len() as u64)
        + ordinals
            .iter()
            .map(|ordinal| varint_len(*ordinal as u64))
            .sum::<usize>()
}

fn override_path_bytes_delta(ordinals: &[usize]) -> usize {
    let mut bytes = 1 + varint_len(ordinals.len() as u64);
    let mut prev = 0usize;
    for (idx, ordinal) in ordinals.iter().enumerate() {
        let delta = if idx == 0 {
            *ordinal
        } else {
            ordinal.saturating_sub(prev)
        };
        bytes += varint_len(delta as u64);
        prev = *ordinal;
    }
    bytes
}

fn override_path_bytes_runs(ordinals: &[usize]) -> usize {
    let runs = ordinal_runs(ordinals);
    1 + varint_len(runs.len() as u64)
        + runs
            .iter()
            .map(|(start, len)| varint_len(*start as u64) + varint_len(*len as u64))
            .sum::<usize>()
}

fn ordinal_runs(ordinals: &[usize]) -> Vec<(usize, usize)> {
    if ordinals.is_empty() {
        return Vec::new();
    }
    let mut runs = Vec::new();
    let mut start = ordinals[0];
    let mut prev = ordinals[0];
    let mut len = 1usize;
    for ordinal in ordinals.iter().copied().skip(1) {
        if ordinal == prev + 1 {
            len += 1;
        } else {
            runs.push((start, len));
            start = ordinal;
            len = 1;
        }
        prev = ordinal;
    }
    runs.push((start, len));
    runs
}

fn varint_len(mut value: u64) -> usize {
    let mut bytes = 1usize;
    while value >= 0x80 {
        value >>= 7;
        bytes += 1;
    }
    bytes
}

fn replay_artifact(


    cli_exe: &Path,
    artifact: &LawProgramArtifact,
    temp_dir: &TempDir,
) -> Result<(Vec<ReplayEvalRow>, Vec<ReplayFileSummary>)> {
    let mut rows = Vec::with_capacity(artifact.windows.len());
    let mut by_file = BTreeMap::<usize, Vec<&ProgramWindow>>::new();
    for window in &artifact.windows {
        by_file.entry(window.input_index).or_default().push(window);
    }

    for window in &artifact.windows {
        let input_bytes = fs::read(&artifact.config.inputs[window.input_index]).with_context(|| {
            format!(
                "read replay input {}",
                artifact.config.inputs[window.input_index]
            )
        })?;
        if window.end > input_bytes.len() || window.start > window.end {
            bail!(
                "artifact window out of range input={} start={} end={} len={}",
                artifact.config.inputs[window.input_index],
                window.start,
                window.end,
                input_bytes.len()
            );
        }
        let slice = &input_bytes[window.start..window.end];
        let slice_path = temp_dir.path().join(format!(
            "window_{:02}_{:04}_{:08}_{:08}.bin",
            window.input_index, window.window_idx, window.start, window.end
        ));
        fs::write(&slice_path, slice)
            .with_context(|| format!("write replay slice {}", slice_path.display()))?;
        let eval = eval_window_fixed(cli_exe, artifact, &slice_path, window.selected_chunk_bytes)?;
        rows.push(ReplayEvalRow {
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
        });
    }

    rows.sort_by_key(|row| (row.input_index, row.target_ordinal, row.window_idx));

    let mut file_summaries = Vec::new();
    for (input_index, windows) in by_file {
        let file = artifact
            .files
            .iter()
            .find(|row| row.input == artifact.config.inputs[input_index])
            .ok_or_else(|| anyhow!("missing file summary for {}", artifact.config.inputs[input_index]))?;
        let file_rows = rows
            .iter()
            .filter(|row| row.input_index == input_index)
            .collect::<Vec<_>>();
        let searched_target_window_payload_exact = windows
            .iter()
            .map(|row| row.searched_payload_exact)
            .sum::<usize>();
        let artifact_selected_target_window_payload_exact = windows
            .iter()
            .map(|row| row.selected_payload_exact)
            .sum::<usize>();
        let replay_target_window_payload_exact = file_rows
            .iter()
            .map(|row| row.replay_payload_exact)
            .sum::<usize>();
        let replay_selected_total_piecewise_payload_exact = file
            .searched_total_piecewise_payload_exact
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
    Ok((rows, file_summaries))
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

fn run_surface_scoreboard(
    cli_exe: &Path,
    artifact: &LawProgramArtifact,
    replay_selected_total_piecewise_payload_exact: i64,
) -> Result<SurfaceScoreboard> {
    let frozen_total = run_surface_total(cli_exe, artifact, "apex-lane-law-freeze")?;
    let split_total = run_surface_total(cli_exe, artifact, "apex-lane-law-split-freeze")?;
    let bridge_total = run_surface_total(cli_exe, artifact, "apex-lane-law-bridge-freeze")?;

    Ok(SurfaceScoreboard {
        searched_total_piecewise_payload_exact: artifact.summary.searched_total_piecewise_payload_exact,
        artifact_selected_total_piecewise_payload_exact: artifact.summary.selected_total_piecewise_payload_exact,
        replay_selected_total_piecewise_payload_exact,
        frozen_total_piecewise_payload_exact: Some(frozen_total),
        split_total_piecewise_payload_exact: Some(split_total),
        bridge_total_piecewise_payload_exact: Some(bridge_total),
    })
}

fn run_surface_total(cli_exe: &Path, artifact: &LawProgramArtifact, subcmd: &str) -> Result<i64> {
    let mut cmd = Command::new(cli_exe);
    cmd.arg("apextrace").arg(subcmd).arg("--recipe").arg(&artifact.config.recipe);
    for input in &artifact.config.inputs {
        cmd.arg("--in").arg(input);
    }
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
            artifact.config.newline_only_from_spacelike
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
        "apex-lane-law-freeze" => {
            cmd.arg("--freeze-chunk-bytes")
                .arg(artifact.summary.default_local_chunk_bytes.to_string())
                .arg("--freeze-chunk-search-objective")
                .arg(&artifact.summary.eval_chunk_search_objective)
                .arg("--freeze-chunk-raw-slack")
                .arg(artifact.summary.eval_chunk_raw_slack.to_string());
        }
        "apex-lane-law-split-freeze" => {
            cmd.arg("--split-chunk-sweep")
                .arg(artifact.summary.default_local_chunk_bytes.to_string())
                .arg("--split-chunk-search-objective")
                .arg(&artifact.summary.eval_chunk_search_objective)
                .arg("--split-chunk-raw-slack")
                .arg(artifact.summary.eval_chunk_raw_slack.to_string());
        }
        "apex-lane-law-bridge-freeze" => {
            cmd.arg("--bridge-chunk-sweep")
                .arg(artifact.summary.default_local_chunk_bytes.to_string())
                .arg("--bridge-chunk-search-objective")
                .arg(&artifact.summary.eval_chunk_search_objective)
                .arg("--bridge-chunk-raw-slack")
                .arg(artifact.summary.eval_chunk_raw_slack.to_string());
        }
        other => bail!("unsupported surface subcommand {}", other),
    }

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


fn run_manifest_txt(cli_exe: &Path, args: &BuildArgs, input: &str) -> Result<Output> {
    let mut cmd = Command::new(cli_exe);
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
        .arg(format!("--newline-only-from-spacelike={}", args.newline_only_from_spacelike))
        .arg("--merge-gap-bytes")
        .arg(args.merge_gap_bytes.to_string())
        .arg("--format")
        .arg("txt");
    if args.allow_overlap_scout {
        cmd.arg("--allow-overlap-scout");
    }
    let output = cmd.output().with_context(|| format!("run manifest child {}", cli_exe.display()))?;
    if !output.status.success() {
        bail!(
            "manifest child failed status={}
stdout:
{}
stderr:
{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(output)
}

fn parse_manifest_positions(stdout: &[u8]) -> Result<BTreeMap<usize, ManifestWindowPos>> {
    let body = String::from_utf8_lossy(stdout);
    let mut out = BTreeMap::new();
    for line in body.lines() {
        let line = line.trim();
        if !line.starts_with("window_idx=") {
            continue;
        }
        let map = tokenize_kv_line(line);
        let window_idx = parse_required_usize(&map, "window_idx")?;
        let pos = ManifestWindowPos {
            start: parse_required_usize(&map, "start")?,
            end: parse_required_usize(&map, "end")?,
            span_bytes: parse_required_usize(&map, "span_bytes")?,
        };
        out.insert(window_idx, pos);
    }
    Ok(out)
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
    Ok(FrozenEvalRow {
        compact_field_total_payload_exact: parse_required_usize(
            &map,
            "compact_field_total_payload_exact",
        )?,
        field_match_pct: parse_required_f64(&map, "field_match_pct")?,
        field_pred_collapse_90_flag: parse_required_bool(&map, "field_pred_collapse_90_flag")?,
        field_newline_extinct_flag: parse_required_bool(&map, "field_newline_extinct_flag")?,
        field_newline_floor_used: parse_required_usize(&map, "field_newline_floor_used")?,
    })
}

fn parse_txt_summary(stdout: &[u8]) -> Result<BTreeMap<String, String>> {
    let mut map = BTreeMap::new();
    let body = String::from_utf8_lossy(stdout);
    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if line.starts_with("---") {
            break;
        }
        if let Some((k, v)) = line.split_once('=') {
            map.insert(k.trim().to_string(), v.trim().to_string());
        }
    }
    Ok(map)
}

fn parse_csv_sections(stdout: &[u8]) -> Result<ParsedCsvSections> {
    let body = String::from_utf8_lossy(stdout);
    let mut header: Vec<String> = Vec::new();
    let mut sections = ParsedCsvSections {
        summary_rows: Vec::new(),
        file_rows: Vec::new(),
        window_rows: Vec::new(),
        override_selected_rows: Vec::new(),
    };

    for raw_line in body.lines() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        let cells = parse_csv_line(line)?;
        if cells.is_empty() {
            continue;
        }
        if cells[0] == "section" {
            header = cells;
            continue;
        }
        if header.is_empty() {
            bail!("csv row seen before header: {}", line);
        }
        if header.len() != cells.len() {
            bail!(
                "csv row/header length mismatch header_len={} row_len={} line={}",
                header.len(),
                cells.len(),
                line
            );
        }
        let row = header
            .iter()
            .cloned()
            .zip(cells.into_iter())
            .collect::<BTreeMap<_, _>>();
        match row.get("section").map(String::as_str) {
            Some("summary") => sections.summary_rows.push(row),
            Some("file") => sections.file_rows.push(row),
            Some("window") => sections.window_rows.push(row),
            Some("override_selected") => sections.override_selected_rows.push(row),
            _ => {}
        }
    }

    Ok(sections)
}

fn parse_csv_line(line: &str) -> Result<Vec<String>> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut chars = line.chars().peekable();
    let mut in_quotes = false;
    while let Some(ch) = chars.next() {
        match ch {
            '"' => {
                if in_quotes {
                    if matches!(chars.peek(), Some('"')) {
                        cur.push('"');
                        chars.next();
                    } else {
                        in_quotes = false;
                    }
                } else if cur.is_empty() {
                    in_quotes = true;
                } else {
                    cur.push(ch);
                }
            }
            ',' if !in_quotes => {
                out.push(cur);
                cur = String::new();
            }
            _ => cur.push(ch),
        }
    }
    if in_quotes {
        bail!("unterminated quoted csv line: {}", line);
    }
    out.push(cur);
    Ok(out)
}

fn parse_csv_usize_list(source: &str) -> Result<Vec<usize>> {
    let mut out = Vec::new();
    for raw in source.split(',') {
        let token = raw.trim();
        if token.is_empty() {
            continue;
        }
        let value = token
            .parse::<usize>()
            .with_context(|| format!("parse usize list element from {}", token))?;
        out.push(value);
    }
    if out.is_empty() {
        bail!("usize csv list resolved to empty set from {}", source);
    }
    Ok(out)
}

fn tokenize_kv_line(payload: &str) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    for token in payload.split_whitespace() {
        if let Some((k, v)) = token.split_once('=') {
            map.insert(k.to_string(), v.to_string());
        }
    }
    map
}


fn render_artifact_report(
    artifact: &LawProgramArtifact,
    bytes: &[u8],
    path: &str,
    body_scores: Option<&[BodyCandidateScore]>,
) -> String {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    let crc32 = hasher.finalize();
    let mut out = String::new();
    out.push_str(&format!("artifact_path={}
", path));
    out.push_str(&format!("artifact_bytes={}
", bytes.len()));
    out.push_str(&format!("artifact_crc32=0x{:08X}
", crc32));
    out.push_str(&format!("recipe={}
", artifact.summary.recipe));
    out.push_str(&format!("file_count={}
", artifact.summary.file_count));
    out.push_str(&format!("union_law_count={}
", artifact.summary.union_law_count));
    out.push_str(&format!(
        "target_global_law_id={}
",
        artifact.summary.target_global_law_id
    ));
    out.push_str(&format!(
        "default_local_chunk_bytes={}
",
        artifact.summary.default_local_chunk_bytes
    ));
    out.push_str(&format!(
        "searched_total_piecewise_payload_exact={}
",
        artifact.summary.searched_total_piecewise_payload_exact
    ));
    out.push_str(&format!(
        "projected_default_total_piecewise_payload_exact={}
",
        artifact.summary.projected_default_total_piecewise_payload_exact
    ));
    out.push_str(&format!(
        "selected_total_piecewise_payload_exact={}
",
        artifact.summary.selected_total_piecewise_payload_exact
    ));
    out.push_str(&format!(
        "override_path_mode={}
",
        artifact.summary.override_path_mode
    ));
    out.push_str(&format!(
        "override_path_bytes_exact={}
",
        artifact.summary.override_path_bytes_exact
    ));
    out.push_str(&format!("window_count={}
", artifact.windows.len()));
    out.push_str(&format!("override_count={}
", artifact.overrides.len()));

    if let Some(body_scores) = body_scores {
        out.push_str("
--- body-scoreboard ---
");
        for row in body_scores {
            out.push_str(&format!(
                "chunk_bytes={} selected_total_piecewise_payload_exact={} selected_target_window_payload_exact={} selected_override_window_count={} override_path_bytes_exact={} projected_default_total_piecewise_payload_exact={} target_window_count={}
",
                row.chunk_bytes,
                row.selected_total_piecewise_payload_exact,
                row.selected_target_window_payload_exact,
                row.selected_override_window_count,
                row.override_path_bytes_exact,
                row.projected_default_total_piecewise_payload_exact,
                row.target_window_count,
            ));
        }
    }

    out.push_str("
--- files ---
");
    for file in &artifact.files {
        out.push_str(&format!(
            "input={} searched_total_piecewise_payload_exact={} projected_default_total_piecewise_payload_exact={} selected_total_piecewise_payload_exact={} target_window_count={} override_path_mode={} override_path_bytes_exact={} selected_override_window_count={}
",
            file.input,
            file.searched_total_piecewise_payload_exact,
            file.projected_default_total_piecewise_payload_exact,
            file.selected_total_piecewise_payload_exact,
            file.target_window_count,
            file.override_path_mode,
            file.override_path_bytes_exact,
            file.selected_override_window_count,
        ));
    }
    out.push_str("
--- selected-overrides ---
");
    for row in &artifact.overrides {
        out.push_str(&format!(
            "input={} window_idx={} target_ordinal={} best_chunk_bytes={} default_payload_exact={} best_payload_exact={} gain_exact={}
",
            row.input,
            row.window_idx,
            row.target_ordinal,
            row.best_chunk_bytes,
            row.default_payload_exact,
            row.best_payload_exact,
            row.gain_exact,
        ));
    }
    out
}


fn render_replay_report(
    artifact_path: &str,
    artifact: &LawProgramArtifact,
    rows: &[ReplayEvalRow],
    file_summaries: &[ReplayFileSummary],
    replay_selected_total_piecewise_payload_exact: i64,
    drift_exact: i64,
    collapse_90_failures: usize,
    newline_extinct_failures: usize,
    scoreboard: Option<&SurfaceScoreboard>,
) -> String {
    let mut out = String::new();
    out.push_str(&format!("artifact_path={}
", artifact_path));
    out.push_str(&format!(
        "target_global_law_id={}
",
        artifact.summary.target_global_law_id
    ));
    out.push_str(&format!(
        "artifact_selected_total_piecewise_payload_exact={}
",
        artifact.summary.selected_total_piecewise_payload_exact
    ));
    out.push_str(&format!(
        "artifact_override_path_mode={}
",
        artifact.summary.override_path_mode
    ));
    out.push_str(&format!(
        "artifact_override_path_bytes_exact={}
",
        artifact.summary.override_path_bytes_exact
    ));
    out.push_str(&format!(
        "replay_selected_total_piecewise_payload_exact={}
",
        replay_selected_total_piecewise_payload_exact
    ));
    out.push_str(&format!("drift_exact={}
", drift_exact));
    out.push_str(&format!("collapse_90_failures={}
", collapse_90_failures));
    out.push_str(&format!("newline_extinct_failures={}
", newline_extinct_failures));

    if let Some(scoreboard) = scoreboard {
        out.push_str("
--- scoreboard ---
");
        out.push_str(&format!(
            "searched_total_piecewise_payload_exact={}
",
            scoreboard.searched_total_piecewise_payload_exact
        ));
        out.push_str(&format!(
            "artifact_selected_total_piecewise_payload_exact={}
",
            scoreboard.artifact_selected_total_piecewise_payload_exact
        ));
        out.push_str(&format!(
            "replay_selected_total_piecewise_payload_exact={}
",
            scoreboard.replay_selected_total_piecewise_payload_exact
        ));
        if let Some(v) = scoreboard.frozen_total_piecewise_payload_exact {
            out.push_str(&format!("frozen_total_piecewise_payload_exact={}
", v));
        }
        if let Some(v) = scoreboard.split_total_piecewise_payload_exact {
            out.push_str(&format!("split_total_piecewise_payload_exact={}
", v));
        }
        if let Some(v) = scoreboard.bridge_total_piecewise_payload_exact {
            out.push_str(&format!("bridge_total_piecewise_payload_exact={}
", v));
        }
    }

    out.push_str("
--- files ---
");
    for file in file_summaries {
        out.push_str(&format!(
            "input={} searched_total_piecewise_payload_exact={} artifact_selected_total_piecewise_payload_exact={} replay_selected_total_piecewise_payload_exact={} searched_target_window_payload_exact={} artifact_selected_target_window_payload_exact={} replay_target_window_payload_exact={} override_path_bytes_exact={} target_window_count={} drift_exact={} improved_vs_searched_count={} equal_vs_searched_count={} worsened_vs_searched_count={}
",
            file.input,
            file.searched_total_piecewise_payload_exact,
            file.artifact_selected_total_piecewise_payload_exact,
            file.replay_selected_total_piecewise_payload_exact,
            file.searched_target_window_payload_exact,
            file.artifact_selected_target_window_payload_exact,
            file.replay_target_window_payload_exact,
            file.override_path_bytes_exact,
            file.target_window_count,
            file.drift_exact,
            file.improved_vs_searched_count,
            file.equal_vs_searched_count,
            file.worsened_vs_searched_count,
        ));
    }

    let mut drifts = rows.to_vec();
    drifts.sort_by_key(|row| {
        (
            std::cmp::Reverse(row.delta_vs_artifact_exact.abs()),
            row.input_index,
            row.window_idx,
        )
    });
    out.push_str("
--- largest-window-drifts ---
");
    for row in drifts.into_iter().take(12) {
        out.push_str(&format!(
            "input={} window_idx={} target_ordinal={} start={} end={} selected_chunk_bytes={} searched_payload_exact={} artifact_selected_payload_exact={} replay_payload_exact={} delta_vs_artifact_exact={} delta_vs_searched_exact={} field_match_pct={:.6} collapse_90_flag={} newline_extinct_flag={} newline_floor_used={}
",
            row.input,
            row.window_idx,
            row.target_ordinal,
            row.start,
            row.end,
            row.selected_chunk_bytes,
            row.searched_payload_exact,
            row.artifact_selected_payload_exact,
            row.replay_payload_exact,
            row.delta_vs_artifact_exact,
            row.delta_vs_searched_exact,
            row.field_match_pct,
            row.collapse_90_flag,
            row.newline_extinct_flag,
            row.newline_floor_used,
        ));
    }

    out
}

impl LawProgramArtifact {

    fn encode(&self) -> Result<Vec<u8>> {
        let mut w = BinWriter::default();
        w.bytes(ARTIFACT_MAGIC);
        w.u8(ARTIFACT_VERSION);
        self.config.encode(&mut w);
        self.summary.encode(&mut w);
        w.uvar(self.files.len() as u64);
        for row in &self.files {
            row.encode(&mut w);
        }
        w.uvar(self.windows.len() as u64);
        for row in &self.windows {
            row.encode(&mut w);
        }
        w.uvar(self.overrides.len() as u64);
        for row in &self.overrides {
            row.encode(&mut w);
        }
        Ok(w.finish())
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        let mut r = BinReader::new(bytes);
        let magic = r.fixed_bytes(4)?;
        if magic.as_slice() != ARTIFACT_MAGIC {
            bail!("bad artifact magic");
        }
        let version = r.u8()?;
        if version != ARTIFACT_VERSION {
            bail!("unsupported artifact version {}", version);
        }
        let config = ReplayConfig::decode(&mut r)?;
        let summary = ProgramSummary::decode(&mut r)?;
        let file_len = r.uvar()? as usize;
        let mut files = Vec::with_capacity(file_len);
        for _ in 0..file_len {
            files.push(ProgramFile::decode(&mut r)?);
        }
        let window_len = r.uvar()? as usize;
        let mut windows = Vec::with_capacity(window_len);
        for _ in 0..window_len {
            windows.push(ProgramWindow::decode(&mut r)?);
        }
        let override_len = r.uvar()? as usize;
        let mut overrides = Vec::with_capacity(override_len);
        for _ in 0..override_len {
            overrides.push(ProgramOverride::decode(&mut r)?);
        }
        if !r.is_eof() {
            bail!("trailing bytes after artifact decode");
        }
        Ok(Self {
            config,
            summary,
            files,
            windows,
            overrides,
        })
    }
}

#[derive(Default)]
struct BinWriter {
    buf: Vec<u8>,
}

impl BinWriter {
    fn finish(self) -> Vec<u8> {
        self.buf
    }

    fn bytes(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    fn u8(&mut self, v: u8) {
        self.buf.push(v);
    }

    fn bool(&mut self, v: bool) {
        self.u8(if v { 1 } else { 0 });
    }

    fn uvar(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.buf.push(((v as u8) & 0x7F) | 0x80);
            v >>= 7;
        }
        self.buf.push(v as u8);
    }

    fn ivar(&mut self, v: i64) {
        let zigzag = ((v << 1) ^ (v >> 63)) as u64;
        self.uvar(zigzag);
    }

    fn string(&mut self, s: &str) {
        self.uvar(s.len() as u64);
        self.bytes(s.as_bytes());
    }

    fn opt_u64(&mut self, v: Option<u64>) {
        match v {
            Some(v) => {
                self.bool(true);
                self.uvar(v);
            }
            None => self.bool(false),
        }
    }

    fn opt_string(&mut self, v: &Option<String>) {
        match v {
            Some(v) => {
                self.bool(true);
                self.string(v);
            }
            None => self.bool(false),
        }
    }
}

struct BinReader<'a> {
    cur: Cursor<&'a [u8]>,
}

impl<'a> BinReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { cur: Cursor::new(bytes) }
    }

    fn is_eof(&self) -> bool {
        self.cur.position() as usize == self.cur.get_ref().len()
    }

    fn u8(&mut self) -> Result<u8> {
        let mut b = [0u8; 1];
        self.cur.read_exact(&mut b).context("read u8")?;
        Ok(b[0])
    }

    fn bool(&mut self) -> Result<bool> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            v => bail!("invalid bool byte {}", v),
        }
    }

    fn uvar(&mut self) -> Result<u64> {
        let mut shift = 0u32;
        let mut out = 0u64;
        loop {
            let b = self.u8()?;
            out |= ((b & 0x7F) as u64) << shift;
            if (b & 0x80) == 0 {
                return Ok(out);
            }
            shift += 7;
            if shift >= 64 {
                bail!("uvar too large");
            }
        }
    }

    fn ivar(&mut self) -> Result<i64> {
        let u = self.uvar()?;
        Ok(((u >> 1) as i64) ^ (-((u & 1) as i64)))
    }

    fn fixed_bytes(&mut self, len: usize) -> Result<Vec<u8>> {
        let mut out = vec![0u8; len];
        self.cur.read_exact(&mut out).with_context(|| format!("read {} bytes", len))?;
        Ok(out)
    }

    fn string(&mut self) -> Result<String> {
        let len = self.uvar()? as usize;
        let bytes = self.fixed_bytes(len)?;
        String::from_utf8(bytes).context("decode utf8 string")
    }

    fn opt_u64(&mut self) -> Result<Option<u64>> {
        if self.bool()? {
            Ok(Some(self.uvar()?))
        } else {
            Ok(None)
        }
    }

    fn opt_string(&mut self) -> Result<Option<String>> {
        if self.bool()? {
            Ok(Some(self.string()?))
        } else {
            Ok(None)
        }
    }
}

impl ReplayConfig {
    fn encode(&self, w: &mut BinWriter) {
        w.string(&self.recipe);
        w.uvar(self.inputs.len() as u64);
        for s in &self.inputs {
            w.string(s);
        }
        w.uvar(self.max_ticks);
        w.uvar(self.window_bytes as u64);
        w.uvar(self.step_bytes as u64);
        w.uvar(self.max_windows as u64);
        w.uvar(self.seed_from);
        w.uvar(self.seed_count);
        w.uvar(self.seed_step);
        w.uvar(self.recipe_seed);
        w.string(&self.chunk_sweep);
        w.string(&self.chunk_search_objective);
        w.uvar(self.chunk_raw_slack);
        w.uvar(self.map_max_depth as u64);
        w.uvar(self.map_depth_shift as u64);
        w.string(&self.boundary_band_sweep);
        w.uvar(self.boundary_delta as u64);
        w.string(&self.field_margin_sweep);
        w.uvar(self.newline_margin_add);
        w.uvar(self.space_to_newline_margin_add);
        w.uvar(self.newline_share_ppm_min as u64);
        w.uvar(self.newline_override_budget as u64);
        w.string(&self.newline_demote_margin_sweep);
        w.uvar(self.newline_demote_keep_ppm_min as u64);
        w.uvar(self.newline_demote_keep_min as u64);
        w.bool(self.newline_only_from_spacelike);
        w.uvar(self.merge_gap_bytes as u64);
        w.bool(self.allow_overlap_scout);
        w.opt_u64(self.freeze_boundary_band.map(|v| v as u64));
        w.opt_u64(self.freeze_field_margin);
        w.opt_u64(self.freeze_newline_demote_margin);
        w.string(&self.local_chunk_sweep);
        w.opt_string(&self.local_chunk_search_objective);
        w.opt_u64(self.local_chunk_raw_slack);
        w.opt_u64(self.default_local_chunk_bytes_arg.map(|v| v as u64));
        w.bool(self.tune_default_body);
        w.opt_string(&self.default_body_chunk_sweep);
        w.string(&self.body_select_objective);
        w.bool(self.emit_body_scoreboard);
        w.uvar(self.min_override_gain_exact as u64);
        w.uvar(self.exact_subset_limit as u64);
        w.opt_string(&self.global_law_id_arg);
    }

    fn decode(r: &mut BinReader<'_>) -> Result<Self> {
        let recipe = r.string()?;
        let inputs_len = r.uvar()? as usize;
        let mut inputs = Vec::with_capacity(inputs_len);
        for _ in 0..inputs_len {
            inputs.push(r.string()?);
        }
        Ok(Self {
            recipe,
            inputs,
            max_ticks: r.uvar()?,
            window_bytes: r.uvar()? as usize,
            step_bytes: r.uvar()? as usize,
            max_windows: r.uvar()? as usize,
            seed_from: r.uvar()?,
            seed_count: r.uvar()?,
            seed_step: r.uvar()?,
            recipe_seed: r.uvar()?,
            chunk_sweep: r.string()?,
            chunk_search_objective: r.string()?,
            chunk_raw_slack: r.uvar()?,
            map_max_depth: r.uvar()? as u8,
            map_depth_shift: r.uvar()? as u8,
            boundary_band_sweep: r.string()?,
            boundary_delta: r.uvar()? as usize,
            field_margin_sweep: r.string()?,
            newline_margin_add: r.uvar()?,
            space_to_newline_margin_add: r.uvar()?,
            newline_share_ppm_min: r.uvar()? as u32,
            newline_override_budget: r.uvar()? as usize,
            newline_demote_margin_sweep: r.string()?,
            newline_demote_keep_ppm_min: r.uvar()? as u32,
            newline_demote_keep_min: r.uvar()? as usize,
            newline_only_from_spacelike: r.bool()?,
            merge_gap_bytes: r.uvar()? as usize,
            allow_overlap_scout: r.bool()?,
            freeze_boundary_band: r.opt_u64()?.map(|v| v as usize),
            freeze_field_margin: r.opt_u64()?,
            freeze_newline_demote_margin: r.opt_u64()?,
            local_chunk_sweep: r.string()?,
            local_chunk_search_objective: r.opt_string()?,
            local_chunk_raw_slack: r.opt_u64()?,
            default_local_chunk_bytes_arg: r.opt_u64()?.map(|v| v as usize),
            tune_default_body: r.bool()?,
            default_body_chunk_sweep: r.opt_string()?,
            body_select_objective: r.string()?,
            emit_body_scoreboard: r.bool()?,
            min_override_gain_exact: r.uvar()? as usize,
            exact_subset_limit: r.uvar()? as usize,
            global_law_id_arg: r.opt_string()?,
        })
    }
}

impl ProgramSummary {
    fn encode(&self, w: &mut BinWriter) {
        w.string(&self.recipe);
        w.uvar(self.file_count as u64);
        w.uvar(self.honest_file_count as u64);
        w.uvar(self.union_law_count as u64);
        w.string(&self.target_global_law_id);
        w.uvar(self.target_global_law_path_hits as u64);
        w.uvar(self.target_global_law_file_count as u64);
        w.uvar(self.target_global_law_total_window_count as u64);
        w.uvar(self.target_global_law_total_segment_count as u64);
        w.uvar(self.target_global_law_total_covered_bytes as u64);
        w.string(&self.target_global_law_dominant_knob_signature);
        w.uvar(self.eval_boundary_band as u64);
        w.uvar(self.eval_field_margin);
        w.uvar(self.eval_newline_demote_margin);
        w.string(&self.eval_chunk_search_objective);
        w.uvar(self.eval_chunk_raw_slack);
        w.string(&self.eval_chunk_candidates);
        w.uvar(self.eval_chunk_candidate_count as u64);
        w.uvar(self.default_local_chunk_bytes as u64);
        w.uvar(self.default_local_chunk_window_wins as u64);
        w.ivar(self.searched_total_piecewise_payload_exact);
        w.ivar(self.projected_default_total_piecewise_payload_exact);
        w.ivar(self.delta_default_total_piecewise_payload_exact);
        w.ivar(self.projected_unpriced_best_mix_total_piecewise_payload_exact);
        w.ivar(self.delta_unpriced_best_mix_total_piecewise_payload_exact);
        w.ivar(self.selected_total_piecewise_payload_exact);
        w.ivar(self.delta_selected_total_piecewise_payload_exact);
        w.uvar(self.target_window_count as u64);
        w.uvar(self.searched_target_window_payload_exact as u64);
        w.uvar(self.default_target_window_payload_exact as u64);
        w.uvar(self.best_mix_target_window_payload_exact as u64);
        w.uvar(self.selected_target_window_payload_exact as u64);
        w.ivar(self.delta_selected_target_window_payload_exact);
        w.string(&self.override_path_mode);
        w.uvar(self.override_path_bytes_exact as u64);
        w.uvar(self.selected_override_window_count as u64);
        w.uvar(self.improved_target_window_count as u64);
        w.uvar(self.equal_target_window_count as u64);
        w.uvar(self.worsened_target_window_count as u64);
    }

    fn decode(r: &mut BinReader<'_>) -> Result<Self> {
        Ok(Self {
            recipe: r.string()?,
            file_count: r.uvar()? as usize,
            honest_file_count: r.uvar()? as usize,
            union_law_count: r.uvar()? as usize,
            target_global_law_id: r.string()?,
            target_global_law_path_hits: r.uvar()? as usize,
            target_global_law_file_count: r.uvar()? as usize,
            target_global_law_total_window_count: r.uvar()? as usize,
            target_global_law_total_segment_count: r.uvar()? as usize,
            target_global_law_total_covered_bytes: r.uvar()? as usize,
            target_global_law_dominant_knob_signature: r.string()?,
            eval_boundary_band: r.uvar()? as usize,
            eval_field_margin: r.uvar()?,
            eval_newline_demote_margin: r.uvar()?,
            eval_chunk_search_objective: r.string()?,
            eval_chunk_raw_slack: r.uvar()?,
            eval_chunk_candidates: r.string()?,
            eval_chunk_candidate_count: r.uvar()? as usize,
            default_local_chunk_bytes: r.uvar()? as usize,
            default_local_chunk_window_wins: r.uvar()? as usize,
            searched_total_piecewise_payload_exact: r.ivar()?,
            projected_default_total_piecewise_payload_exact: r.ivar()?,
            delta_default_total_piecewise_payload_exact: r.ivar()?,
            projected_unpriced_best_mix_total_piecewise_payload_exact: r.ivar()?,
            delta_unpriced_best_mix_total_piecewise_payload_exact: r.ivar()?,
            selected_total_piecewise_payload_exact: r.ivar()?,
            delta_selected_total_piecewise_payload_exact: r.ivar()?,
            target_window_count: r.uvar()? as usize,
            searched_target_window_payload_exact: r.uvar()? as usize,
            default_target_window_payload_exact: r.uvar()? as usize,
            best_mix_target_window_payload_exact: r.uvar()? as usize,
            selected_target_window_payload_exact: r.uvar()? as usize,
            delta_selected_target_window_payload_exact: r.ivar()?,
            override_path_mode: r.string()?,
            override_path_bytes_exact: r.uvar()? as usize,
            selected_override_window_count: r.uvar()? as usize,
            improved_target_window_count: r.uvar()? as usize,
            equal_target_window_count: r.uvar()? as usize,
            worsened_target_window_count: r.uvar()? as usize,
        })
    }
}

impl ProgramFile {
    fn encode(&self, w: &mut BinWriter) {
        w.string(&self.input);
        w.ivar(self.searched_total_piecewise_payload_exact);
        w.ivar(self.projected_default_total_piecewise_payload_exact);
        w.ivar(self.projected_unpriced_best_mix_total_piecewise_payload_exact);
        w.ivar(self.selected_total_piecewise_payload_exact);
        w.uvar(self.target_window_count as u64);
        w.string(&self.override_path_mode);
        w.uvar(self.override_path_bytes_exact as u64);
        w.uvar(self.selected_override_window_count as u64);
    }
    fn decode(r: &mut BinReader<'_>) -> Result<Self> {
        Ok(Self {
            input: r.string()?,
            searched_total_piecewise_payload_exact: r.ivar()?,
            projected_default_total_piecewise_payload_exact: r.ivar()?,
            projected_unpriced_best_mix_total_piecewise_payload_exact: r.ivar()?,
            selected_total_piecewise_payload_exact: r.ivar()?,
            target_window_count: r.uvar()? as usize,
            override_path_mode: r.string()?,
            override_path_bytes_exact: r.uvar()? as usize,
            selected_override_window_count: r.uvar()? as usize,
        })
    }
}

impl ProgramWindow {
    fn encode(&self, w: &mut BinWriter) {
        w.uvar(self.input_index as u64);
        w.string(&self.input);
        w.uvar(self.window_idx as u64);
        w.uvar(self.target_ordinal as u64);
        w.uvar(self.start as u64);
        w.uvar(self.end as u64);
        w.uvar(self.span_bytes as u64);
        w.uvar(self.searched_payload_exact as u64);
        w.uvar(self.default_payload_exact as u64);
        w.uvar(self.best_payload_exact as u64);
        w.uvar(self.selected_payload_exact as u64);
        w.uvar(self.searched_chunk_bytes as u64);
        w.uvar(self.best_chunk_bytes as u64);
        w.uvar(self.selected_chunk_bytes as u64);
        w.ivar(self.selected_gain_exact);
    }
    fn decode(r: &mut BinReader<'_>) -> Result<Self> {
        Ok(Self {
            input_index: r.uvar()? as usize,
            input: r.string()?,
            window_idx: r.uvar()? as usize,
            target_ordinal: r.uvar()? as usize,
            start: r.uvar()? as usize,
            end: r.uvar()? as usize,
            span_bytes: r.uvar()? as usize,
            searched_payload_exact: r.uvar()? as usize,
            default_payload_exact: r.uvar()? as usize,
            best_payload_exact: r.uvar()? as usize,
            selected_payload_exact: r.uvar()? as usize,
            searched_chunk_bytes: r.uvar()? as usize,
            best_chunk_bytes: r.uvar()? as usize,
            selected_chunk_bytes: r.uvar()? as usize,
            selected_gain_exact: r.ivar()?,
        })
    }
}

impl ProgramOverride {
    fn encode(&self, w: &mut BinWriter) {
        w.uvar(self.input_index as u64);
        w.string(&self.input);
        w.uvar(self.window_idx as u64);
        w.uvar(self.target_ordinal as u64);
        w.uvar(self.best_chunk_bytes as u64);
        w.uvar(self.default_payload_exact as u64);
        w.uvar(self.best_payload_exact as u64);
        w.uvar(self.gain_exact as u64);
    }
    fn decode(r: &mut BinReader<'_>) -> Result<Self> {
        Ok(Self {
            input_index: r.uvar()? as usize,
            input: r.string()?,
            window_idx: r.uvar()? as usize,
            target_ordinal: r.uvar()? as usize,
            best_chunk_bytes: r.uvar()? as usize,
            default_payload_exact: r.uvar()? as usize,
            best_payload_exact: r.uvar()? as usize,
            gain_exact: r.uvar()? as usize,
        })
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
    match raw.as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => bail!("parse bool key {} from {}", key, raw),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn csv_line_roundtrip_quotes() {
        let row = parse_csv_line("summary,\"a,b\",42").expect("parse csv");
        assert_eq!(row, vec!["summary", "a,b", "42"]);
    }

    #[test]
    fn txt_summary_stops_at_sections() {
        let raw = b"recipe=configs/tuned_validated.k8r\nfile_count=2\n\n--- files ---\ninput=x\n";
        let map = parse_txt_summary(raw).expect("parse txt summary");
        assert_eq!(map.get("recipe").unwrap(), "configs/tuned_validated.k8r");
        assert_eq!(map.get("file_count").unwrap(), "2");
        assert!(map.get("input").is_none());
    }


    #[test]
    fn select_best_materialized_prefers_lowest_selected_total_then_chunk() {
        fn make(chunk: usize, selected_total: i64, override_path: usize, override_count: usize) -> BuildMaterialized {
            BuildMaterialized {
                body_scores: Vec::new(),
                artifact: LawProgramArtifact {
                    config: ReplayConfig {
                        recipe: String::new(),
                        inputs: Vec::new(),
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
                        file_count: 0,
                        honest_file_count: 0,
                        union_law_count: 0,
                        target_global_law_id: String::new(),
                        target_global_law_path_hits: 0,
                        target_global_law_file_count: 0,
                        target_global_law_total_window_count: 0,
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
                        projected_default_total_piecewise_payload_exact: selected_total,
                        delta_default_total_piecewise_payload_exact: 0,
                        projected_unpriced_best_mix_total_piecewise_payload_exact: 0,
                        delta_unpriced_best_mix_total_piecewise_payload_exact: 0,
                        selected_total_piecewise_payload_exact: selected_total,
                        delta_selected_total_piecewise_payload_exact: 0,
                        target_window_count: 0,
                        searched_target_window_payload_exact: 0,
                        default_target_window_payload_exact: 0,
                        best_mix_target_window_payload_exact: 0,
                        selected_target_window_payload_exact: 0,
                        delta_selected_target_window_payload_exact: 0,
                        override_path_mode: "delta".to_string(),
                        override_path_bytes_exact: override_path,
                        selected_override_window_count: override_count,
                        improved_target_window_count: 0,
                        equal_target_window_count: 0,
                        worsened_target_window_count: 0,
                    },
                    files: Vec::new(),
                    windows: Vec::new(),
                    overrides: Vec::new(),
                },
            }
        }
        let items = vec![make(128, 100, 9, 2), make(96, 100, 9, 2), make(64, 101, 1, 1)];
        let idx = select_best_materialized_index(&items, "selected-total").unwrap();
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


#[test]
fn choose_best_override_path_plan_prefers_runs_for_dense_sequence() {
    let plan = choose_best_override_path_plan(&[4, 5, 6, 7]);
    assert_eq!(plan.mode, OverridePathMode::Runs);
}

#[test]
fn select_override_plan_skips_unprofitable_singleton() {
    let candidates = vec![OverrideCandidateRef {
        window_idx: 0,
        target_ordinal: 11,
        gain_exact: 1,
    }];
    let plan = select_override_plan(&candidates, 20);
    assert_eq!(plan.mode, OverridePathMode::None);
    assert_eq!(plan.path_bytes_exact, 0);
    assert!(plan.selected_window_ordinals.is_empty());
}

    #[test]
    fn artifact_roundtrip() {
        let artifact = LawProgramArtifact {
            config: ReplayConfig {
                recipe: "configs/tuned_validated.k8r".to_string(),
                inputs: vec!["text/Genesis1.txt".to_string()],
                max_ticks: 20,
                window_bytes: 256,
                step_bytes: 256,
                max_windows: 12,
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
                freeze_boundary_band: Some(12),
                freeze_field_margin: Some(4),
                freeze_newline_demote_margin: Some(4),
                local_chunk_sweep: "32,64,96,128".to_string(),
                local_chunk_search_objective: None,
                local_chunk_raw_slack: None,
                default_local_chunk_bytes_arg: Some(96),
                tune_default_body: true,
                default_body_chunk_sweep: Some("64,96,128".to_string()),
                body_select_objective: "selected-total".to_string(),
                emit_body_scoreboard: true,
                min_override_gain_exact: 1,
                exact_subset_limit: 20,
                global_law_id_arg: Some("G1".to_string()),
            },
            summary: ProgramSummary {
                recipe: "configs/tuned_validated.k8r".to_string(),
                file_count: 1,
                honest_file_count: 1,
                union_law_count: 2,
                target_global_law_id: "G1".to_string(),
                target_global_law_path_hits: 5,
                target_global_law_file_count: 1,
                target_global_law_total_window_count: 3,
                target_global_law_total_segment_count: 2,
                target_global_law_total_covered_bytes: 768,
                target_global_law_dominant_knob_signature: "chunk_bytes=64|chunk_search_objective=raw|chunk_raw_slack=1".to_string(),
                eval_boundary_band: 12,
                eval_field_margin: 4,
                eval_newline_demote_margin: 4,
                eval_chunk_search_objective: "raw".to_string(),
                eval_chunk_raw_slack: 1,
                eval_chunk_candidates: "32,64,96,128".to_string(),
                eval_chunk_candidate_count: 4,
                default_local_chunk_bytes: 96,
                default_local_chunk_window_wins: 2,
                searched_total_piecewise_payload_exact: 2200,
                projected_default_total_piecewise_payload_exact: 2128,
                delta_default_total_piecewise_payload_exact: -72,
                projected_unpriced_best_mix_total_piecewise_payload_exact: 2119,
                delta_unpriced_best_mix_total_piecewise_payload_exact: -81,
                selected_total_piecewise_payload_exact: 2126,
                delta_selected_total_piecewise_payload_exact: -74,
                target_window_count: 3,
                searched_target_window_payload_exact: 600,
                default_target_window_payload_exact: 520,
                best_mix_target_window_payload_exact: 510,
                selected_target_window_payload_exact: 512,
                delta_selected_target_window_payload_exact: -88,
                override_path_mode: "delta".to_string(),
                override_path_bytes_exact: 7,
                selected_override_window_count: 1,
                improved_target_window_count: 2,
                equal_target_window_count: 1,
                worsened_target_window_count: 0,
            },
            files: vec![ProgramFile {
                input: "text/Genesis1.txt".to_string(),
                searched_total_piecewise_payload_exact: 2200,
                projected_default_total_piecewise_payload_exact: 2128,
                projected_unpriced_best_mix_total_piecewise_payload_exact: 2119,
                selected_total_piecewise_payload_exact: 2126,
                target_window_count: 3,
                override_path_mode: "delta".to_string(),
                override_path_bytes_exact: 7,
                selected_override_window_count: 1,
            }],
            windows: vec![ProgramWindow {
                input_index: 0,
                input: "text/Genesis1.txt".to_string(),
                window_idx: 0,
                target_ordinal: 0,
                start: 0,
                end: 256,
                span_bytes: 256,
                searched_payload_exact: 200,
                default_payload_exact: 180,
                best_payload_exact: 170,
                selected_payload_exact: 170,
                searched_chunk_bytes: 64,
                best_chunk_bytes: 32,
                selected_chunk_bytes: 32,
                selected_gain_exact: 30,
            }],
            overrides: vec![ProgramOverride {
                input_index: 0,
                input: "text/Genesis1.txt".to_string(),
                window_idx: 0,
                target_ordinal: 0,
                best_chunk_bytes: 32,
                default_payload_exact: 180,
                best_payload_exact: 170,
                gain_exact: 10,
            }],
        };
        let bytes = artifact.encode().expect("encode artifact");
        let decoded = LawProgramArtifact::decode(&bytes).expect("decode artifact");
        assert_eq!(artifact.config, decoded.config);
        assert_eq!(artifact.summary, decoded.summary);
        assert_eq!(artifact.files, decoded.files);
        assert_eq!(artifact.windows, decoded.windows);
        assert_eq!(artifact.overrides, decoded.overrides);
    }
}
