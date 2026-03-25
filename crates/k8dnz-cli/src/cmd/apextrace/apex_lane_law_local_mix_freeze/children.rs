use anyhow::{anyhow, Context, Result};
use std::path::Path;
use std::process::Command;

use crate::cmd::apextrace::ApexLaneLawLocalMixFreezeArgs;

use super::config::chunk_search_objective_name;
use super::parsing::{parse_best_line, tokenize_kv_line};
use super::types::{EvalConfig, FrozenEvalRow, SearchKnobTuple};
use super::util::truncate_for_error;

pub(crate) fn parse_knob_signature(raw: &str) -> Result<SearchKnobTuple> {
    let tokens = tokenize_kv_line(raw);
    Ok(SearchKnobTuple {
        chunk_bytes: super::parsing::parse_required_usize(&tokens, "chunk_bytes")?,
        chunk_search_objective: super::parsing::parse_required_string(
            &tokens,
            "chunk_search_objective",
        )?,
        chunk_raw_slack: super::parsing::parse_required_u64(&tokens, "chunk_raw_slack")?,
    })
}

pub(crate) fn run_child_apex_lane_manifest(
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
            if args.newline_only_from_spacelike {
                "true"
            } else {
                "false"
            }
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

pub(crate) fn run_child_frozen_apex_map_lane(
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
            if args.newline_only_from_spacelike {
                "true"
            } else {
                "false"
            }
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
            truncate_for_error(&stdout),
        ));
    }

    parse_best_line(&output.stderr)
}
