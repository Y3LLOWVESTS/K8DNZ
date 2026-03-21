use anyhow::{anyhow, Context, Result};
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::cmd::apextrace::{ApexLaneManifestArgs, ChunkSearchObjective, RenderFormat};

use super::common::write_or_print;

const HEADER_MAGIC: &[u8; 4] = b"AKMH";
const LAW_MAGIC: &[u8; 4] = b"AKML";
const WINDOW_PATH_MAGIC: &[u8; 4] = b"AKMW";
const SEGMENT_PATH_MAGIC: &[u8; 4] = b"AKMS";
const VERSION: u8 = 1;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct SearchKnobTuple {
    chunk_bytes: usize,
    boundary_band: usize,
    field_margin: u64,
    newline_demote_margin: u64,
    chunk_search_objective: String,
    chunk_raw_slack: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ReplayLawTuple {
    boundary_band: usize,
    field_margin: u64,
    newline_demote_margin: u64,
}

#[derive(Clone, Debug)]
struct WindowRow {
    window_idx: usize,
    start: usize,
    end: usize,
    span_bytes: usize,
    elapsed_ms: u128,
    law_id: String,
    search: SearchKnobTuple,
    law: ReplayLawTuple,
    compact_field_total_payload_exact: usize,
    field_patch_bytes: usize,
    field_match_pct: f64,
    majority_baseline_match_pct: f64,
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
struct SegmentRow {
    segment_idx: usize,
    law_id: String,
    start: usize,
    end: usize,
    span_bytes: usize,
    window_count: usize,
    first_window_idx: usize,
    last_window_idx: usize,
    mean_compact_field_total_payload_exact: f64,
    mean_field_match_pct: f64,
    mean_field_balanced_accuracy_pct: f64,
    mean_field_macro_f1_pct: f64,
    mean_field_f1_newline_pct: f64,
}

#[derive(Clone, Debug)]
struct LawSummary {
    law_id: String,
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
struct ManifestSummary {
    input: String,
    recipe: String,
    input_bytes: usize,
    window_bytes: usize,
    step_bytes: usize,
    windows_analyzed: usize,
    total_window_span_bytes: usize,
    coverage_bytes: usize,
    overlap_bytes: usize,
    honest_non_overlapping: bool,
    allow_overlap_scout: bool,
    distinct_law_count: usize,
    segment_count: usize,
    law_switch_count: usize,
    total_elapsed_ms: u128,
    boundary_delta: usize,
    map_max_depth: u8,
    map_depth_shift: u8,
    newline_margin_add: u64,
    space_to_newline_margin_add: u64,
    newline_share_ppm_min: u32,
    newline_override_budget: usize,
    newline_demote_keep_ppm_min: u32,
    newline_demote_keep_min: usize,
    newline_only_from_spacelike: bool,
    local_compact_payload_bytes_exact: usize,
    shared_header_bytes_exact: usize,
    law_dictionary_bytes_exact: usize,
    window_path_bytes_exact: usize,
    segment_path_bytes_exact: usize,
    selected_path_mode: String,
    selected_path_bytes_exact: usize,
    total_piecewise_payload_exact: usize,
    seed_from: u64,
    seed_count: u64,
    seed_step: u64,
    recipe_seed: u64,
    chunk_sweep: String,
    boundary_band_sweep: String,
    field_margin_sweep: String,
    newline_demote_margin_sweep: String,
}

#[derive(Clone, Debug)]
struct ParsedBestRow {
    search: SearchKnobTuple,
    law: ReplayLawTuple,
    compact_field_total_payload_exact: usize,
    field_patch_bytes: usize,
    field_match_pct: f64,
    majority_baseline_match_pct: f64,
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
struct ReplayHeader {
    window_bytes: usize,
    step_bytes: usize,
    boundary_delta: usize,
    map_max_depth: u8,
    map_depth_shift: u8,
    newline_margin_add: u64,
    space_to_newline_margin_add: u64,
    newline_share_ppm_min: u32,
    newline_override_budget: usize,
    newline_demote_keep_ppm_min: u32,
    newline_demote_keep_min: usize,
    newline_only_from_spacelike: bool,
}

#[derive(Clone, Debug)]
struct WindowAccounting {
    total_window_span_bytes: usize,
    coverage_bytes: usize,
    overlap_bytes: usize,
    honest_non_overlapping: bool,
}

pub fn run_apex_lane_manifest(args: ApexLaneManifestArgs) -> Result<()> {
    if args.window_bytes == 0 {
        return Err(anyhow!("apex-lane-manifest requires --window-bytes >= 1"));
    }
    if args.step_bytes == 0 {
        return Err(anyhow!("apex-lane-manifest requires --step-bytes >= 1"));
    }
    if args.max_windows == 0 {
        return Err(anyhow!("apex-lane-manifest requires --max-windows >= 1"));
    }
    if args.field_from_global {
        return Err(anyhow!(
            "apex-lane-manifest does not support --field-from-global yet because replay accounting is defined for compact chunk manifests only"
        ));
    }

    let input = fs::read(&args.r#in).with_context(|| format!("read {}", args.r#in))?;
    let windows = build_windows(input.len(), args.window_bytes, args.step_bytes, args.max_windows);
    if windows.is_empty() {
        return Err(anyhow!(
            "apex-lane-manifest found no windows: input_bytes={} window_bytes={} step_bytes={}",
            input.len(),
            args.window_bytes,
            args.step_bytes
        ));
    }

    let accounting = compute_window_accounting(&windows);
    let coverage_bytes = accounting.coverage_bytes;
    let overlap_bytes = accounting.overlap_bytes;
    let honest_non_overlapping = accounting.honest_non_overlapping;
    if !honest_non_overlapping && !args.allow_overlap_scout {
        return Err(anyhow!(
            "apex-lane-manifest requires non-overlapping windows for honest accounting; got overlap_bytes={} from window_bytes={} step_bytes={}. Re-run with --step-bytes {} or pass --allow-overlap-scout for advisory output.",
            overlap_bytes,
            args.window_bytes,
            args.step_bytes,
            args.window_bytes,
        ));
    }

    let temp_dir = make_temp_dir("apex_lane_manifest")?;
    let exe = env::current_exe().context("resolve current executable for apex-lane-manifest")?;
    let started = Instant::now();
    let mut windows_out = Vec::with_capacity(windows.len());

    for (window_idx, (start, end)) in windows.iter().copied().enumerate() {
        let slice = &input[start..end];
        let window_path =
            temp_dir.join(format!("window_{:04}_{:08}_{:08}.bin", window_idx, start, end));
        fs::write(&window_path, slice)
            .with_context(|| format!("write window slice {}", window_path.display()))?;

        eprintln!(
            "apex-lane-manifest: start window_idx={} start={} end={} span_bytes={}",
            window_idx,
            start,
            end,
            end.saturating_sub(start)
        );

        let child_started = Instant::now();
        let output = run_child_apex_map_lane(&exe, &args, &window_path)?;
        let elapsed_ms = child_started.elapsed().as_millis();
        let parsed = parse_best_line(&output.stderr).with_context(|| {
            format!(
                "parse apex-map-lane best line for window_idx={} start={} end={}",
                window_idx, start, end
            )
        })?;

        windows_out.push(WindowRow {
            window_idx,
            start,
            end,
            span_bytes: end.saturating_sub(start),
            elapsed_ms,
            law_id: String::new(),
            search: parsed.search,
            law: parsed.law,
            compact_field_total_payload_exact: parsed.compact_field_total_payload_exact,
            field_patch_bytes: parsed.field_patch_bytes,
            field_match_pct: parsed.field_match_pct,
            majority_baseline_match_pct: parsed.majority_baseline_match_pct,
            field_match_vs_majority_pct: parsed.field_match_vs_majority_pct,
            field_balanced_accuracy_pct: parsed.field_balanced_accuracy_pct,
            field_macro_f1_pct: parsed.field_macro_f1_pct,
            field_f1_newline_pct: parsed.field_f1_newline_pct,
            field_pred_dominant_label: parsed.field_pred_dominant_label,
            field_pred_dominant_share_pct: parsed.field_pred_dominant_share_pct,
            field_pred_collapse_90_flag: parsed.field_pred_collapse_90_flag,
            field_pred_newline_delta: parsed.field_pred_newline_delta,
            field_newline_demoted: parsed.field_newline_demoted,
            field_newline_after_demote: parsed.field_newline_after_demote,
            field_newline_floor_used: parsed.field_newline_floor_used,
            field_newline_extinct_flag: parsed.field_newline_extinct_flag,
        });

        let row = windows_out.last().expect("window row exists");
        eprintln!(
            "apex-lane-manifest: done window_idx={} law=? boundary_band={} field_margin={} newline_demote_margin={} payload={} match_pct={:.6} elapsed_ms={}",
            row.window_idx,
            row.law.boundary_band,
            row.law.field_margin,
            row.law.newline_demote_margin,
            row.compact_field_total_payload_exact,
            row.field_match_pct,
            row.elapsed_ms
        );
    }

    let law_id_map = assign_law_ids(&windows_out);
    for row in &mut windows_out {
        row.law_id = law_id_map
            .get(&row.law)
            .cloned()
            .unwrap_or_else(|| "L?".to_string());
    }

    let segments = build_segments(&windows_out, args.merge_gap_bytes);
    let laws = build_law_summaries(&windows_out, &segments);
    let law_index_by_id = laws
        .iter()
        .enumerate()
        .map(|(idx, law)| (law.law_id.clone(), idx))
        .collect::<BTreeMap<_, _>>();

    let replay_header = ReplayHeader {
        window_bytes: args.window_bytes,
        step_bytes: args.step_bytes,
        boundary_delta: args.boundary_delta,
        map_max_depth: args.map_max_depth,
        map_depth_shift: args.map_depth_shift,
        newline_margin_add: args.newline_margin_add,
        space_to_newline_margin_add: args.space_to_newline_margin_add,
        newline_share_ppm_min: args.newline_share_ppm_min,
        newline_override_budget: args.newline_override_budget,
        newline_demote_keep_ppm_min: args.newline_demote_keep_ppm_min,
        newline_demote_keep_min: args.newline_demote_keep_min,
        newline_only_from_spacelike: args.newline_only_from_spacelike,
    };

    let shared_header_bytes_exact = encode_replay_header(&replay_header).len();
    let law_dictionary_bytes_exact = encode_law_dictionary(&laws).len();
    let window_path_bytes_exact =
        encode_window_path(input.len(), &windows_out, &law_index_by_id).len();
    let segment_path_bytes_exact =
        encode_segment_path(input.len(), &segments, &law_index_by_id).len();
    let (selected_path_mode, selected_path_bytes_exact) =
        if segment_path_bytes_exact <= window_path_bytes_exact {
            ("segment".to_string(), segment_path_bytes_exact)
        } else {
            ("window".to_string(), window_path_bytes_exact)
        };

    let local_compact_payload_bytes_exact = windows_out
        .iter()
        .map(|row| row.compact_field_total_payload_exact)
        .sum::<usize>();

    let total_piecewise_payload_exact = local_compact_payload_bytes_exact
        .saturating_add(shared_header_bytes_exact)
        .saturating_add(law_dictionary_bytes_exact)
        .saturating_add(selected_path_bytes_exact);

    let summary = ManifestSummary {
        input: args.r#in.clone(),
        recipe: args.recipe.clone(),
        input_bytes: input.len(),
        window_bytes: args.window_bytes,
        step_bytes: args.step_bytes,
        windows_analyzed: windows_out.len(),
        total_window_span_bytes: accounting.total_window_span_bytes,
        coverage_bytes,
        overlap_bytes,
        honest_non_overlapping,
        allow_overlap_scout: args.allow_overlap_scout,
        distinct_law_count: laws.len(),
        segment_count: segments.len(),
        law_switch_count: segments.len().saturating_sub(1),
        total_elapsed_ms: started.elapsed().as_millis(),
        boundary_delta: args.boundary_delta,
        map_max_depth: args.map_max_depth,
        map_depth_shift: args.map_depth_shift,
        newline_margin_add: args.newline_margin_add,
        space_to_newline_margin_add: args.space_to_newline_margin_add,
        newline_share_ppm_min: args.newline_share_ppm_min,
        newline_override_budget: args.newline_override_budget,
        newline_demote_keep_ppm_min: args.newline_demote_keep_ppm_min,
        newline_demote_keep_min: args.newline_demote_keep_min,
        newline_only_from_spacelike: args.newline_only_from_spacelike,
        local_compact_payload_bytes_exact,
        shared_header_bytes_exact,
        law_dictionary_bytes_exact,
        window_path_bytes_exact,
        segment_path_bytes_exact,
        selected_path_mode,
        selected_path_bytes_exact,
        total_piecewise_payload_exact,
        seed_from: args.seed_from,
        seed_count: args.seed_count,
        seed_step: args.seed_step,
        recipe_seed: args.recipe_seed,
        chunk_sweep: args.chunk_sweep.clone(),
        boundary_band_sweep: args.boundary_band_sweep.clone(),
        field_margin_sweep: args.field_margin_sweep.clone(),
        newline_demote_margin_sweep: args.newline_demote_margin_sweep.clone(),
    };

    let body = match args.format {
        RenderFormat::Txt => render_txt(&summary, &laws, &segments, &windows_out),
        RenderFormat::Csv => render_csv(&summary, &laws, &segments, &windows_out),
    };
    write_or_print(args.out.as_deref(), &body)?;

    if args.keep_temp_dir {
        eprintln!("apex-lane-manifest: temp_dir={}", temp_dir.display());
    } else if let Err(err) = fs::remove_dir_all(&temp_dir) {
        eprintln!(
            "apex-lane-manifest: warning could not remove temp_dir={} err={}",
            temp_dir.display(),
            err
        );
    }

    if let Some(path) = args.out.as_deref() {
        eprintln!(
            "apex-lane-manifest: out={} windows={} laws={} segments={} total_piecewise_payload_exact={} total_elapsed_ms={} honest_non_overlapping={}",
            path,
            summary.windows_analyzed,
            summary.distinct_law_count,
            summary.segment_count,
            summary.total_piecewise_payload_exact,
            summary.total_elapsed_ms,
            summary.honest_non_overlapping,
        );
    } else {
        eprintln!(
            "apex-lane-manifest: windows={} laws={} segments={} total_piecewise_payload_exact={} total_elapsed_ms={} honest_non_overlapping={}",
            summary.windows_analyzed,
            summary.distinct_law_count,
            summary.segment_count,
            summary.total_piecewise_payload_exact,
            summary.total_elapsed_ms,
            summary.honest_non_overlapping,
        );
    }

    Ok(())
}

fn build_windows(
    input_len: usize,
    window_bytes: usize,
    step_bytes: usize,
    max_windows: usize,
) -> Vec<(usize, usize)> {
    if input_len == 0 || max_windows == 0 {
        return Vec::new();
    }
    if input_len <= window_bytes {
        return vec![(0, input_len)];
    }

    let mut out = Vec::new();
    let mut start = 0usize;
    while start < input_len && out.len() < max_windows {
        let end = start.saturating_add(window_bytes).min(input_len);
        out.push((start, end));
        if end == input_len {
            break;
        }
        start = start.saturating_add(step_bytes);
    }
    out
}

fn coverage_bytes(windows: &[(usize, usize)]) -> usize {
    if windows.is_empty() {
        return 0;
    }

    let mut sorted = windows.to_vec();
    sorted.sort_unstable_by_key(|(start, end)| (*start, *end));

    let mut total = 0usize;
    let mut cur = sorted[0];
    for &(start, end) in sorted.iter().skip(1) {
        if start <= cur.1 {
            cur.1 = cur.1.max(end);
        } else {
            total = total.saturating_add(cur.1.saturating_sub(cur.0));
            cur = (start, end);
        }
    }

    total.saturating_add(cur.1.saturating_sub(cur.0))
}

fn compute_window_accounting(windows: &[(usize, usize)]) -> WindowAccounting {
    let total_window_span_bytes = windows
        .iter()
        .map(|(start, end)| end.saturating_sub(*start))
        .sum::<usize>();
    let coverage_bytes = coverage_bytes(windows);
    let overlap_bytes = total_window_span_bytes.saturating_sub(coverage_bytes);
    let honest_non_overlapping = overlap_bytes == 0;

    WindowAccounting {
        total_window_span_bytes,
        coverage_bytes,
        overlap_bytes,
        honest_non_overlapping,
    }
}

fn run_child_apex_map_lane(
    exe: &Path,
    args: &ApexLaneManifestArgs,
    window_path: &Path,
) -> Result<std::process::Output> {
    let first_chunk = first_csv_token_usize(&args.chunk_sweep, "chunk_sweep")?;
    let first_band = first_csv_token_usize(&args.boundary_band_sweep, "boundary_band_sweep")?;
    let first_margin = first_csv_token_u64(&args.field_margin_sweep, "field_margin_sweep")?;
    let first_demote =
        first_csv_token_u64(&args.newline_demote_margin_sweep, "newline_demote_margin_sweep")?;

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
        .arg(first_chunk.to_string())
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
        .arg("--boundary-band")
        .arg(first_band.to_string())
        .arg("--boundary-band-sweep")
        .arg(&args.boundary_band_sweep)
        .arg("--boundary-delta")
        .arg(args.boundary_delta.to_string())
        .arg("--field-margin")
        .arg(first_margin.to_string())
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
        .arg("--newline-demote-margin")
        .arg(first_demote.to_string())
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
        .arg("--format")
        .arg("txt");

    let output = cmd
        .output()
        .with_context(|| format!("spawn child apex-map-lane for {}", window_path.display()))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Err(anyhow!(
            "child apex-map-lane failed status={} window={} stderr={} stdout={}",
            output.status,
            window_path.display(),
            truncate_for_error(&stderr),
            truncate_for_error(&stdout)
        ));
    }

    Ok(output)
}

fn assign_law_ids(rows: &[WindowRow]) -> BTreeMap<ReplayLawTuple, String> {
    let mut ordered = BTreeMap::<ReplayLawTuple, String>::new();
    for row in rows {
        if !ordered.contains_key(&row.law) {
            let id = format!("L{}", ordered.len());
            ordered.insert(row.law.clone(), id);
        }
    }
    ordered
}

fn build_segments(rows: &[WindowRow], merge_gap_bytes: usize) -> Vec<SegmentRow> {
    if rows.is_empty() {
        return Vec::new();
    }

    let mut out = Vec::new();
    let mut cur_law = rows[0].law_id.clone();
    let mut cur_start = rows[0].start;
    let mut cur_end = rows[0].end;
    let mut cur_first_idx = rows[0].window_idx;
    let mut cur_last_idx = rows[0].window_idx;
    let mut cur_windows = vec![rows[0].clone()];

    for row in rows.iter().skip(1) {
        if row.law_id == cur_law && row.start <= cur_end.saturating_add(merge_gap_bytes) {
            cur_end = cur_end.max(row.end);
            cur_last_idx = row.window_idx;
            cur_windows.push(row.clone());
        } else {
            out.push(finish_segment(
                out.len(),
                &cur_law,
                cur_start,
                cur_end,
                cur_first_idx,
                cur_last_idx,
                &cur_windows,
            ));
            cur_law = row.law_id.clone();
            cur_start = row.start;
            cur_end = row.end;
            cur_first_idx = row.window_idx;
            cur_last_idx = row.window_idx;
            cur_windows.clear();
            cur_windows.push(row.clone());
        }
    }

    out.push(finish_segment(
        out.len(),
        &cur_law,
        cur_start,
        cur_end,
        cur_first_idx,
        cur_last_idx,
        &cur_windows,
    ));
    out
}

fn finish_segment(
    segment_idx: usize,
    law_id: &str,
    start: usize,
    end: usize,
    first_window_idx: usize,
    last_window_idx: usize,
    windows: &[WindowRow],
) -> SegmentRow {
    let count = windows.len().max(1) as f64;
    SegmentRow {
        segment_idx,
        law_id: law_id.to_string(),
        start,
        end,
        span_bytes: end.saturating_sub(start),
        window_count: windows.len(),
        first_window_idx,
        last_window_idx,
        mean_compact_field_total_payload_exact: windows
            .iter()
            .map(|w| w.compact_field_total_payload_exact as f64)
            .sum::<f64>()
            / count,
        mean_field_match_pct: windows.iter().map(|w| w.field_match_pct).sum::<f64>() / count,
        mean_field_balanced_accuracy_pct: windows
            .iter()
            .map(|w| w.field_balanced_accuracy_pct)
            .sum::<f64>()
            / count,
        mean_field_macro_f1_pct: windows.iter().map(|w| w.field_macro_f1_pct).sum::<f64>() / count,
        mean_field_f1_newline_pct: windows
            .iter()
            .map(|w| w.field_f1_newline_pct)
            .sum::<f64>()
            / count,
    }
}

fn build_law_summaries(rows: &[WindowRow], segments: &[SegmentRow]) -> Vec<LawSummary> {
    let mut by_law = BTreeMap::<String, Vec<&WindowRow>>::new();
    for row in rows {
        by_law.entry(row.law_id.clone()).or_default().push(row);
    }

    let mut covered_bytes = BTreeMap::<String, usize>::new();
    let mut segment_count = BTreeMap::<String, usize>::new();
    for seg in segments {
        *covered_bytes.entry(seg.law_id.clone()).or_default() += seg.span_bytes;
        *segment_count.entry(seg.law_id.clone()).or_default() += 1;
    }

    let mut out = Vec::new();
    for (law_id, law_rows) in by_law {
        let count = law_rows.len().max(1) as f64;
        out.push(LawSummary {
            law_id: law_id.clone(),
            law: law_rows[0].law.clone(),
            window_count: law_rows.len(),
            segment_count: *segment_count.get(&law_id).unwrap_or(&0),
            covered_bytes: *covered_bytes.get(&law_id).unwrap_or(&0),
            mean_compact_field_total_payload_exact: law_rows
                .iter()
                .map(|w| w.compact_field_total_payload_exact as f64)
                .sum::<f64>()
                / count,
            mean_field_match_pct: law_rows.iter().map(|w| w.field_match_pct).sum::<f64>() / count,
            mean_field_match_vs_majority_pct: law_rows
                .iter()
                .map(|w| w.field_match_vs_majority_pct)
                .sum::<f64>()
                / count,
            mean_field_balanced_accuracy_pct: law_rows
                .iter()
                .map(|w| w.field_balanced_accuracy_pct)
                .sum::<f64>()
                / count,
            mean_field_macro_f1_pct: law_rows.iter().map(|w| w.field_macro_f1_pct).sum::<f64>() / count,
            mean_field_f1_newline_pct: law_rows
                .iter()
                .map(|w| w.field_f1_newline_pct)
                .sum::<f64>()
                / count,
        });
    }
    out
}

fn encode_replay_header(header: &ReplayHeader) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(HEADER_MAGIC);
    out.push(VERSION);
    out.push(0);
    put_varint(header.window_bytes as u64, &mut out);
    put_varint(header.step_bytes as u64, &mut out);
    put_varint(header.boundary_delta as u64, &mut out);
    out.push(header.map_max_depth);
    out.push(header.map_depth_shift);
    put_varint(header.newline_margin_add, &mut out);
    put_varint(header.space_to_newline_margin_add, &mut out);
    put_varint(header.newline_share_ppm_min as u64, &mut out);
    put_varint(header.newline_override_budget as u64, &mut out);
    put_varint(header.newline_demote_keep_ppm_min as u64, &mut out);
    put_varint(header.newline_demote_keep_min as u64, &mut out);
    out.push(if header.newline_only_from_spacelike {
        1
    } else {
        0
    });
    out
}

fn encode_law_dictionary(laws: &[LawSummary]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(LAW_MAGIC);
    out.push(VERSION);
    out.push(0);
    put_varint(laws.len() as u64, &mut out);
    for law in laws {
        put_varint(law.law.boundary_band as u64, &mut out);
        put_varint(law.law.field_margin, &mut out);
        put_varint(law.law.newline_demote_margin, &mut out);
    }
    out
}

fn encode_window_path(
    input_bytes: usize,
    windows: &[WindowRow],
    law_index_by_id: &BTreeMap<String, usize>,
) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(WINDOW_PATH_MAGIC);
    out.push(VERSION);
    out.push(0);
    put_varint(input_bytes as u64, &mut out);
    put_varint(windows.len() as u64, &mut out);
    for row in windows {
        let law_idx = *law_index_by_id.get(&row.law_id).unwrap_or(&0);
        put_varint(law_idx as u64, &mut out);
        put_varint(row.span_bytes as u64, &mut out);
    }
    out
}

fn encode_segment_path(
    input_bytes: usize,
    segments: &[SegmentRow],
    law_index_by_id: &BTreeMap<String, usize>,
) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(SEGMENT_PATH_MAGIC);
    out.push(VERSION);
    out.push(0);
    put_varint(input_bytes as u64, &mut out);
    put_varint(segments.len() as u64, &mut out);
    let mut prev_end = 0usize;
    for seg in segments {
        let law_idx = *law_index_by_id.get(&seg.law_id).unwrap_or(&0);
        put_varint(law_idx as u64, &mut out);
        put_varint(seg.start.saturating_sub(prev_end) as u64, &mut out);
        put_varint(seg.span_bytes as u64, &mut out);
        prev_end = seg.end;
    }
    out
}

fn parse_best_line(stderr: &[u8]) -> Result<ParsedBestRow> {
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

    let mut map = BTreeMap::<String, String>::new();
    for token in payload.split_whitespace() {
        if let Some((k, v)) = token.split_once('=') {
            map.insert(k.to_string(), v.to_string());
        }
    }

    let search = SearchKnobTuple {
        chunk_bytes: parse_required_usize(&map, "chunk_bytes")?,
        boundary_band: parse_required_usize(&map, "boundary_band")?,
        field_margin: parse_required_u64(&map, "field_margin")?,
        newline_demote_margin: parse_required_u64(&map, "newline_demote_margin")?,
        chunk_search_objective: parse_required_string(&map, "chunk_search_objective")?,
        chunk_raw_slack: parse_required_u64(&map, "chunk_raw_slack")?,
    };

    Ok(ParsedBestRow {
        law: ReplayLawTuple {
            boundary_band: search.boundary_band,
            field_margin: search.field_margin,
            newline_demote_margin: search.newline_demote_margin,
        },
        search,
        compact_field_total_payload_exact: parse_required_usize(
            &map,
            "compact_field_total_payload_exact",
        )?,
        field_patch_bytes: parse_required_usize(&map, "field_patch_bytes")?,
        field_match_pct: parse_required_f64(&map, "field_match_pct")?,
        majority_baseline_match_pct: parse_required_f64(&map, "majority_baseline_match_pct")?,
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

fn parse_required_string(map: &BTreeMap<String, String>, key: &str) -> Result<String> {
    map.get(key)
        .cloned()
        .ok_or_else(|| anyhow!("missing key {} in apex-map-lane best line", key))
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

fn first_csv_token_usize(raw: &str, label: &str) -> Result<usize> {
    let token = raw
        .split(',')
        .map(|v| v.trim())
        .find(|v| !v.is_empty())
        .ok_or_else(|| anyhow!("{} requires at least one value", label))?;
    token
        .parse::<usize>()
        .with_context(|| format!("parse {} first token {} as usize", label, token))
}

fn first_csv_token_u64(raw: &str, label: &str) -> Result<u64> {
    let token = raw
        .split(',')
        .map(|v| v.trim())
        .find(|v| !v.is_empty())
        .ok_or_else(|| anyhow!("{} requires at least one value", label))?;
    token
        .parse::<u64>()
        .with_context(|| format!("parse {} first token {} as u64", label, token))
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

fn render_txt(
    summary: &ManifestSummary,
    laws: &[LawSummary],
    segments: &[SegmentRow],
    windows: &[WindowRow],
) -> String {
    let mut out = String::new();

    macro_rules! line {
        ($k:expr, $v:expr) => {{
            out.push_str($k);
            out.push('=');
            out.push_str(&$v.to_string());
            out.push('\n');
        }};
    }

    line!("input", summary.input.clone());
    line!("recipe", summary.recipe.clone());
    line!("input_bytes", summary.input_bytes);
    line!("window_bytes", summary.window_bytes);
    line!("step_bytes", summary.step_bytes);
    line!("windows_analyzed", summary.windows_analyzed);
    line!("total_window_span_bytes", summary.total_window_span_bytes);
    line!("coverage_bytes", summary.coverage_bytes);
    line!("overlap_bytes", summary.overlap_bytes);
    line!("honest_non_overlapping", summary.honest_non_overlapping);
    line!("allow_overlap_scout", summary.allow_overlap_scout);
    line!("distinct_law_count", summary.distinct_law_count);
    line!("segment_count", summary.segment_count);
    line!("law_switch_count", summary.law_switch_count);
    line!("boundary_delta", summary.boundary_delta);
    line!("map_max_depth", summary.map_max_depth);
    line!("map_depth_shift", summary.map_depth_shift);
    line!("newline_margin_add", summary.newline_margin_add);
    line!(
        "space_to_newline_margin_add",
        summary.space_to_newline_margin_add
    );
    line!("newline_share_ppm_min", summary.newline_share_ppm_min);
    line!("newline_override_budget", summary.newline_override_budget);
    line!(
        "newline_demote_keep_ppm_min",
        summary.newline_demote_keep_ppm_min
    );
    line!("newline_demote_keep_min", summary.newline_demote_keep_min);
    line!(
        "newline_only_from_spacelike",
        summary.newline_only_from_spacelike
    );
    line!(
        "local_compact_payload_bytes_exact",
        summary.local_compact_payload_bytes_exact
    );
    line!("shared_header_bytes_exact", summary.shared_header_bytes_exact);
    line!(
        "law_dictionary_bytes_exact",
        summary.law_dictionary_bytes_exact
    );
    line!("window_path_bytes_exact", summary.window_path_bytes_exact);
    line!("segment_path_bytes_exact", summary.segment_path_bytes_exact);
    line!("selected_path_mode", summary.selected_path_mode.clone());
    line!("selected_path_bytes_exact", summary.selected_path_bytes_exact);
    line!(
        "total_piecewise_payload_exact",
        summary.total_piecewise_payload_exact
    );
    line!("seed_from", summary.seed_from);
    line!("seed_count", summary.seed_count);
    line!("seed_step", summary.seed_step);
    line!("recipe_seed", summary.recipe_seed);
    line!("chunk_sweep", summary.chunk_sweep.clone());
    line!("boundary_band_sweep", summary.boundary_band_sweep.clone());
    line!("field_margin_sweep", summary.field_margin_sweep.clone());
    line!(
        "newline_demote_margin_sweep",
        summary.newline_demote_margin_sweep.clone()
    );
    line!(
        "law_path",
        windows
            .iter()
            .map(|w| w.law_id.as_str())
            .collect::<Vec<_>>()
            .join(",")
    );

    out.push_str("\n--- laws ---\n");
    for law in laws {
        out.push_str(&format!(
            "law_id={} boundary_band={} field_margin={} newline_demote_margin={} window_count={} segment_count={} covered_bytes={} mean_compact_field_total_payload_exact={:.3} mean_field_match_pct={:.6} mean_field_match_vs_majority_pct={:.6} mean_field_balanced_accuracy_pct={:.6} mean_field_macro_f1_pct={:.6} mean_field_f1_newline_pct={:.6}\n",
            law.law_id,
            law.law.boundary_band,
            law.law.field_margin,
            law.law.newline_demote_margin,
            law.window_count,
            law.segment_count,
            law.covered_bytes,
            law.mean_compact_field_total_payload_exact,
            law.mean_field_match_pct,
            law.mean_field_match_vs_majority_pct,
            law.mean_field_balanced_accuracy_pct,
            law.mean_field_macro_f1_pct,
            law.mean_field_f1_newline_pct,
        ));
    }

    out.push_str("\n--- segments ---\n");
    for seg in segments {
        out.push_str(&format!(
            "segment_idx={} law_id={} start={} end={} span_bytes={} window_count={} first_window_idx={} last_window_idx={} mean_compact_field_total_payload_exact={:.3} mean_field_match_pct={:.6} mean_field_balanced_accuracy_pct={:.6} mean_field_macro_f1_pct={:.6} mean_field_f1_newline_pct={:.6}\n",
            seg.segment_idx,
            seg.law_id,
            seg.start,
            seg.end,
            seg.span_bytes,
            seg.window_count,
            seg.first_window_idx,
            seg.last_window_idx,
            seg.mean_compact_field_total_payload_exact,
            seg.mean_field_match_pct,
            seg.mean_field_balanced_accuracy_pct,
            seg.mean_field_macro_f1_pct,
            seg.mean_field_f1_newline_pct,
        ));
    }

    out.push_str("\n--- windows ---\n");
    for row in windows {
        out.push_str(&format!(
            "window_idx={} law_id={} start={} end={} span_bytes={} chunk_bytes={} boundary_band={} field_margin={} newline_demote_margin={} chunk_search_objective={} chunk_raw_slack={} compact_field_total_payload_exact={} field_patch_bytes={} field_match_pct={:.6} majority_baseline_match_pct={:.6} field_match_vs_majority_pct={:.6} field_balanced_accuracy_pct={:.6} field_macro_f1_pct={:.6} field_f1_newline_pct={:.6} field_pred_dominant_label={} field_pred_dominant_share_pct={:.6} field_pred_collapse_90_flag={} field_pred_newline_delta={} field_newline_demoted={} field_newline_after_demote={} field_newline_floor_used={} field_newline_extinct_flag={}\n",
            row.window_idx,
            row.law_id,
            row.start,
            row.end,
            row.span_bytes,
            row.search.chunk_bytes,
            row.law.boundary_band,
            row.law.field_margin,
            row.law.newline_demote_margin,
            row.search.chunk_search_objective,
            row.search.chunk_raw_slack,
            row.compact_field_total_payload_exact,
            row.field_patch_bytes,
            row.field_match_pct,
            row.majority_baseline_match_pct,
            row.field_match_vs_majority_pct,
            row.field_balanced_accuracy_pct,
            row.field_macro_f1_pct,
            row.field_f1_newline_pct,
            row.field_pred_dominant_label,
            row.field_pred_dominant_share_pct,
            row.field_pred_collapse_90_flag,
            row.field_pred_newline_delta,
            row.field_newline_demoted,
            row.field_newline_after_demote,
            row.field_newline_floor_used,
            row.field_newline_extinct_flag,
        ));
    }

    out
}

fn render_csv(
    summary: &ManifestSummary,
    laws: &[LawSummary],
    segments: &[SegmentRow],
    windows: &[WindowRow],
) -> String {
    let mut out = String::new();
    out.push_str(&csv_row(&[
        "row_kind".to_string(),
        "id".to_string(),
        "law_id".to_string(),
        "input".to_string(),
        "recipe".to_string(),
        "start".to_string(),
        "end".to_string(),
        "span_bytes".to_string(),
        "window_count".to_string(),
        "chunk_bytes".to_string(),
        "boundary_band".to_string(),
        "field_margin".to_string(),
        "newline_demote_margin".to_string(),
        "compact_field_total_payload_exact".to_string(),
        "field_patch_bytes".to_string(),
        "field_match_pct".to_string(),
        "mean_field_match_pct".to_string(),
        "shared_header_bytes_exact".to_string(),
        "law_dictionary_bytes_exact".to_string(),
        "window_path_bytes_exact".to_string(),
        "segment_path_bytes_exact".to_string(),
        "selected_path_mode".to_string(),
        "selected_path_bytes_exact".to_string(),
        "local_compact_payload_bytes_exact".to_string(),
        "total_piecewise_payload_exact".to_string(),
    ]));

    out.push_str(&csv_row(&[
        "summary".to_string(),
        "summary".to_string(),
        String::new(),
        summary.input.clone(),
        summary.recipe.clone(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        summary.shared_header_bytes_exact.to_string(),
        summary.law_dictionary_bytes_exact.to_string(),
        summary.window_path_bytes_exact.to_string(),
        summary.segment_path_bytes_exact.to_string(),
        summary.selected_path_mode.clone(),
        summary.selected_path_bytes_exact.to_string(),
        summary.local_compact_payload_bytes_exact.to_string(),
        summary.total_piecewise_payload_exact.to_string(),
    ]));

    for law in laws {
        out.push_str(&csv_row(&[
            "law".to_string(),
            law.law_id.clone(),
            law.law_id.clone(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            law.law.boundary_band.to_string(),
            law.law.field_margin.to_string(),
            law.law.newline_demote_margin.to_string(),
            format!("{:.3}", law.mean_compact_field_total_payload_exact),
            String::new(),
            String::new(),
            format!("{:.6}", law.mean_field_match_pct),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
        ]));
    }

    for seg in segments {
        out.push_str(&csv_row(&[
            "segment".to_string(),
            seg.segment_idx.to_string(),
            seg.law_id.clone(),
            String::new(),
            String::new(),
            seg.start.to_string(),
            seg.end.to_string(),
            seg.span_bytes.to_string(),
            seg.window_count.to_string(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            format!("{:.3}", seg.mean_compact_field_total_payload_exact),
            String::new(),
            String::new(),
            format!("{:.6}", seg.mean_field_match_pct),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
        ]));
    }

    for row in windows {
        out.push_str(&csv_row(&[
            "window".to_string(),
            row.window_idx.to_string(),
            row.law_id.clone(),
            String::new(),
            String::new(),
            row.start.to_string(),
            row.end.to_string(),
            row.span_bytes.to_string(),
            String::new(),
            row.search.chunk_bytes.to_string(),
            row.law.boundary_band.to_string(),
            row.law.field_margin.to_string(),
            row.law.newline_demote_margin.to_string(),
            row.compact_field_total_payload_exact.to_string(),
            row.field_patch_bytes.to_string(),
            format!("{:.6}", row.field_match_pct),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
        ]));
    }

    out
}

fn csv_row(cols: &[String]) -> String {
    let mut out = String::new();
    for (idx, col) in cols.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        out.push_str(&csv_escape(col));
    }
    out.push('\n');
    out
}

fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        assign_law_ids, build_segments, compute_window_accounting, coverage_bytes,
        encode_law_dictionary, encode_segment_path, encode_window_path, LawSummary,
        ReplayLawTuple, SearchKnobTuple, WindowRow,
    };
    use std::collections::BTreeMap;

    fn sample_window(window_idx: usize, start: usize, end: usize, boundary_band: usize) -> WindowRow {
        WindowRow {
            window_idx,
            start,
            end,
            span_bytes: end.saturating_sub(start),
            elapsed_ms: 1,
            law_id: String::new(),
            search: SearchKnobTuple {
                chunk_bytes: 64,
                boundary_band,
                field_margin: 4,
                newline_demote_margin: 0,
                chunk_search_objective: "raw".to_string(),
                chunk_raw_slack: 1,
            },
            law: ReplayLawTuple {
                boundary_band,
                field_margin: 4,
                newline_demote_margin: 0,
            },
            compact_field_total_payload_exact: 10,
            field_patch_bytes: 3,
            field_match_pct: 70.0,
            majority_baseline_match_pct: 80.0,
            field_match_vs_majority_pct: -10.0,
            field_balanced_accuracy_pct: 50.0,
            field_macro_f1_pct: 45.0,
            field_f1_newline_pct: 12.0,
            field_pred_dominant_label: "other".to_string(),
            field_pred_dominant_share_pct: 90.0,
            field_pred_collapse_90_flag: true,
            field_pred_newline_delta: -2,
            field_newline_demoted: 0,
            field_newline_after_demote: 1,
            field_newline_floor_used: 0,
            field_newline_extinct_flag: false,
        }
    }

    #[test]
    fn coverage_bytes_handles_overlap() {
        let windows = vec![(0usize, 10usize), (5usize, 15usize), (20usize, 30usize)];
        assert_eq!(coverage_bytes(&windows), 25);
    }

    #[test]
    fn segment_path_is_smaller_than_window_path_for_repeated_laws() {
        let mut rows = vec![
            sample_window(0, 0, 10, 8),
            sample_window(1, 10, 20, 8),
            sample_window(2, 20, 30, 8),
            sample_window(3, 30, 40, 12),
        ];
        let law_ids = assign_law_ids(&rows);
        for row in &mut rows {
            row.law_id = law_ids.get(&row.law).cloned().unwrap();
        }
        let segments = build_segments(&rows, 0);
        let law_index_by_id = law_ids
            .values()
            .enumerate()
            .map(|(idx, law_id)| (law_id.clone(), idx))
            .collect::<BTreeMap<_, _>>();
        let window_bytes = encode_window_path(40, &rows, &law_index_by_id).len();
        let segment_bytes = encode_segment_path(40, &segments, &law_index_by_id).len();
        assert!(segment_bytes < window_bytes);
    }

    #[test]
    fn law_dictionary_collapses_same_replay_law() {
        let mut rows = vec![sample_window(0, 0, 10, 8), sample_window(1, 10, 20, 8)];
        rows[1].search.chunk_bytes = 32;
        let law_ids = assign_law_ids(&rows);
        for row in &mut rows {
            row.law_id = law_ids.get(&row.law).cloned().unwrap();
        }
        let laws = vec![LawSummary {
            law_id: "L0".to_string(),
            law: ReplayLawTuple {
                boundary_band: 8,
                field_margin: 4,
                newline_demote_margin: 0,
            },
            window_count: 2,
            segment_count: 1,
            covered_bytes: 20,
            mean_compact_field_total_payload_exact: 10.0,
            mean_field_match_pct: 70.0,
            mean_field_match_vs_majority_pct: -10.0,
            mean_field_balanced_accuracy_pct: 50.0,
            mean_field_macro_f1_pct: 45.0,
            mean_field_f1_newline_pct: 12.0,
        }];
        let encoded = encode_law_dictionary(&laws);
        assert!(!encoded.is_empty());
        assert_eq!(law_ids.len(), 1);
    }

    #[test]
    fn window_accounting_detects_overlap() {
        let windows = vec![(0usize, 10usize), (5usize, 15usize), (20usize, 30usize)];
        let accounting = compute_window_accounting(&windows);
        assert_eq!(accounting.total_window_span_bytes, 30);
        assert_eq!(accounting.coverage_bytes, 25);
        assert_eq!(accounting.overlap_bytes, 5);
        assert!(!accounting.honest_non_overlapping);
    }

    #[test]
    fn window_accounting_reports_non_overlap_as_honest() {
        let windows = vec![(0usize, 10usize), (10usize, 20usize), (20usize, 30usize)];
        let accounting = compute_window_accounting(&windows);
        assert_eq!(accounting.total_window_span_bytes, 30);
        assert_eq!(accounting.coverage_bytes, 30);
        assert_eq!(accounting.overlap_bytes, 0);
        assert!(accounting.honest_non_overlapping);
    }

}