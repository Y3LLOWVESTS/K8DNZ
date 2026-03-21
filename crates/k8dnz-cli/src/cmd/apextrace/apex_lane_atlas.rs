use anyhow::{anyhow, Context, Result};
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::cmd::apextrace::{ApexLaneAtlasArgs, ChunkSearchObjective, RenderFormat};

use super::common::write_or_print;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct KnobTuple {
    chunk_bytes: usize,
    boundary_band: usize,
    field_margin: u64,
    newline_demote_margin: u64,
    chunk_search_objective: String,
    chunk_raw_slack: u64,
}

#[derive(Clone, Debug)]
struct WindowRow {
    window_idx: usize,
    start: usize,
    end: usize,
    span_bytes: usize,
    elapsed_ms: u128,
    law_id: String,
    knob: KnobTuple,
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
    mean_compact_field_total_payload_exact: f64,
    mean_field_match_pct: f64,
    mean_field_balanced_accuracy_pct: f64,
    mean_field_macro_f1_pct: f64,
    mean_field_f1_newline_pct: f64,
}

#[derive(Clone, Debug)]
struct LawSummary {
    law_id: String,
    knob: KnobTuple,
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
struct AtlasSummary {
    input: String,
    recipe: String,
    input_bytes: usize,
    window_bytes: usize,
    step_bytes: usize,
    windows_analyzed: usize,
    distinct_law_count: usize,
    segment_count: usize,
    law_switch_count: usize,
    total_elapsed_ms: u128,
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
    knob: KnobTuple,
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

pub fn run_apex_lane_atlas(args: ApexLaneAtlasArgs) -> Result<()> {
    if args.window_bytes == 0 {
        return Err(anyhow!("apex-lane-atlas requires --window-bytes >= 1"));
    }
    if args.step_bytes == 0 {
        return Err(anyhow!("apex-lane-atlas requires --step-bytes >= 1"));
    }
    if args.max_windows == 0 {
        return Err(anyhow!("apex-lane-atlas requires --max-windows >= 1"));
    }

    let input = fs::read(&args.r#in).with_context(|| format!("read {}", args.r#in))?;
    let windows = build_windows(input.len(), args.window_bytes, args.step_bytes, args.max_windows);
    if windows.is_empty() {
        return Err(anyhow!(
            "apex-lane-atlas found no windows: input_bytes={} window_bytes={} step_bytes={}",
            input.len(),
            args.window_bytes,
            args.step_bytes
        ));
    }

    let temp_dir = make_temp_dir("apex_lane_atlas")?;
    let exe = env::current_exe().context("resolve current executable for apex-lane-atlas")?;
    let started = Instant::now();
    let mut windows_out = Vec::with_capacity(windows.len());

    for (window_idx, (start, end)) in windows.iter().copied().enumerate() {
        let slice = &input[start..end];
        let window_path = temp_dir.join(format!("window_{:04}_{:08}_{:08}.bin", window_idx, start, end));
        fs::write(&window_path, slice)
            .with_context(|| format!("write window slice {}", window_path.display()))?;

        eprintln!(
            "apex-lane-atlas: start window_idx={} start={} end={} span_bytes={}",
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
            knob: parsed.knob,
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

        eprintln!(
            "apex-lane-atlas: done window_idx={} law=? chunk_bytes={} boundary_band={} field_margin={} newline_demote_margin={} payload={} match_pct={:.6} elapsed_ms={}",
            window_idx,
            windows_out.last().unwrap().knob.chunk_bytes,
            windows_out.last().unwrap().knob.boundary_band,
            windows_out.last().unwrap().knob.field_margin,
            windows_out.last().unwrap().knob.newline_demote_margin,
            windows_out.last().unwrap().compact_field_total_payload_exact,
            windows_out.last().unwrap().field_match_pct,
            elapsed_ms
        );
    }

    let law_id_map = assign_law_ids(&windows_out);
    for row in &mut windows_out {
        row.law_id = law_id_map
            .get(&row.knob)
            .cloned()
            .unwrap_or_else(|| "L?".to_string());
    }

    let segments = build_segments(&windows_out, args.merge_gap_bytes);
    let law_summaries = build_law_summaries(&windows_out, &segments);
    let summary = AtlasSummary {
        input: args.r#in.clone(),
        recipe: args.recipe.clone(),
        input_bytes: input.len(),
        window_bytes: args.window_bytes,
        step_bytes: args.step_bytes,
        windows_analyzed: windows_out.len(),
        distinct_law_count: law_summaries.len(),
        segment_count: segments.len(),
        law_switch_count: segments.len().saturating_sub(1),
        total_elapsed_ms: started.elapsed().as_millis(),
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
        RenderFormat::Txt => render_txt(&summary, &law_summaries, &segments, &windows_out),
        RenderFormat::Csv => render_csv(&summary, &law_summaries, &segments, &windows_out),
    };
    write_or_print(args.out.as_deref(), &body)?;

    if args.keep_temp_dir {
        eprintln!("apex-lane-atlas: temp_dir={}", temp_dir.display());
    } else if let Err(err) = fs::remove_dir_all(&temp_dir) {
        eprintln!(
            "apex-lane-atlas: warning could not remove temp_dir={} err={}",
            temp_dir.display(),
            err
        );
    }

    if let Some(path) = args.out.as_deref() {
        eprintln!(
            "apex-lane-atlas: out={} windows={} distinct_laws={} segments={} total_elapsed_ms={}",
            path,
            summary.windows_analyzed,
            summary.distinct_law_count,
            summary.segment_count,
            summary.total_elapsed_ms
        );
    } else {
        eprintln!(
            "apex-lane-atlas: windows={} distinct_laws={} segments={} total_elapsed_ms={}",
            summary.windows_analyzed,
            summary.distinct_law_count,
            summary.segment_count,
            summary.total_elapsed_ms
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

fn run_child_apex_map_lane(
    exe: &Path,
    args: &ApexLaneAtlasArgs,
    window_path: &Path,
) -> Result<std::process::Output> {
    let first_chunk = first_csv_token_usize(&args.chunk_sweep, "chunk_sweep")?;
    let first_band = first_csv_token_usize(&args.boundary_band_sweep, "boundary_band_sweep")?;
    let first_margin = first_csv_token_u64(&args.field_margin_sweep, "field_margin_sweep")?;
    let first_demote = first_csv_token_u64(&args.newline_demote_margin_sweep, "newline_demote_margin_sweep")?;

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
            if args.newline_only_from_spacelike { "true" } else { "false" }
        ))
        .arg("--format")
        .arg("txt");

    if args.field_from_global {
        cmd.arg("--field-from-global");
    }

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

fn assign_law_ids(rows: &[WindowRow]) -> BTreeMap<KnobTuple, String> {
    let mut ordered = BTreeMap::<KnobTuple, String>::new();
    for row in rows {
        if !ordered.contains_key(&row.knob) {
            let id = format!("L{}", ordered.len());
            ordered.insert(row.knob.clone(), id);
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
    let mut cur_windows = vec![rows[0].clone()];

    for row in rows.iter().skip(1) {
        if row.law_id == cur_law && row.start <= cur_end.saturating_add(merge_gap_bytes) {
            cur_end = cur_end.max(row.end);
            cur_windows.push(row.clone());
        } else {
            out.push(finish_segment(out.len(), &cur_law, cur_start, cur_end, &cur_windows));
            cur_law = row.law_id.clone();
            cur_start = row.start;
            cur_end = row.end;
            cur_windows.clear();
            cur_windows.push(row.clone());
        }
    }

    out.push(finish_segment(out.len(), &cur_law, cur_start, cur_end, &cur_windows));
    out
}

fn finish_segment(
    segment_idx: usize,
    law_id: &str,
    start: usize,
    end: usize,
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
            knob: law_rows[0].knob.clone(),
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

    Ok(ParsedBestRow {
        knob: KnobTuple {
            chunk_bytes: parse_required_usize(&map, "chunk_bytes")?,
            boundary_band: parse_required_usize(&map, "boundary_band")?,
            field_margin: parse_required_u64(&map, "field_margin")?,
            newline_demote_margin: parse_required_u64(&map, "newline_demote_margin")?,
            chunk_search_objective: parse_required_string(&map, "chunk_search_objective")?,
            chunk_raw_slack: parse_required_u64(&map, "chunk_raw_slack")?,
        },
        compact_field_total_payload_exact: parse_required_usize(&map, "compact_field_total_payload_exact")?,
        field_patch_bytes: parse_required_usize(&map, "field_patch_bytes")?,
        field_match_pct: parse_required_f64(&map, "field_match_pct")?,
        majority_baseline_match_pct: parse_required_f64(&map, "majority_baseline_match_pct")?,
        field_match_vs_majority_pct: parse_required_f64(&map, "field_match_vs_majority_pct")?,
        field_balanced_accuracy_pct: parse_required_f64(&map, "field_balanced_accuracy_pct")?,
        field_macro_f1_pct: parse_required_f64(&map, "field_macro_f1_pct")?,
        field_f1_newline_pct: parse_required_f64(&map, "field_f1_newline_pct")?,
        field_pred_dominant_label: parse_required_string(&map, "field_pred_dominant_label")?,
        field_pred_dominant_share_pct: parse_required_f64(&map, "field_pred_dominant_share_pct")?,
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
    fs::create_dir_all(&dir)
        .with_context(|| format!("create temp dir {}", dir.display()))?;
    Ok(dir)
}

fn render_txt(
    summary: &AtlasSummary,
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
    line!("distinct_law_count", summary.distinct_law_count);
    line!("segment_count", summary.segment_count);
    line!("law_switch_count", summary.law_switch_count);
    line!("total_elapsed_ms", summary.total_elapsed_ms);
    line!("seed_from", summary.seed_from);
    line!("seed_count", summary.seed_count);
    line!("seed_step", summary.seed_step);
    line!("recipe_seed", summary.recipe_seed);
    line!("chunk_sweep", summary.chunk_sweep.clone());
    line!("boundary_band_sweep", summary.boundary_band_sweep.clone());
    line!("field_margin_sweep", summary.field_margin_sweep.clone());
    line!("newline_demote_margin_sweep", summary.newline_demote_margin_sweep.clone());
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
            "law_id={} chunk_bytes={} boundary_band={} field_margin={} newline_demote_margin={} chunk_search_objective={} chunk_raw_slack={} window_count={} segment_count={} covered_bytes={} mean_compact_field_total_payload_exact={:.3} mean_field_match_pct={:.6} mean_field_match_vs_majority_pct={:.6} mean_field_balanced_accuracy_pct={:.6} mean_field_macro_f1_pct={:.6} mean_field_f1_newline_pct={:.6}\n",
            law.law_id,
            law.knob.chunk_bytes,
            law.knob.boundary_band,
            law.knob.field_margin,
            law.knob.newline_demote_margin,
            law.knob.chunk_search_objective,
            law.knob.chunk_raw_slack,
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
            "segment_idx={} law_id={} start={} end={} span_bytes={} window_count={} mean_compact_field_total_payload_exact={:.3} mean_field_match_pct={:.6} mean_field_balanced_accuracy_pct={:.6} mean_field_macro_f1_pct={:.6} mean_field_f1_newline_pct={:.6}\n",
            seg.segment_idx,
            seg.law_id,
            seg.start,
            seg.end,
            seg.span_bytes,
            seg.window_count,
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
            "window_idx={} law_id={} start={} end={} span_bytes={} elapsed_ms={} chunk_bytes={} boundary_band={} field_margin={} newline_demote_margin={} compact_field_total_payload_exact={} field_patch_bytes={} field_match_pct={:.6} majority_baseline_match_pct={:.6} field_match_vs_majority_pct={:.6} field_balanced_accuracy_pct={:.6} field_macro_f1_pct={:.6} field_f1_newline_pct={:.6} field_pred_dominant_label={} field_pred_dominant_share_pct={:.6} field_pred_collapse_90_flag={} field_pred_newline_delta={} field_newline_demoted={} field_newline_after_demote={} field_newline_floor_used={} field_newline_extinct_flag={}\n",
            row.window_idx,
            row.law_id,
            row.start,
            row.end,
            row.span_bytes,
            row.elapsed_ms,
            row.knob.chunk_bytes,
            row.knob.boundary_band,
            row.knob.field_margin,
            row.knob.newline_demote_margin,
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
    summary: &AtlasSummary,
    laws: &[LawSummary],
    segments: &[SegmentRow],
    windows: &[WindowRow],
) -> String {
    let header = [
        "row_kind",
        "id",
        "law_id",
        "input",
        "recipe",
        "input_bytes",
        "window_bytes",
        "step_bytes",
        "windows_analyzed",
        "distinct_law_count",
        "segment_count",
        "law_switch_count",
        "total_elapsed_ms",
        "start",
        "end",
        "span_bytes",
        "window_count",
        "covered_bytes",
        "elapsed_ms",
        "chunk_bytes",
        "boundary_band",
        "field_margin",
        "newline_demote_margin",
        "chunk_search_objective",
        "chunk_raw_slack",
        "compact_field_total_payload_exact",
        "field_patch_bytes",
        "field_match_pct",
        "majority_baseline_match_pct",
        "field_match_vs_majority_pct",
        "field_balanced_accuracy_pct",
        "field_macro_f1_pct",
        "field_f1_newline_pct",
        "field_pred_dominant_label",
        "field_pred_dominant_share_pct",
        "field_pred_collapse_90_flag",
        "field_pred_newline_delta",
        "field_newline_demoted",
        "field_newline_after_demote",
        "field_newline_floor_used",
        "field_newline_extinct_flag",
        "mean_compact_field_total_payload_exact",
        "mean_field_match_pct",
        "mean_field_match_vs_majority_pct",
        "mean_field_balanced_accuracy_pct",
        "mean_field_macro_f1_pct",
        "mean_field_f1_newline_pct",
    ];

    let mut out = String::new();
    push_csv_row(&mut out, &header.iter().map(|s| s.to_string()).collect::<Vec<_>>());

    push_csv_row(
        &mut out,
        &vec![
            "summary".to_string(),
            "summary".to_string(),
            "".to_string(),
            summary.input.clone(),
            summary.recipe.clone(),
            summary.input_bytes.to_string(),
            summary.window_bytes.to_string(),
            summary.step_bytes.to_string(),
            summary.windows_analyzed.to_string(),
            summary.distinct_law_count.to_string(),
            summary.segment_count.to_string(),
            summary.law_switch_count.to_string(),
            summary.total_elapsed_ms.to_string(),
            blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(),
        ],
    );

    for law in laws {
        push_csv_row(
            &mut out,
            &vec![
                "law".to_string(),
                law.law_id.clone(),
                law.law_id.clone(),
                blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(),
                blank(), blank(), blank(),
                law.window_count.to_string(),
                law.covered_bytes.to_string(),
                blank(),
                law.knob.chunk_bytes.to_string(),
                law.knob.boundary_band.to_string(),
                law.knob.field_margin.to_string(),
                law.knob.newline_demote_margin.to_string(),
                law.knob.chunk_search_objective.clone(),
                law.knob.chunk_raw_slack.to_string(),
                blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(),
                format!("{:.3}", law.mean_compact_field_total_payload_exact),
                format!("{:.6}", law.mean_field_match_pct),
                format!("{:.6}", law.mean_field_match_vs_majority_pct),
                format!("{:.6}", law.mean_field_balanced_accuracy_pct),
                format!("{:.6}", law.mean_field_macro_f1_pct),
                format!("{:.6}", law.mean_field_f1_newline_pct),
            ],
        );
    }

    for seg in segments {
        push_csv_row(
            &mut out,
            &vec![
                "segment".to_string(),
                seg.segment_idx.to_string(),
                seg.law_id.clone(),
                blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(),
                seg.start.to_string(),
                seg.end.to_string(),
                seg.span_bytes.to_string(),
                seg.window_count.to_string(),
                blank(),
                blank(),
                blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(),
                format!("{:.3}", seg.mean_compact_field_total_payload_exact),
                format!("{:.6}", seg.mean_field_match_pct),
                blank(),
                format!("{:.6}", seg.mean_field_balanced_accuracy_pct),
                format!("{:.6}", seg.mean_field_macro_f1_pct),
                format!("{:.6}", seg.mean_field_f1_newline_pct),
            ],
        );
    }

    for row in windows {
        push_csv_row(
            &mut out,
            &vec![
                "window".to_string(),
                row.window_idx.to_string(),
                row.law_id.clone(),
                blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(), blank(),
                row.start.to_string(),
                row.end.to_string(),
                row.span_bytes.to_string(),
                blank(),
                blank(),
                row.elapsed_ms.to_string(),
                row.knob.chunk_bytes.to_string(),
                row.knob.boundary_band.to_string(),
                row.knob.field_margin.to_string(),
                row.knob.newline_demote_margin.to_string(),
                row.knob.chunk_search_objective.clone(),
                row.knob.chunk_raw_slack.to_string(),
                row.compact_field_total_payload_exact.to_string(),
                row.field_patch_bytes.to_string(),
                format!("{:.6}", row.field_match_pct),
                format!("{:.6}", row.majority_baseline_match_pct),
                format!("{:.6}", row.field_match_vs_majority_pct),
                format!("{:.6}", row.field_balanced_accuracy_pct),
                format!("{:.6}", row.field_macro_f1_pct),
                format!("{:.6}", row.field_f1_newline_pct),
                row.field_pred_dominant_label.clone(),
                format!("{:.6}", row.field_pred_dominant_share_pct),
                row.field_pred_collapse_90_flag.to_string(),
                row.field_pred_newline_delta.to_string(),
                row.field_newline_demoted.to_string(),
                row.field_newline_after_demote.to_string(),
                row.field_newline_floor_used.to_string(),
                row.field_newline_extinct_flag.to_string(),
                blank(), blank(), blank(), blank(), blank(), blank(),
            ],
        );
    }

    out
}

fn push_csv_row(out: &mut String, cells: &[String]) {
    let escaped = cells.iter().map(|s| csv_escape(s)).collect::<Vec<_>>();
    out.push_str(&escaped.join(","));
    out.push('\n');
}

fn blank() -> String {
    String::new()
}

fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}
