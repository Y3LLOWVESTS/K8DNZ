use anyhow::{anyhow, Context, Result};
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::cmd::apextrace::{ApexPunctKindAtlasArgs, ChunkSearchObjective, RenderFormat};

use super::common::write_or_print;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct SearchKnobTuple {
    chunk_bytes: usize,
    boundary_band: usize,
    field_margin: u64,
    chunk_search_objective: String,
    chunk_raw_slack: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ReplayLawTuple {
    boundary_band: usize,
    field_margin: u64,
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
    field_total_payload_exact: usize,
    field_patch_bytes: usize,
    field_match_pct: f64,
    field_balanced_accuracy_pct: f64,
    field_macro_f1_pct: f64,
    field_non_majority_macro_f1_pct: f64,
    codec_recommendation: String,
    frontier_recommendation: String,
}

#[derive(Clone, Debug)]
struct SegmentRow {
    segment_idx: usize,
    law_id: String,
    start: usize,
    end: usize,
    span_bytes: usize,
    window_count: usize,
    mean_field_total_payload_exact: f64,
    mean_field_match_pct: f64,
    mean_field_balanced_accuracy_pct: f64,
    mean_field_macro_f1_pct: f64,
    mean_field_non_majority_macro_f1_pct: f64,
}

#[derive(Clone, Debug)]
struct LawSummary {
    law_id: String,
    law: ReplayLawTuple,
    window_count: usize,
    segment_count: usize,
    covered_bytes: usize,
    mean_field_total_payload_exact: f64,
    mean_field_match_pct: f64,
    mean_field_balanced_accuracy_pct: f64,
    mean_field_macro_f1_pct: f64,
    mean_field_non_majority_macro_f1_pct: f64,
}

#[derive(Clone, Debug)]
struct AtlasSummary {
    input: String,
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
    term_margin_add: u64,
    pause_margin_add: u64,
    wrap_margin_add: u64,
    term_share_ppm_min: u32,
    pause_share_ppm_min: u32,
    wrap_share_ppm_min: u32,
}

#[derive(Clone, Debug)]
struct ParsedBestRow {
    search: SearchKnobTuple,
    law: ReplayLawTuple,
    field_total_payload_exact: usize,
    field_patch_bytes: usize,
    field_match_pct: f64,
    field_balanced_accuracy_pct: f64,
    field_macro_f1_pct: f64,
    field_non_majority_macro_f1_pct: f64,
    codec_recommendation: String,
    frontier_recommendation: String,
}

pub fn run_apex_punct_kind_atlas(args: ApexPunctKindAtlasArgs) -> Result<()> {
    if args.window_bytes == 0 {
        return Err(anyhow!("apex-punct-kind-atlas requires --window-bytes >= 1"));
    }
    if args.step_bytes == 0 {
        return Err(anyhow!("apex-punct-kind-atlas requires --step-bytes >= 1"));
    }
    if args.max_windows == 0 {
        return Err(anyhow!("apex-punct-kind-atlas requires --max-windows >= 1"));
    }

    let input = fs::read(&args.r#in).with_context(|| format!("read {}", args.r#in))?;
    let windows = build_windows(input.len(), args.window_bytes, args.step_bytes, args.max_windows);
    if windows.is_empty() {
        return Err(anyhow!(
            "apex-punct-kind-atlas found no windows: input_bytes={} window_bytes={} step_bytes={}",
            input.len(),
            args.window_bytes,
            args.step_bytes
        ));
    }

    let temp_dir = make_temp_dir("apex_punct_kind_atlas")?;
    let exe = env::current_exe().context("resolve current executable for apex-punct-kind-atlas")?;
    let started = Instant::now();
    let mut rows = Vec::with_capacity(windows.len());

    for (window_idx, (start, end)) in windows.iter().copied().enumerate() {
        let slice = &input[start..end];
        let window_path = temp_dir.join(format!("window_{:04}_{:08}_{:08}.bin", window_idx, start, end));
        fs::write(&window_path, slice)
            .with_context(|| format!("write punct-kind window slice {}", window_path.display()))?;

        eprintln!(
            "apex-punct-kind-atlas: start window_idx={} start={} end={} span_bytes={}",
            window_idx,
            start,
            end,
            end.saturating_sub(start)
        );

        let child_started = Instant::now();
        let output = run_child_apex_map_punct_kind(&exe, &args, &window_path)?;
        let elapsed_ms = child_started.elapsed().as_millis();
        let parsed = parse_best_line(&output.stderr).with_context(|| {
            format!(
                "parse apex-map-punct-kind best-field line for window_idx={} start={} end={}",
                window_idx, start, end
            )
        })?;

        rows.push(WindowRow {
            window_idx,
            start,
            end,
            span_bytes: end.saturating_sub(start),
            elapsed_ms,
            law_id: String::new(),
            search: parsed.search.clone(),
            law: parsed.law.clone(),
            field_total_payload_exact: parsed.field_total_payload_exact,
            field_patch_bytes: parsed.field_patch_bytes,
            field_match_pct: parsed.field_match_pct,
            field_balanced_accuracy_pct: parsed.field_balanced_accuracy_pct,
            field_macro_f1_pct: parsed.field_macro_f1_pct,
            field_non_majority_macro_f1_pct: parsed.field_non_majority_macro_f1_pct,
            codec_recommendation: parsed.codec_recommendation,
            frontier_recommendation: parsed.frontier_recommendation,
        });

        eprintln!(
            "apex-punct-kind-atlas: done window_idx={} law=? chunk_bytes={} boundary_band={} field_margin={} field_total_payload_exact={} field_match_pct={:.6} elapsed_ms={}",
            window_idx,
            rows.last().unwrap().search.chunk_bytes,
            rows.last().unwrap().search.boundary_band,
            rows.last().unwrap().search.field_margin,
            rows.last().unwrap().field_total_payload_exact,
            rows.last().unwrap().field_match_pct,
            elapsed_ms
        );
    }

    let law_ids = assign_law_ids(&rows);
    for row in &mut rows {
        row.law_id = law_ids
            .get(&row.law)
            .cloned()
            .unwrap_or_else(|| "P?".to_string());
    }

    let segments = build_segments(&rows, args.merge_gap_bytes);
    let laws = build_law_summaries(&rows, &segments);
    let summary = AtlasSummary {
        input: args.r#in.clone(),
        input_bytes: input.len(),
        window_bytes: args.window_bytes,
        step_bytes: args.step_bytes,
        windows_analyzed: rows.len(),
        distinct_law_count: laws.len(),
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
        term_margin_add: args.term_margin_add,
        pause_margin_add: args.pause_margin_add,
        wrap_margin_add: args.wrap_margin_add,
        term_share_ppm_min: args.term_share_ppm_min,
        pause_share_ppm_min: args.pause_share_ppm_min,
        wrap_share_ppm_min: args.wrap_share_ppm_min,
    };

    let body = match args.format {
        RenderFormat::Txt => render_txt(&summary, &laws, &segments, &rows),
        RenderFormat::Csv => render_csv(&summary, &laws, &segments, &rows),
    };
    write_or_print(args.out.as_deref(), &body)?;

    if args.keep_temp_dir {
        eprintln!("apex-punct-kind-atlas: temp_dir={}", temp_dir.display());
    } else if let Err(err) = fs::remove_dir_all(&temp_dir) {
        eprintln!(
            "apex-punct-kind-atlas: warning could not remove temp_dir={} err={}",
            temp_dir.display(),
            err
        );
    }

    Ok(())
}

fn run_child_apex_map_punct_kind(
    exe: &Path,
    args: &ApexPunctKindAtlasArgs,
    window_path: &Path,
) -> Result<std::process::Output> {
    let first_chunk = first_csv_token_usize(&args.chunk_sweep, "chunk_sweep")?;
    let first_band = first_csv_token_usize(&args.boundary_band_sweep, "boundary_band_sweep")?;
    let first_margin = first_csv_token_u64(&args.field_margin_sweep, "field_margin_sweep")?;

    let mut cmd = Command::new(exe);
    cmd.arg("apextrace")
        .arg("apex-map-punct-kind")
        .arg("--in")
        .arg(window_path)
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
        .arg("--term-margin-add")
        .arg(args.term_margin_add.to_string())
        .arg("--pause-margin-add")
        .arg(args.pause_margin_add.to_string())
        .arg("--wrap-margin-add")
        .arg(args.wrap_margin_add.to_string())
        .arg("--term-share-ppm-min")
        .arg(args.term_share_ppm_min.to_string())
        .arg("--pause-share-ppm-min")
        .arg(args.pause_share_ppm_min.to_string())
        .arg("--wrap-share-ppm-min")
        .arg(args.wrap_share_ppm_min.to_string())
        .arg("--format")
        .arg("txt");

    if args.field_from_global {
        cmd.arg("--field-from-global");
    }

    let output = cmd
        .output()
        .with_context(|| format!("spawn child apex-map-punct-kind for {}", window_path.display()))?;

    if !output.status.success() {
        return Err(anyhow!(
            "child apex-map-punct-kind failed status={} window={} stderr={} stdout={}",
            output.status,
            window_path.display(),
            truncate_for_error(&String::from_utf8_lossy(&output.stderr)),
            truncate_for_error(&String::from_utf8_lossy(&output.stdout)),
        ));
    }

    Ok(output)
}

fn build_windows(input_len: usize, window_bytes: usize, step_bytes: usize, max_windows: usize) -> Vec<(usize, usize)> {
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

fn assign_law_ids(rows: &[WindowRow]) -> BTreeMap<ReplayLawTuple, String> {
    let mut out = BTreeMap::<ReplayLawTuple, String>::new();
    for row in rows {
        if !out.contains_key(&row.law) {
            let id = format!("P{}", out.len());
            out.insert(row.law.clone(), id);
        }
    }
    out
}

fn build_segments(rows: &[WindowRow], merge_gap_bytes: usize) -> Vec<SegmentRow> {
    if rows.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut cur_law = rows[0].law_id.clone();
    let mut cur_start = rows[0].start;
    let mut cur_end = rows[0].end;
    let mut cur = vec![rows[0].clone()];

    for row in rows.iter().skip(1) {
        if row.law_id == cur_law && row.start <= cur_end.saturating_add(merge_gap_bytes) {
            cur_end = cur_end.max(row.end);
            cur.push(row.clone());
        } else {
            out.push(finish_segment(out.len(), &cur_law, cur_start, cur_end, &cur));
            cur_law = row.law_id.clone();
            cur_start = row.start;
            cur_end = row.end;
            cur.clear();
            cur.push(row.clone());
        }
    }
    out.push(finish_segment(out.len(), &cur_law, cur_start, cur_end, &cur));
    out
}

fn finish_segment(segment_idx: usize, law_id: &str, start: usize, end: usize, rows: &[WindowRow]) -> SegmentRow {
    let denom = rows.len().max(1) as f64;
    SegmentRow {
        segment_idx,
        law_id: law_id.to_string(),
        start,
        end,
        span_bytes: end.saturating_sub(start),
        window_count: rows.len(),
        mean_field_total_payload_exact: rows.iter().map(|r| r.field_total_payload_exact as f64).sum::<f64>() / denom,
        mean_field_match_pct: rows.iter().map(|r| r.field_match_pct).sum::<f64>() / denom,
        mean_field_balanced_accuracy_pct: rows.iter().map(|r| r.field_balanced_accuracy_pct).sum::<f64>() / denom,
        mean_field_macro_f1_pct: rows.iter().map(|r| r.field_macro_f1_pct).sum::<f64>() / denom,
        mean_field_non_majority_macro_f1_pct: rows.iter().map(|r| r.field_non_majority_macro_f1_pct).sum::<f64>() / denom,
    }
}

fn build_law_summaries(rows: &[WindowRow], segments: &[SegmentRow]) -> Vec<LawSummary> {
    let mut by_law = BTreeMap::<String, Vec<&WindowRow>>::new();
    for row in rows {
        by_law.entry(row.law_id.clone()).or_default().push(row);
    }
    let mut covered = BTreeMap::<String, usize>::new();
    let mut seg_counts = BTreeMap::<String, usize>::new();
    for seg in segments {
        *covered.entry(seg.law_id.clone()).or_default() += seg.span_bytes;
        *seg_counts.entry(seg.law_id.clone()).or_default() += 1;
    }
    let mut out = Vec::new();
    for (law_id, law_rows) in by_law {
        let denom = law_rows.len().max(1) as f64;
        out.push(LawSummary {
            law_id: law_id.clone(),
            law: law_rows[0].law.clone(),
            window_count: law_rows.len(),
            segment_count: *seg_counts.get(&law_id).unwrap_or(&0),
            covered_bytes: *covered.get(&law_id).unwrap_or(&0),
            mean_field_total_payload_exact: law_rows.iter().map(|r| r.field_total_payload_exact as f64).sum::<f64>() / denom,
            mean_field_match_pct: law_rows.iter().map(|r| r.field_match_pct).sum::<f64>() / denom,
            mean_field_balanced_accuracy_pct: law_rows.iter().map(|r| r.field_balanced_accuracy_pct).sum::<f64>() / denom,
            mean_field_macro_f1_pct: law_rows.iter().map(|r| r.field_macro_f1_pct).sum::<f64>() / denom,
            mean_field_non_majority_macro_f1_pct: law_rows.iter().map(|r| r.field_non_majority_macro_f1_pct).sum::<f64>() / denom,
        });
    }
    out
}

fn parse_best_line(stderr: &[u8]) -> Result<ParsedBestRow> {
    let stderr = String::from_utf8_lossy(stderr);
    let prefix = "apextrace apex-map-punct-kind best-field:";
    let line = stderr
        .lines()
        .rev()
        .find(|line| line.trim_start().starts_with(prefix))
        .ok_or_else(|| anyhow!("missing best-field line in apex-map-punct-kind stderr"))?;
    let payload = line
        .trim_start()
        .strip_prefix(prefix)
        .ok_or_else(|| anyhow!("apex-map-punct-kind best-field line missing prefix"))?
        .trim();
    let map = tokenize_kv_line(payload);
    Ok(ParsedBestRow {
        search: SearchKnobTuple {
            chunk_bytes: parse_required_usize(&map, "chunk_bytes")?,
            boundary_band: parse_required_usize(&map, "boundary_band")?,
            field_margin: parse_required_u64(&map, "field_margin")?,
            chunk_search_objective: parse_required_string(&map, "chunk_search_objective")?,
            chunk_raw_slack: parse_required_u64(&map, "chunk_raw_slack")?,
        },
        law: ReplayLawTuple {
            boundary_band: parse_required_usize(&map, "boundary_band")?,
            field_margin: parse_required_u64(&map, "field_margin")?,
        },
        field_total_payload_exact: parse_required_usize(&map, "field_total_payload_exact")?,
        field_patch_bytes: parse_required_usize(&map, "field_patch_bytes")?,
        field_match_pct: parse_required_f64(&map, "field_match_pct")?,
        field_balanced_accuracy_pct: parse_required_f64(&map, "field_balanced_accuracy_pct")?,
        field_macro_f1_pct: parse_required_f64(&map, "field_macro_f1_pct")?,
        field_non_majority_macro_f1_pct: parse_required_f64(&map, "field_non_majority_macro_f1_pct")?,
        codec_recommendation: parse_required_string(&map, "codec_recommendation")?,
        frontier_recommendation: parse_required_string(&map, "frontier_recommendation")?,
    })
}

fn tokenize_kv_line(line: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for token in line.split_whitespace() {
        if let Some((k, v)) = token.split_once('=') {
            out.insert(k.to_string(), v.to_string());
        }
    }
    out
}

fn parse_required_string(map: &BTreeMap<String, String>, key: &str) -> Result<String> {
    map.get(key)
        .cloned()
        .ok_or_else(|| anyhow!("missing key {}", key))
}
fn parse_required_usize(map: &BTreeMap<String, String>, key: &str) -> Result<usize> {
    parse_required_string(map, key)?.parse::<usize>().with_context(|| format!("parse usize {}", key))
}
fn parse_required_u64(map: &BTreeMap<String, String>, key: &str) -> Result<u64> {
    parse_required_string(map, key)?.parse::<u64>().with_context(|| format!("parse u64 {}", key))
}
fn parse_required_f64(map: &BTreeMap<String, String>, key: &str) -> Result<f64> {
    parse_required_string(map, key)?.parse::<f64>().with_context(|| format!("parse f64 {}", key))
}

fn first_csv_token_usize(raw: &str, label: &str) -> Result<usize> {
    let token = raw.split(',').map(|v| v.trim()).find(|v| !v.is_empty()).ok_or_else(|| anyhow!("{} requires at least one value", label))?;
    token.parse::<usize>().with_context(|| format!("parse {} first token {} as usize", label, token))
}
fn first_csv_token_u64(raw: &str, label: &str) -> Result<u64> {
    let token = raw.split(',').map(|v| v.trim()).find(|v| !v.is_empty()).ok_or_else(|| anyhow!("{} requires at least one value", label))?;
    token.parse::<u64>().with_context(|| format!("parse {} first token {} as u64", label, token))
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
    if s.len() <= LIMIT { s.to_string() } else { format!("{}...", &s[..LIMIT]) }
}

fn make_temp_dir(prefix: &str) -> Result<PathBuf> {
    let stamp = SystemTime::now().duration_since(UNIX_EPOCH).context("system time before unix epoch")?.as_nanos();
    let dir = env::temp_dir().join(format!("{}_{}_{}", prefix, std::process::id(), stamp));
    fs::create_dir_all(&dir).with_context(|| format!("create temp dir {}", dir.display()))?;
    Ok(dir)
}

fn render_txt(summary: &AtlasSummary, laws: &[LawSummary], segments: &[SegmentRow], rows: &[WindowRow]) -> String {
    let mut out = String::new();
    macro_rules! line { ($k:expr, $v:expr) => {{ out.push_str($k); out.push('='); out.push_str(&$v.to_string()); out.push('\n'); }}; }
    line!("input", summary.input.clone());
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
    line!("term_margin_add", summary.term_margin_add);
    line!("pause_margin_add", summary.pause_margin_add);
    line!("wrap_margin_add", summary.wrap_margin_add);
    line!("term_share_ppm_min", summary.term_share_ppm_min);
    line!("pause_share_ppm_min", summary.pause_share_ppm_min);
    line!("wrap_share_ppm_min", summary.wrap_share_ppm_min);
    line!("law_path", rows.iter().map(|r| r.law_id.as_str()).collect::<Vec<_>>().join(","));

    out.push_str("\n--- laws ---\n");
    for law in laws {
        out.push_str(&format!(
            "law_id={} boundary_band={} field_margin={} window_count={} segment_count={} covered_bytes={} mean_field_total_payload_exact={:.6} mean_field_match_pct={:.6} mean_field_balanced_accuracy_pct={:.6} mean_field_macro_f1_pct={:.6} mean_field_non_majority_macro_f1_pct={:.6}\n",
            law.law_id,
            law.law.boundary_band,
            law.law.field_margin,
            law.window_count,
            law.segment_count,
            law.covered_bytes,
            law.mean_field_total_payload_exact,
            law.mean_field_match_pct,
            law.mean_field_balanced_accuracy_pct,
            law.mean_field_macro_f1_pct,
            law.mean_field_non_majority_macro_f1_pct,
        ));
    }

    out.push_str("\n--- segments ---\n");
    for seg in segments {
        out.push_str(&format!(
            "segment_idx={} law_id={} start={} end={} span_bytes={} window_count={} mean_field_total_payload_exact={:.6} mean_field_match_pct={:.6} mean_field_balanced_accuracy_pct={:.6} mean_field_macro_f1_pct={:.6} mean_field_non_majority_macro_f1_pct={:.6}\n",
            seg.segment_idx,
            seg.law_id,
            seg.start,
            seg.end,
            seg.span_bytes,
            seg.window_count,
            seg.mean_field_total_payload_exact,
            seg.mean_field_match_pct,
            seg.mean_field_balanced_accuracy_pct,
            seg.mean_field_macro_f1_pct,
            seg.mean_field_non_majority_macro_f1_pct,
        ));
    }

    out.push_str("\n--- windows ---\n");
    for row in rows {
        out.push_str(&format!(
            "window_idx={} law_id={} start={} end={} span_bytes={} elapsed_ms={} chunk_bytes={} boundary_band={} field_margin={} chunk_search_objective={} chunk_raw_slack={} field_total_payload_exact={} field_patch_bytes={} field_match_pct={:.6} field_balanced_accuracy_pct={:.6} field_macro_f1_pct={:.6} field_non_majority_macro_f1_pct={:.6} codec_recommendation={} frontier_recommendation={}\n",
            row.window_idx,
            row.law_id,
            row.start,
            row.end,
            row.span_bytes,
            row.elapsed_ms,
            row.search.chunk_bytes,
            row.search.boundary_band,
            row.search.field_margin,
            row.search.chunk_search_objective,
            row.search.chunk_raw_slack,
            row.field_total_payload_exact,
            row.field_patch_bytes,
            row.field_match_pct,
            row.field_balanced_accuracy_pct,
            row.field_macro_f1_pct,
            row.field_non_majority_macro_f1_pct,
            row.codec_recommendation,
            row.frontier_recommendation,
        ));
    }
    out
}

fn render_csv(summary: &AtlasSummary, laws: &[LawSummary], segments: &[SegmentRow], rows: &[WindowRow]) -> String {
    let mut out = String::new();
    push_csv_row(&mut out, &[
        "row_kind","id","law_id","input","input_bytes","window_bytes","step_bytes","windows_analyzed","distinct_law_count","segment_count","law_switch_count","total_elapsed_ms","start","end","span_bytes","window_count","covered_bytes","elapsed_ms","chunk_bytes","boundary_band","field_margin","chunk_search_objective","chunk_raw_slack","field_total_payload_exact","field_patch_bytes","field_match_pct","field_balanced_accuracy_pct","field_macro_f1_pct","field_non_majority_macro_f1_pct","codec_recommendation","frontier_recommendation","mean_field_total_payload_exact","mean_field_match_pct","mean_field_balanced_accuracy_pct","mean_field_macro_f1_pct","mean_field_non_majority_macro_f1_pct"
    ]);
    push_csv_row(&mut out, &[
        "summary".to_string(),"summary".to_string(),String::new(),summary.input.clone(),summary.input_bytes.to_string(),summary.window_bytes.to_string(),summary.step_bytes.to_string(),summary.windows_analyzed.to_string(),summary.distinct_law_count.to_string(),summary.segment_count.to_string(),summary.law_switch_count.to_string(),summary.total_elapsed_ms.to_string(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new()
    ]);
    for law in laws {
        push_csv_row(&mut out, &[
            "law".to_string(),law.law_id.clone(),law.law_id.clone(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),law.window_count.to_string(),law.covered_bytes.to_string(),String::new(),String::new(),law.law.boundary_band.to_string(),law.law.field_margin.to_string(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),format!("{:.6}", law.mean_field_total_payload_exact),format!("{:.6}", law.mean_field_match_pct),format!("{:.6}", law.mean_field_balanced_accuracy_pct),format!("{:.6}", law.mean_field_macro_f1_pct),format!("{:.6}", law.mean_field_non_majority_macro_f1_pct)
        ]);
    }
    for seg in segments {
        push_csv_row(&mut out, &[
            "segment".to_string(),seg.segment_idx.to_string(),seg.law_id.clone(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),seg.start.to_string(),seg.end.to_string(),seg.span_bytes.to_string(),seg.window_count.to_string(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),format!("{:.6}", seg.mean_field_total_payload_exact),format!("{:.6}", seg.mean_field_match_pct),format!("{:.6}", seg.mean_field_balanced_accuracy_pct),format!("{:.6}", seg.mean_field_macro_f1_pct),format!("{:.6}", seg.mean_field_non_majority_macro_f1_pct)
        ]);
    }
    for row in rows {
        push_csv_row(&mut out, &[
            "window".to_string(),row.window_idx.to_string(),row.law_id.clone(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),row.start.to_string(),row.end.to_string(),row.span_bytes.to_string(),String::new(),String::new(),row.elapsed_ms.to_string(),row.search.chunk_bytes.to_string(),row.search.boundary_band.to_string(),row.search.field_margin.to_string(),row.search.chunk_search_objective.clone(),row.search.chunk_raw_slack.to_string(),row.field_total_payload_exact.to_string(),row.field_patch_bytes.to_string(),format!("{:.6}", row.field_match_pct),format!("{:.6}", row.field_balanced_accuracy_pct),format!("{:.6}", row.field_macro_f1_pct),format!("{:.6}", row.field_non_majority_macro_f1_pct),row.codec_recommendation.clone(),row.frontier_recommendation.clone(),String::new(),String::new(),String::new(),String::new(),String::new()
        ]);
    }
    out
}

fn push_csv_row(out: &mut String, cells: &[impl AsRef<str>]) {
    let escaped = cells.iter().map(|s| csv_escape(s.as_ref())).collect::<Vec<_>>();
    out.push_str(&escaped.join(","));
    out.push('\n');
}

fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}
