use anyhow::{anyhow, Context, Result};
use k8dnz_apextrace::{generate_bytes, ApexKey, ApexMap, ApexMapCfg, RefineCfg, SearchCfg};
use k8dnz_core::repr::{
    case_lanes::{case_label, CaseLanes},
    text_norm,
};
use k8dnz_core::symbol::patch::PatchList;

use crate::cmd::apextrace::{ApexMapCaseArgs, ChunkSearchObjective, RenderFormat};

use super::common::write_or_print;
use super::compact_manifest::{CompactChunkKey, CompactChunkManifest};
use super::symbol_metrics::{compute_symbol_metrics, SymbolMetrics};

const APEX_KEY_BYTES_EXACT: usize = 48;
const CLASS_COUNT: u8 = 2;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct LaneScore {
    matches: u64,
    prefix: u64,
    total: u64,
    longest_run: u64,
    longest_run_start: u64,
}

impl LaneScore {
    fn better_than(&self, other: &Self) -> bool {
        (self.matches, self.longest_run, self.prefix) > (other.matches, other.longest_run, other.prefix)
    }
}

#[derive(Clone, Debug)]
struct LaneBest {
    key: ApexKey,
    predicted: Vec<u8>,
    score: LaneScore,
}

#[derive(Clone, Debug)]
struct LaneChunkBest {
    chunk_index: usize,
    start: usize,
    end: usize,
    key: ApexKey,
    patch_entries: usize,
    patch_bytes: usize,
}

#[derive(Clone, Debug)]
struct LaneChunkedBest {
    chunk_bytes: usize,
    chunk_key_bytes_exact: usize,
    predicted: Vec<u8>,
    chunks: Vec<LaneChunkBest>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct ChunkSearchSummary {
    mean_balanced_accuracy_pct: f64,
    mean_macro_f1_pct: f64,
    mean_minority_f1_pct: f64,
    mean_abs_minority_delta: f64,
    majority_flip_count: usize,
    collapse_90_count: usize,
}

#[derive(Clone, Debug)]
struct ApexMapCaseReport {
    input: String,
    normalized_len: usize,
    letter_positions: usize,
    case_len: usize,

    majority_class: u8,
    majority_class_label: &'static str,
    majority_count: u64,
    majority_baseline_match_pct: f64,
    target_entropy_bits: f64,

    global_patch_entries: usize,
    global_patch_bytes: usize,
    global_total_payload_exact: usize,
    global_match_pct: f64,
    global_match_vs_majority_pct: f64,
    global_balanced_accuracy_pct: f64,
    global_macro_f1_pct: f64,

    chunk_bytes: usize,
    chunk_count: usize,
    chunk_patch_entries: usize,
    chunk_patch_bytes: usize,
    chunk_total_payload_exact: usize,
    compact_manifest_bytes_exact: usize,
    compact_chunk_total_payload_exact: usize,
    chunk_match_pct: f64,
    chunk_match_vs_majority_pct: f64,
    chunk_balanced_accuracy_pct: f64,
    chunk_macro_f1_pct: f64,
    chunk_search_objective: String,
    chunk_raw_slack: u64,
    chunk_mean_balanced_accuracy_pct: f64,
    chunk_mean_macro_f1_pct: f64,
    chunk_mean_minority_f1_pct: f64,
    chunk_mean_abs_minority_delta: f64,
    chunk_majority_flip_count: usize,
    chunk_collapse_90_count: usize,

    field_source: String,
    map_node_count: usize,
    map_depth_seen: u8,
    map_depth_shift: u8,
    map_max_depth_arg: u8,
    boundary_band: usize,
    boundary_delta: usize,
    field_margin: u64,
    lower_margin_add: u64,
    upper_margin_add: u64,
    lower_share_ppm_min: u32,
    upper_share_ppm_min: u32,

    field_patch_entries: usize,
    field_patch_bytes: usize,
    field_total_payload_exact: usize,
    compact_field_total_payload_exact: usize,
    field_match_pct: f64,
    field_match_vs_majority_pct: f64,
    field_balanced_accuracy_pct: f64,
    field_macro_precision_pct: f64,
    field_macro_recall_pct: f64,
    field_macro_f1_pct: f64,
    field_weighted_f1_pct: f64,
    field_pred_entropy_bits: f64,
    field_hist_l1_pct: f64,
    field_pred_dominant_class: u8,
    field_pred_dominant_label: &'static str,
    field_pred_dominant_share_ppm: u64,
    field_pred_dominant_share_pct: f64,
    field_pred_collapse_90_flag: bool,

    field_precision_lower_pct: f64,
    field_recall_lower_pct: f64,
    field_f1_lower_pct: f64,
    field_precision_upper_pct: f64,
    field_recall_upper_pct: f64,
    field_f1_upper_pct: f64,

    field_overrides: usize,
    field_boundary_count: usize,
    field_touched_positions: usize,
    field_lower_applied: usize,
    field_upper_applied: usize,

    delta_field_patch_vs_global: i64,
    delta_field_patch_vs_chunked: i64,
    delta_field_total_vs_global: i64,
    delta_field_total_vs_chunked: i64,
    delta_compact_chunk_total_vs_global: i64,
    delta_compact_field_total_vs_global: i64,
    delta_compact_field_total_vs_chunked: i64,
    delta_compact_field_total_vs_compact_chunked: i64,

    target_hist: [u64; 4],
    global_pred_hist: [u64; 4],
    chunk_pred_hist: [u64; 4],
    field_pred_hist: [u64; 4],
}

#[derive(Clone, Debug)]
struct ApexMapCaseRun {
    report: ApexMapCaseReport,
}

pub fn run_apex_map_case(args: ApexMapCaseArgs) -> Result<()> {
    let chunk_values = parse_usize_sweep_values(args.chunk_sweep.as_deref(), args.chunk_bytes, "chunk")?;
    let boundary_band_values =
        parse_usize_sweep_values(args.boundary_band_sweep.as_deref(), args.boundary_band, "boundary_band")?;
    let field_margin_values =
        parse_u64_sweep_values(args.field_margin_sweep.as_deref(), args.field_margin, "field_margin")?;

    let input = std::fs::read(&args.r#in).with_context(|| format!("read {}", args.r#in))?;
    let norm = text_norm::normalize_newlines(&input);
    let cases = CaseLanes::split(&norm);
    if cases.case_lane.is_empty() {
        return Err(anyhow!("apex-map-case: input contains no ASCII letters"));
    }

    let cfg = SearchCfg {
        seed_from: args.seed_from,
        seed_count: args.seed_count,
        seed_step: args.seed_step,
        recipe_seed: args.recipe_seed,
    };

    let global = brute_force_best_symbol_lane(&cases.case_lane, cfg, CLASS_COUNT)?;
    let global_patch = PatchList::from_pred_actual(&global.predicted, &cases.case_lane)
        .map_err(|e| anyhow!("apex-map-case global patch build failed: {e}"))?;
    let global_patch_bytes = global_patch.encode();
    let global_total_payload_exact = global_patch_bytes.len().saturating_add(APEX_KEY_BYTES_EXACT);
    let global_patch_entries = global_patch.entries.len();

    let mut runs = Vec::new();

    for chunk_bytes in chunk_values {
        for &boundary_band in &boundary_band_values {
            for &field_margin in &field_margin_values {
                runs.push(run_apex_map_case_once(
                    &args,
                    &norm,
                    &cases,
                    &global,
                    global_patch_entries,
                    &global_patch_bytes,
                    global_total_payload_exact,
                    cfg,
                    chunk_bytes,
                    boundary_band,
                    field_margin,
                )?);
            }
        }
    }

    if runs.is_empty() {
        return Err(anyhow!("apex-map-case: no runs executed"));
    }

    let best_idx = best_run_index(&runs);
    let best = &runs[best_idx].report;

    let body = match args.format {
        RenderFormat::Csv => render_reports_csv(&runs),
        RenderFormat::Txt => render_reports_txt(&runs),
    };

    write_or_print(args.out.as_deref(), &body)?;
    print_summary(args.out.as_deref(), args.format, &runs, best_idx);

    eprintln!(
        "apex-map-case best: chunk={} boundary_band={} field_margin={} compact_field_total_payload_exact={} field_match_pct={:.6}",
        best.chunk_bytes,
        best.boundary_band,
        best.field_margin,
        best.compact_field_total_payload_exact,
        best.field_match_pct,
    );

    Ok(())
}

fn run_apex_map_case_once(
    args: &ApexMapCaseArgs,
    norm: &[u8],
    cases: &CaseLanes,
    global: &LaneBest,
    global_patch_entries: usize,
    global_patch_bytes: &[u8],
    global_total_payload_exact: usize,
    cfg: SearchCfg,
    chunk_bytes: usize,
    boundary_band: usize,
    field_margin: u64,
) -> Result<ApexMapCaseRun> {
    let target_metrics = compute_symbol_metrics(&cases.case_lane, &cases.case_lane, CLASS_COUNT)?;

    let (chunked, chunk_summary) = brute_force_best_symbol_lane_chunked(
        &cases.case_lane,
        cfg,
        chunk_bytes,
        args.chunk_search_objective,
        args.chunk_raw_slack,
        CLASS_COUNT,
    )?;
    let chunk_patch = PatchList::from_pred_actual(&chunked.predicted, &cases.case_lane)
        .map_err(|e| anyhow!("apex-map-case chunked patch build failed: {e}"))?;
    let chunk_patch_bytes = chunk_patch.encode();

    let field_source = if args.field_from_global {
        global.predicted.clone()
    } else {
        chunked.predicted.clone()
    };

    let map = ApexMap::from_symbols(
        &field_source,
        ApexMapCfg {
            class_count: CLASS_COUNT,
            max_depth: args.map_max_depth,
            depth_shift: args.map_depth_shift,
        },
    )?;

    let boundaries = chunked.chunks.iter().skip(1).map(|chunk| chunk.start).collect::<Vec<_>>();

    let mut refine_cfg = RefineCfg::new(boundary_band, args.boundary_delta, field_margin);
    refine_cfg.desired_margin_add[0] = args.lower_margin_add;
    refine_cfg.desired_margin_add[1] = args.upper_margin_add;
    refine_cfg.dominant_share_ppm_min[0] = args.lower_share_ppm_min;
    refine_cfg.dominant_share_ppm_min[1] = args.upper_share_ppm_min;

    let (field_predicted, field_stats) = map.refine_boundaries(&chunked.predicted, &boundaries, refine_cfg)?;

    let global_metrics = compute_symbol_metrics(&cases.case_lane, &global.predicted, CLASS_COUNT)?;
    let chunk_metrics = compute_symbol_metrics(&cases.case_lane, &chunked.predicted, CLASS_COUNT)?;
    let field_metrics = compute_symbol_metrics(&cases.case_lane, &field_predicted, CLASS_COUNT)?;

    let field_patch = PatchList::from_pred_actual(&field_predicted, &cases.case_lane)
        .map_err(|e| anyhow!("apex-map-case field patch build failed: {e}"))?;
    let field_patch_bytes = field_patch.encode();

    let compact_manifest = CompactChunkManifest {
        total_len: chunked.predicted.len() as u64,
        chunk_bytes: chunked.chunk_bytes as u64,
        recipe_seed: cfg.recipe_seed,
        keys: chunked
            .chunks
            .iter()
            .map(|chunk| CompactChunkKey {
                root_quadrant: chunk.key.root_quadrant,
                root_seed: chunk.key.root_seed,
            })
            .collect::<Vec<_>>(),
    };
    let compact_manifest_bytes = compact_manifest.encode();
    let compact_manifest_decoded = CompactChunkManifest::decode(&compact_manifest_bytes)?;
    if compact_manifest_decoded != compact_manifest {
        return Err(anyhow!("apex-map-case compact manifest roundtrip mismatch"));
    }

    let chunk_total_payload_exact = chunk_patch_bytes.len().saturating_add(chunked.chunk_key_bytes_exact);
    let field_total_payload_exact = field_patch_bytes.len().saturating_add(chunked.chunk_key_bytes_exact);
    let compact_manifest_bytes_exact = compact_manifest_bytes.len();
    let compact_chunk_total_payload_exact = chunk_patch_bytes.len().saturating_add(compact_manifest_bytes_exact);
    let compact_field_total_payload_exact = field_patch_bytes.len().saturating_add(compact_manifest_bytes_exact);

    let report = ApexMapCaseReport {
        input: args.r#in.clone(),
        normalized_len: norm.len(),
        letter_positions: cases.letter_len,
        case_len: cases.case_lane.len(),

        majority_class: target_metrics.majority_class,
        majority_class_label: case_label(target_metrics.majority_class),
        majority_count: target_metrics.majority_count,
        majority_baseline_match_pct: target_metrics.majority_baseline_match_pct,
        target_entropy_bits: target_metrics.target_entropy_bits,

        global_patch_entries,
        global_patch_bytes: global_patch_bytes.len(),
        global_total_payload_exact,
        global_match_pct: global_metrics.raw_match_pct,
        global_match_vs_majority_pct: global_metrics.raw_match_vs_majority_pct,
        global_balanced_accuracy_pct: global_metrics.balanced_accuracy_pct,
        global_macro_f1_pct: global_metrics.macro_f1_pct,

        chunk_bytes,
        chunk_count: chunked.chunks.len(),
        chunk_patch_entries: chunk_patch.entries.len(),
        chunk_patch_bytes: chunk_patch_bytes.len(),
        chunk_total_payload_exact,
        compact_manifest_bytes_exact,
        compact_chunk_total_payload_exact,
        chunk_match_pct: chunk_metrics.raw_match_pct,
        chunk_match_vs_majority_pct: chunk_metrics.raw_match_vs_majority_pct,
        chunk_balanced_accuracy_pct: chunk_metrics.balanced_accuracy_pct,
        chunk_macro_f1_pct: chunk_metrics.macro_f1_pct,
        chunk_search_objective: chunk_search_objective_name(args.chunk_search_objective).to_string(),
        chunk_raw_slack: args.chunk_raw_slack,
        chunk_mean_balanced_accuracy_pct: chunk_summary.mean_balanced_accuracy_pct,
        chunk_mean_macro_f1_pct: chunk_summary.mean_macro_f1_pct,
        chunk_mean_minority_f1_pct: chunk_summary.mean_minority_f1_pct,
        chunk_mean_abs_minority_delta: chunk_summary.mean_abs_minority_delta,
        chunk_majority_flip_count: chunk_summary.majority_flip_count,
        chunk_collapse_90_count: chunk_summary.collapse_90_count,

        field_source: if args.field_from_global { "global".to_string() } else { "chunked".to_string() },
        map_node_count: map.node_count(),
        map_depth_seen: map.max_depth_seen(),
        map_depth_shift: args.map_depth_shift,
        map_max_depth_arg: args.map_max_depth,
        boundary_band,
        boundary_delta: args.boundary_delta,
        field_margin,
        lower_margin_add: args.lower_margin_add,
        upper_margin_add: args.upper_margin_add,
        lower_share_ppm_min: args.lower_share_ppm_min,
        upper_share_ppm_min: args.upper_share_ppm_min,

        field_patch_entries: field_patch.entries.len(),
        field_patch_bytes: field_patch_bytes.len(),
        field_total_payload_exact,
        compact_field_total_payload_exact,
        field_match_pct: field_metrics.raw_match_pct,
        field_match_vs_majority_pct: field_metrics.raw_match_vs_majority_pct,
        field_balanced_accuracy_pct: field_metrics.balanced_accuracy_pct,
        field_macro_precision_pct: field_metrics.macro_precision_pct,
        field_macro_recall_pct: field_metrics.macro_recall_pct,
        field_macro_f1_pct: field_metrics.macro_f1_pct,
        field_weighted_f1_pct: field_metrics.weighted_f1_pct,
        field_pred_entropy_bits: field_metrics.pred_entropy_bits,
        field_hist_l1_pct: field_metrics.hist_l1_pct,
        field_pred_dominant_class: field_metrics.pred_dominant_class,
        field_pred_dominant_label: case_label(field_metrics.pred_dominant_class),
        field_pred_dominant_share_ppm: field_metrics.pred_dominant_share_ppm,
        field_pred_dominant_share_pct: field_metrics.pred_dominant_share_pct,
        field_pred_collapse_90_flag: field_metrics.pred_collapse_90_flag,

        field_precision_lower_pct: field_metrics.per_class[0].precision_pct,
        field_recall_lower_pct: field_metrics.per_class[0].recall_pct,
        field_f1_lower_pct: field_metrics.per_class[0].f1_pct,
        field_precision_upper_pct: field_metrics.per_class[1].precision_pct,
        field_recall_upper_pct: field_metrics.per_class[1].recall_pct,
        field_f1_upper_pct: field_metrics.per_class[1].f1_pct,

        field_overrides: field_stats.overrides,
        field_boundary_count: field_stats.boundary_count,
        field_touched_positions: field_stats.touched_positions,
        field_lower_applied: field_stats.applied_by_desired[0],
        field_upper_applied: field_stats.applied_by_desired[1],

        delta_field_patch_vs_global: (field_patch_bytes.len() as i64) - (global_patch_bytes.len() as i64),
        delta_field_patch_vs_chunked: (field_patch_bytes.len() as i64) - (chunk_patch_bytes.len() as i64),
        delta_field_total_vs_global: (field_total_payload_exact as i64) - (global_total_payload_exact as i64),
        delta_field_total_vs_chunked: (field_total_payload_exact as i64) - (chunk_total_payload_exact as i64),
        delta_compact_chunk_total_vs_global: (compact_chunk_total_payload_exact as i64) - (global_total_payload_exact as i64),
        delta_compact_field_total_vs_global: (compact_field_total_payload_exact as i64) - (global_total_payload_exact as i64),
        delta_compact_field_total_vs_chunked: (compact_field_total_payload_exact as i64) - (chunk_total_payload_exact as i64),
        delta_compact_field_total_vs_compact_chunked: (compact_field_total_payload_exact as i64) - (compact_chunk_total_payload_exact as i64),

        target_hist: field_metrics.target_hist,
        global_pred_hist: global_metrics.pred_hist,
        chunk_pred_hist: chunk_metrics.pred_hist,
        field_pred_hist: field_metrics.pred_hist,
    };

    Ok(ApexMapCaseRun { report })
}

fn best_run_index(runs: &[ApexMapCaseRun]) -> usize {
    let mut best_idx = 0usize;
    for idx in 1..runs.len() {
        let best = &runs[best_idx].report;
        let cand = &runs[idx].report;
        let better = (
            cand.field_pred_collapse_90_flag,
            cand.compact_field_total_payload_exact,
            cand.field_patch_bytes,
            std::cmp::Reverse((cand.field_balanced_accuracy_pct * 1_000_000.0) as i64),
            std::cmp::Reverse((cand.field_macro_f1_pct * 1_000_000.0) as i64),
            cand.field_hist_l1_pct.to_bits(),
        ) < (
            best.field_pred_collapse_90_flag,
            best.compact_field_total_payload_exact,
            best.field_patch_bytes,
            std::cmp::Reverse((best.field_balanced_accuracy_pct * 1_000_000.0) as i64),
            std::cmp::Reverse((best.field_macro_f1_pct * 1_000_000.0) as i64),
            best.field_hist_l1_pct.to_bits(),
        );
        if better {
            best_idx = idx;
        }
    }
    best_idx
}

fn parse_usize_sweep_values(raw: Option<&str>, fallback: usize, label: &str) -> Result<Vec<usize>> {
    let Some(raw) = raw else {
        return Ok(vec![fallback]);
    };

    let mut out = Vec::new();
    for part in raw.split(',') {
        let token = part.trim();
        if token.is_empty() {
            continue;
        }
        let value = token
            .parse::<usize>()
            .with_context(|| format!("parse {} sweep value {}", label, token))?;
        if !out.contains(&value) {
            out.push(value);
        }
    }

    if out.is_empty() {
        return Err(anyhow!("apex-map-case: {} sweep produced no values", label));
    }

    Ok(out)
}

fn parse_u64_sweep_values(raw: Option<&str>, fallback: u64, label: &str) -> Result<Vec<u64>> {
    let Some(raw) = raw else {
        return Ok(vec![fallback]);
    };

    let mut out = Vec::new();
    for part in raw.split(',') {
        let token = part.trim();
        if token.is_empty() {
            continue;
        }
        let value = token
            .parse::<u64>()
            .with_context(|| format!("parse {} sweep value {}", label, token))?;
        if !out.contains(&value) {
            out.push(value);
        }
    }

    if out.is_empty() {
        return Err(anyhow!("apex-map-case: {} sweep produced no values", label));
    }

    Ok(out)
}

fn brute_force_best_symbol_lane(target: &[u8], cfg: SearchCfg, class_count: u8) -> Result<LaneBest> {
    if cfg.seed_step == 0 {
        return Err(anyhow!("apex-map-case: seed_step must be >= 1"));
    }

    let byte_len = target.len() as u64;
    let mut best: Option<LaneBest> = None;

    for quadrant in 0u8..=3 {
        let mut i = 0u64;
        while i < cfg.seed_count {
            let seed = cfg.seed_from.saturating_add(i.saturating_mul(cfg.seed_step));
            let key = ApexKey::new_dibit_v1(byte_len, quadrant, seed, cfg.recipe_seed)?;
            let predicted = generate_bytes(&key)?
                .into_iter()
                .map(|b| bucket_u8_local(b, class_count))
                .collect::<Vec<_>>();
            let score = score_symbol_lane(target, &predicted, class_count)?;
            let cand = LaneBest { key, predicted, score };
            match &best {
                None => best = Some(cand),
                Some(cur) if cand.score.better_than(&cur.score) => best = Some(cand),
                Some(_) => {}
            }
            i = i.saturating_add(1);
        }
    }

    best.ok_or_else(|| anyhow!("apex-map-case: search produced no candidates"))
}

fn brute_force_best_symbol_lane_chunked(
    target: &[u8],
    cfg: SearchCfg,
    chunk_bytes: usize,
    objective: ChunkSearchObjective,
    raw_slack: u64,
    class_count: u8,
) -> Result<(LaneChunkedBest, ChunkSearchSummary)> {
    if chunk_bytes == 0 {
        return Err(anyhow!("apex-map-case: chunk_bytes must be >= 1"));
    }

    let mut predicted = Vec::with_capacity(target.len());
    let mut chunks = Vec::new();
    let mut start = 0usize;
    let mut chunk_index = 0usize;

    let mut sum_balanced_accuracy_pct = 0.0;
    let mut sum_macro_f1_pct = 0.0;
    let mut sum_minority_f1_pct = 0.0;
    let mut sum_abs_minority_delta = 0.0;
    let mut majority_flip_count = 0usize;
    let mut collapse_90_count = 0usize;

    while start < target.len() {
        let end = start.saturating_add(chunk_bytes).min(target.len());
        let slice = &target[start..end];
        let (best, metrics) = brute_force_best_symbol_lane_objective(slice, cfg, objective, raw_slack, class_count)?;
        let patch = PatchList::from_pred_actual(&best.predicted, slice)
            .map_err(|e| anyhow!("apex-map-case chunk patch build failed: {e}"))?;
        let patch_bytes = patch.encode();

        sum_balanced_accuracy_pct += metrics.balanced_accuracy_pct;
        sum_macro_f1_pct += metrics.macro_f1_pct;
        sum_minority_f1_pct += minority_f1(&metrics, class_count);
        sum_abs_minority_delta += minority_delta_abs(&metrics, class_count) as f64;
        if metrics.pred_dominant_class != metrics.majority_class {
            majority_flip_count = majority_flip_count.saturating_add(1);
        }
        if metrics.pred_collapse_90_flag {
            collapse_90_count = collapse_90_count.saturating_add(1);
        }

        predicted.extend_from_slice(&best.predicted);
        chunks.push(LaneChunkBest {
            chunk_index,
            start,
            end,
            key: best.key,
            patch_entries: patch.entries.len(),
            patch_bytes: patch_bytes.len(),
        });

        start = end;
        chunk_index = chunk_index.saturating_add(1);
    }

    let denom = chunks.len().max(1) as f64;

    Ok((
        LaneChunkedBest {
            chunk_bytes,
            chunk_key_bytes_exact: chunks.len().saturating_mul(APEX_KEY_BYTES_EXACT),
            predicted,
            chunks,
        },
        ChunkSearchSummary {
            mean_balanced_accuracy_pct: sum_balanced_accuracy_pct / denom,
            mean_macro_f1_pct: sum_macro_f1_pct / denom,
            mean_minority_f1_pct: sum_minority_f1_pct / denom,
            mean_abs_minority_delta: sum_abs_minority_delta / denom,
            majority_flip_count,
            collapse_90_count,
        },
    ))
}

fn brute_force_best_symbol_lane_objective(
    target: &[u8],
    cfg: SearchCfg,
    objective: ChunkSearchObjective,
    raw_slack: u64,
    class_count: u8,
) -> Result<(LaneBest, SymbolMetrics)> {
    if cfg.seed_step == 0 {
        return Err(anyhow!("apex-map-case: seed_step must be >= 1"));
    }

    if objective == ChunkSearchObjective::Raw {
        let best = brute_force_best_symbol_lane(target, cfg, class_count)?;
        let metrics = compute_symbol_metrics(target, &best.predicted, class_count)?;
        return Ok((best, metrics));
    }

    let raw_anchor = brute_force_best_symbol_lane(target, cfg, class_count)?;
    let raw_anchor_matches = raw_anchor.score.matches;
    let byte_len = target.len() as u64;
    let mut best: Option<(LaneBest, SymbolMetrics)> = None;

    for quadrant in 0u8..=3 {
        let mut i = 0u64;
        while i < cfg.seed_count {
            let seed = cfg.seed_from.saturating_add(i.saturating_mul(cfg.seed_step));
            let key = ApexKey::new_dibit_v1(byte_len, quadrant, seed, cfg.recipe_seed)?;
            let predicted = generate_bytes(&key)?
                .into_iter()
                .map(|b| bucket_u8_local(b, class_count))
                .collect::<Vec<_>>();
            let score = score_symbol_lane(target, &predicted, class_count)?;

            if objective == ChunkSearchObjective::RawGuarded
                && raw_anchor_matches.saturating_sub(score.matches) > raw_slack
            {
                i = i.saturating_add(1);
                continue;
            }

            let metrics = compute_symbol_metrics(target, &predicted, class_count)?;
            let cand = LaneBest { key, predicted, score };
            match &best {
                None => best = Some((cand, metrics)),
                Some((cur, cur_metrics)) if chunk_candidate_better(&cand, &metrics, cur, cur_metrics, objective, class_count) => {
                    best = Some((cand, metrics))
                }
                Some(_) => {}
            }

            i = i.saturating_add(1);
        }
    }

    if let Some(best) = best {
        Ok(best)
    } else {
        let metrics = compute_symbol_metrics(target, &raw_anchor.predicted, class_count)?;
        Ok((raw_anchor, metrics))
    }
}

fn chunk_candidate_better(
    cand: &LaneBest,
    cand_metrics: &SymbolMetrics,
    cur: &LaneBest,
    cur_metrics: &SymbolMetrics,
    objective: ChunkSearchObjective,
    class_count: u8,
) -> bool {
    let cand_minority_delta_abs = minority_delta_abs(cand_metrics, class_count);
    let cur_minority_delta_abs = minority_delta_abs(cur_metrics, class_count);
    let cand_minority_f1 = (minority_f1(cand_metrics, class_count) * 1_000_000.0) as i64;
    let cur_minority_f1 = (minority_f1(cur_metrics, class_count) * 1_000_000.0) as i64;
    let cand_majority_flip = cand_metrics.pred_dominant_class != cand_metrics.majority_class;
    let cur_majority_flip = cur_metrics.pred_dominant_class != cur_metrics.majority_class;

    let cand_key = (
        cand_metrics.pred_collapse_90_flag,
        cand_majority_flip,
        cand_minority_delta_abs,
        std::cmp::Reverse(cand_minority_f1),
        std::cmp::Reverse((cand_metrics.balanced_accuracy_pct * 1_000_000.0) as i64),
        std::cmp::Reverse((cand_metrics.macro_f1_pct * 1_000_000.0) as i64),
        cand_metrics.hist_l1,
        std::cmp::Reverse(cand.score.matches),
        std::cmp::Reverse(cand.score.longest_run),
        std::cmp::Reverse(cand.score.prefix),
    );

    let cur_key = (
        cur_metrics.pred_collapse_90_flag,
        cur_majority_flip,
        cur_minority_delta_abs,
        std::cmp::Reverse(cur_minority_f1),
        std::cmp::Reverse((cur_metrics.balanced_accuracy_pct * 1_000_000.0) as i64),
        std::cmp::Reverse((cur_metrics.macro_f1_pct * 1_000_000.0) as i64),
        cur_metrics.hist_l1,
        std::cmp::Reverse(cur.score.matches),
        std::cmp::Reverse(cur.score.longest_run),
        std::cmp::Reverse(cur.score.prefix),
    );

    match objective {
        ChunkSearchObjective::Raw => cand.score.better_than(&cur.score),
        ChunkSearchObjective::RawGuarded | ChunkSearchObjective::Honest | ChunkSearchObjective::Newline => cand_key < cur_key,
    }
}

fn minority_delta_abs(metrics: &SymbolMetrics, class_count: u8) -> i64 {
    let mut out = 0i64;
    for cls in 0..class_count as usize {
        if cls == metrics.majority_class as usize {
            continue;
        }
        out += ((metrics.pred_hist[cls] as i64) - (metrics.target_hist[cls] as i64)).abs();
    }
    out
}

fn minority_f1(metrics: &SymbolMetrics, class_count: u8) -> f64 {
    let mut sum = 0.0;
    let mut count = 0usize;
    for cls in 0..class_count as usize {
        if cls == metrics.majority_class as usize {
            continue;
        }
        if metrics.per_class[cls].support > 0 || metrics.per_class[cls].predicted > 0 {
            sum += metrics.per_class[cls].f1_pct;
            count += 1;
        }
    }
    if count == 0 {
        0.0
    } else {
        sum / (count as f64)
    }
}

fn chunk_search_objective_name(objective: ChunkSearchObjective) -> &'static str {
    match objective {
        ChunkSearchObjective::Raw => "raw",
        ChunkSearchObjective::RawGuarded => "raw-guarded",
        ChunkSearchObjective::Honest => "honest",
        ChunkSearchObjective::Newline => "newline-as-honest",
    }
}

fn score_symbol_lane(target: &[u8], predicted: &[u8], class_count: u8) -> Result<LaneScore> {
    if target.len() != predicted.len() {
        return Err(anyhow!(
            "apex-map-case: target len {} != predicted len {}",
            target.len(),
            predicted.len()
        ));
    }

    let mut matches = 0u64;
    let mut prefix = 0u64;
    let mut still_prefix = true;
    let mut current_run = 0u64;
    let mut current_run_start = 0u64;
    let mut longest_run = 0u64;
    let mut longest_run_start = 0u64;

    for (idx, (&t, &p)) in target.iter().zip(predicted.iter()).enumerate() {
        let _ = symbol_slot_local(t, class_count)?;
        let _ = symbol_slot_local(p, class_count)?;
        if t == p {
            matches = matches.saturating_add(1);
            if still_prefix {
                prefix = prefix.saturating_add(1);
            }
            if current_run == 0 {
                current_run_start = idx as u64;
            }
            current_run = current_run.saturating_add(1);
            if current_run > longest_run {
                longest_run = current_run;
                longest_run_start = current_run_start;
            }
        } else {
            still_prefix = false;
            current_run = 0;
        }
    }

    Ok(LaneScore {
        matches,
        prefix,
        total: target.len() as u64,
        longest_run,
        longest_run_start,
    })
}

fn render_reports_txt(runs: &[ApexMapCaseRun]) -> String {
    let mut out = String::new();
    for (idx, run) in runs.iter().enumerate() {
        if idx > 0 {
            out.push('\n');
        }
        out.push_str(&render_report_txt(&run.report));
    }
    out
}

fn render_report_txt(row: &ApexMapCaseReport) -> String {
    let mut out = String::new();

    out.push_str(&format!("input={}\n", row.input));
    out.push_str(&format!(
        "normalized_len={} letter_positions={} case_len={}\n",
        row.normalized_len, row.letter_positions, row.case_len
    ));
    out.push_str(&format!(
        "majority_class={} majority_label={} majority_count={} majority_baseline_match_pct={:.6} target_entropy_bits={:.6}\n",
        row.majority_class,
        row.majority_class_label,
        row.majority_count,
        row.majority_baseline_match_pct,
        row.target_entropy_bits,
    ));
    out.push_str(&format!(
        "global_patch_entries={} global_patch_bytes={} global_total_payload_exact={} global_match_pct={:.6} global_match_vs_majority_pct={:.6} global_balanced_accuracy_pct={:.6} global_macro_f1_pct={:.6}\n",
        row.global_patch_entries,
        row.global_patch_bytes,
        row.global_total_payload_exact,
        row.global_match_pct,
        row.global_match_vs_majority_pct,
        row.global_balanced_accuracy_pct,
        row.global_macro_f1_pct,
    ));
    out.push_str(&format!(
        "chunk_bytes={} chunk_count={} chunk_patch_entries={} chunk_patch_bytes={} chunk_total_payload_exact={} compact_manifest_bytes_exact={} compact_chunk_total_payload_exact={} chunk_match_pct={:.6} chunk_match_vs_majority_pct={:.6} chunk_balanced_accuracy_pct={:.6} chunk_macro_f1_pct={:.6} chunk_search_objective={} chunk_raw_slack={} chunk_mean_balanced_accuracy_pct={:.6} chunk_mean_macro_f1_pct={:.6} chunk_mean_minority_f1_pct={:.6} chunk_mean_abs_minority_delta={:.6} chunk_majority_flip_count={} chunk_collapse_90_count={}\n",
        row.chunk_bytes,
        row.chunk_count,
        row.chunk_patch_entries,
        row.chunk_patch_bytes,
        row.chunk_total_payload_exact,
        row.compact_manifest_bytes_exact,
        row.compact_chunk_total_payload_exact,
        row.chunk_match_pct,
        row.chunk_match_vs_majority_pct,
        row.chunk_balanced_accuracy_pct,
        row.chunk_macro_f1_pct,
        row.chunk_search_objective,
        row.chunk_raw_slack,
        row.chunk_mean_balanced_accuracy_pct,
        row.chunk_mean_macro_f1_pct,
        row.chunk_mean_minority_f1_pct,
        row.chunk_mean_abs_minority_delta,
        row.chunk_majority_flip_count,
        row.chunk_collapse_90_count,
    ));
    out.push_str(&format!(
        "field_source={} map_node_count={} map_depth_seen={} map_depth_shift={} map_max_depth_arg={} boundary_band={} boundary_delta={} field_margin={} lower_margin_add={} upper_margin_add={} lower_share_ppm_min={} upper_share_ppm_min={}\n",
        row.field_source,
        row.map_node_count,
        row.map_depth_seen,
        row.map_depth_shift,
        row.map_max_depth_arg,
        row.boundary_band,
        row.boundary_delta,
        row.field_margin,
        row.lower_margin_add,
        row.upper_margin_add,
        row.lower_share_ppm_min,
        row.upper_share_ppm_min,
    ));
    out.push_str(&format!(
        "field_patch_entries={} field_patch_bytes={} field_total_payload_exact={} compact_field_total_payload_exact={} field_match_pct={:.6} field_match_vs_majority_pct={:.6} field_balanced_accuracy_pct={:.6} field_macro_precision_pct={:.6} field_macro_recall_pct={:.6} field_macro_f1_pct={:.6} field_weighted_f1_pct={:.6} field_pred_entropy_bits={:.6} field_hist_l1_pct={:.6}\n",
        row.field_patch_entries,
        row.field_patch_bytes,
        row.field_total_payload_exact,
        row.compact_field_total_payload_exact,
        row.field_match_pct,
        row.field_match_vs_majority_pct,
        row.field_balanced_accuracy_pct,
        row.field_macro_precision_pct,
        row.field_macro_recall_pct,
        row.field_macro_f1_pct,
        row.field_weighted_f1_pct,
        row.field_pred_entropy_bits,
        row.field_hist_l1_pct,
    ));
    out.push_str(&format!(
        "field_pred_dominant_class={} field_pred_dominant_label={} field_pred_dominant_share_ppm={} field_pred_dominant_share_pct={:.6} field_pred_collapse_90_flag={}\n",
        row.field_pred_dominant_class,
        row.field_pred_dominant_label,
        row.field_pred_dominant_share_ppm,
        row.field_pred_dominant_share_pct,
        row.field_pred_collapse_90_flag,
    ));
    out.push_str(&format!(
        "field_precision_lower_pct={:.6} field_recall_lower_pct={:.6} field_f1_lower_pct={:.6} field_precision_upper_pct={:.6} field_recall_upper_pct={:.6} field_f1_upper_pct={:.6}\n",
        row.field_precision_lower_pct,
        row.field_recall_lower_pct,
        row.field_f1_lower_pct,
        row.field_precision_upper_pct,
        row.field_recall_upper_pct,
        row.field_f1_upper_pct,
    ));
    out.push_str(&format!(
        "field_overrides={} field_boundary_count={} field_touched_positions={} field_lower_applied={} field_upper_applied={}\n",
        row.field_overrides,
        row.field_boundary_count,
        row.field_touched_positions,
        row.field_lower_applied,
        row.field_upper_applied,
    ));
    out.push_str(&format!(
        "delta_field_patch_vs_global={} delta_field_patch_vs_chunked={} delta_field_total_vs_global={} delta_field_total_vs_chunked={} delta_compact_chunk_total_vs_global={} delta_compact_field_total_vs_global={} delta_compact_field_total_vs_chunked={} delta_compact_field_total_vs_compact_chunked={}\n",
        row.delta_field_patch_vs_global,
        row.delta_field_patch_vs_chunked,
        row.delta_field_total_vs_global,
        row.delta_field_total_vs_chunked,
        row.delta_compact_chunk_total_vs_global,
        row.delta_compact_field_total_vs_global,
        row.delta_compact_field_total_vs_chunked,
        row.delta_compact_field_total_vs_compact_chunked,
    ));
    out.push_str(&format!(
        "target_hist=[{},{},{},{}] global_pred_hist=[{},{},{},{}] chunk_pred_hist=[{},{},{},{}] field_pred_hist=[{},{},{},{}]\n",
        row.target_hist[0], row.target_hist[1], row.target_hist[2], row.target_hist[3],
        row.global_pred_hist[0], row.global_pred_hist[1], row.global_pred_hist[2], row.global_pred_hist[3],
        row.chunk_pred_hist[0], row.chunk_pred_hist[1], row.chunk_pred_hist[2], row.chunk_pred_hist[3],
        row.field_pred_hist[0], row.field_pred_hist[1], row.field_pred_hist[2], row.field_pred_hist[3],
    ));

    out
}

fn render_reports_csv(runs: &[ApexMapCaseRun]) -> String {
    let mut out = String::from(
        "input,normalized_len,letter_positions,case_len,majority_class,majority_label,majority_count,majority_baseline_match_pct,target_entropy_bits,global_patch_entries,global_patch_bytes,global_total_payload_exact,global_match_pct,global_match_vs_majority_pct,global_balanced_accuracy_pct,global_macro_f1_pct,chunk_bytes,chunk_count,chunk_patch_entries,chunk_patch_bytes,chunk_total_payload_exact,compact_manifest_bytes_exact,compact_chunk_total_payload_exact,chunk_match_pct,chunk_match_vs_majority_pct,chunk_balanced_accuracy_pct,chunk_macro_f1_pct,chunk_search_objective,chunk_raw_slack,chunk_mean_balanced_accuracy_pct,chunk_mean_macro_f1_pct,chunk_mean_minority_f1_pct,chunk_mean_abs_minority_delta,chunk_majority_flip_count,chunk_collapse_90_count,field_source,map_node_count,map_depth_seen,map_depth_shift,map_max_depth_arg,boundary_band,boundary_delta,field_margin,lower_margin_add,upper_margin_add,lower_share_ppm_min,upper_share_ppm_min,field_patch_entries,field_patch_bytes,field_total_payload_exact,compact_field_total_payload_exact,field_match_pct,field_match_vs_majority_pct,field_balanced_accuracy_pct,field_macro_precision_pct,field_macro_recall_pct,field_macro_f1_pct,field_weighted_f1_pct,field_pred_entropy_bits,field_hist_l1_pct,field_pred_dominant_class,field_pred_dominant_label,field_pred_dominant_share_ppm,field_pred_dominant_share_pct,field_pred_collapse_90_flag,field_f1_lower_pct,field_f1_upper_pct,field_overrides,field_boundary_count,field_touched_positions,field_lower_applied,field_upper_applied,delta_field_patch_vs_global,delta_field_patch_vs_chunked,delta_field_total_vs_global,delta_field_total_vs_chunked,delta_compact_chunk_total_vs_global,delta_compact_field_total_vs_global,delta_compact_field_total_vs_chunked,delta_compact_field_total_vs_compact_chunked,target_hist_0,target_hist_1,target_hist_2,target_hist_3,field_pred_hist_0,field_pred_hist_1,field_pred_hist_2,field_pred_hist_3\n"
    );

    for run in runs {
        let row = &run.report;
        let cells = vec![
            csv_escape(&row.input),
            row.normalized_len.to_string(),
            row.letter_positions.to_string(),
            row.case_len.to_string(),
            row.majority_class.to_string(),
            row.majority_class_label.to_string(),
            row.majority_count.to_string(),
            fmt6(row.majority_baseline_match_pct),
            fmt6(row.target_entropy_bits),
            row.global_patch_entries.to_string(),
            row.global_patch_bytes.to_string(),
            row.global_total_payload_exact.to_string(),
            fmt6(row.global_match_pct),
            fmt6(row.global_match_vs_majority_pct),
            fmt6(row.global_balanced_accuracy_pct),
            fmt6(row.global_macro_f1_pct),
            row.chunk_bytes.to_string(),
            row.chunk_count.to_string(),
            row.chunk_patch_entries.to_string(),
            row.chunk_patch_bytes.to_string(),
            row.chunk_total_payload_exact.to_string(),
            row.compact_manifest_bytes_exact.to_string(),
            row.compact_chunk_total_payload_exact.to_string(),
            fmt6(row.chunk_match_pct),
            fmt6(row.chunk_match_vs_majority_pct),
            fmt6(row.chunk_balanced_accuracy_pct),
            fmt6(row.chunk_macro_f1_pct),
            row.chunk_search_objective.clone(),
            row.chunk_raw_slack.to_string(),
            fmt6(row.chunk_mean_balanced_accuracy_pct),
            fmt6(row.chunk_mean_macro_f1_pct),
            fmt6(row.chunk_mean_minority_f1_pct),
            fmt6(row.chunk_mean_abs_minority_delta),
            row.chunk_majority_flip_count.to_string(),
            row.chunk_collapse_90_count.to_string(),
            row.field_source.clone(),
            row.map_node_count.to_string(),
            row.map_depth_seen.to_string(),
            row.map_depth_shift.to_string(),
            row.map_max_depth_arg.to_string(),
            row.boundary_band.to_string(),
            row.boundary_delta.to_string(),
            row.field_margin.to_string(),
            row.lower_margin_add.to_string(),
            row.upper_margin_add.to_string(),
            row.lower_share_ppm_min.to_string(),
            row.upper_share_ppm_min.to_string(),
            row.field_patch_entries.to_string(),
            row.field_patch_bytes.to_string(),
            row.field_total_payload_exact.to_string(),
            row.compact_field_total_payload_exact.to_string(),
            fmt6(row.field_match_pct),
            fmt6(row.field_match_vs_majority_pct),
            fmt6(row.field_balanced_accuracy_pct),
            fmt6(row.field_macro_precision_pct),
            fmt6(row.field_macro_recall_pct),
            fmt6(row.field_macro_f1_pct),
            fmt6(row.field_weighted_f1_pct),
            fmt6(row.field_pred_entropy_bits),
            fmt6(row.field_hist_l1_pct),
            row.field_pred_dominant_class.to_string(),
            row.field_pred_dominant_label.to_string(),
            row.field_pred_dominant_share_ppm.to_string(),
            fmt6(row.field_pred_dominant_share_pct),
            row.field_pred_collapse_90_flag.to_string(),
            fmt6(row.field_f1_lower_pct),
            fmt6(row.field_f1_upper_pct),
            row.field_overrides.to_string(),
            row.field_boundary_count.to_string(),
            row.field_touched_positions.to_string(),
            row.field_lower_applied.to_string(),
            row.field_upper_applied.to_string(),
            row.delta_field_patch_vs_global.to_string(),
            row.delta_field_patch_vs_chunked.to_string(),
            row.delta_field_total_vs_global.to_string(),
            row.delta_field_total_vs_chunked.to_string(),
            row.delta_compact_chunk_total_vs_global.to_string(),
            row.delta_compact_field_total_vs_global.to_string(),
            row.delta_compact_field_total_vs_chunked.to_string(),
            row.delta_compact_field_total_vs_compact_chunked.to_string(),
            row.target_hist[0].to_string(),
            row.target_hist[1].to_string(),
            row.target_hist[2].to_string(),
            row.target_hist[3].to_string(),
            row.field_pred_hist[0].to_string(),
            row.field_pred_hist[1].to_string(),
            row.field_pred_hist[2].to_string(),
            row.field_pred_hist[3].to_string(),
        ];
        out.push_str(&cells.join(","));
        out.push('\n');
    }

    out
}

fn print_summary(out_path: Option<&str>, format: RenderFormat, runs: &[ApexMapCaseRun], best_idx: usize) {
    let best = &runs[best_idx].report;
    match out_path {
        Some(path) => {
            eprintln!(
                "apextrace apex-map-case {:?} saved: {} (runs={} best_chunk={} best_boundary_band={} best_field_margin={} compact_field_total_payload_exact={} field_match_pct={:.6})",
                format,
                path,
                runs.len(),
                best.chunk_bytes,
                best.boundary_band,
                best.field_margin,
                best.compact_field_total_payload_exact,
                best.field_match_pct,
            );
        }
        None => {
            eprintln!(
                "apextrace apex-map-case {:?}: runs={} best_chunk={} best_boundary_band={} best_field_margin={} compact_field_total_payload_exact={} field_match_pct={:.6}",
                format,
                runs.len(),
                best.chunk_bytes,
                best.boundary_band,
                best.field_margin,
                best.compact_field_total_payload_exact,
                best.field_match_pct,
            );
        }
    }
}

fn fmt6(v: f64) -> String {
    format!("{:.6}", v)
}

fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

fn symbol_slot_local(v: u8, class_count: u8) -> Result<usize> {
    if v < class_count {
        Ok(v as usize)
    } else {
        Err(anyhow!("apex-map-case: invalid class symbol {}", v))
    }
}

#[inline]
fn bucket_u8_local(b: u8, k: u8) -> u8 {
    ((b as u16 * k as u16) >> 8) as u8
}