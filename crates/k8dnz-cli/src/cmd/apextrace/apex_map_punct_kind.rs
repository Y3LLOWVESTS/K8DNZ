use anyhow::{anyhow, Context, Result};
use k8dnz_apextrace::{
    generate_bytes, ApexKey, ApexMap, ApexMapCfg, OverrideTrace, RefineCfg, RefineStats, SearchCfg,
};
use k8dnz_core::repr::{
    punct_kind_lanes::{punct_kind_label, PunctKindLanes},
    punct_lanes::PunctLanes,
    text_norm,
};
use k8dnz_core::symbol::patch::PatchList;

use crate::cmd::apextrace::{ApexMapPunctKindArgs, ChunkSearchObjective, RenderFormat};

use super::common::write_or_print;
use super::compact_manifest::{render_compact_manifest_csv, CompactChunkManifest, CompactChunkKey};
use super::symbol_metrics::{compute_symbol_metrics, SymbolMetrics};

const APEX_KEY_BYTES_EXACT: usize = 48;
const CLASS_COUNT: u8 = 3;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct GenericLaneScore {
    matches: u64,
    prefix: u64,
    total: u64,
    longest_run: u64,
    longest_run_start: u64,
}

impl GenericLaneScore {
    fn better_than(&self, other: &Self) -> bool {
        (self.matches, self.longest_run, self.prefix) > (other.matches, other.longest_run, other.prefix)
    }
}

#[derive(Clone, Debug)]
struct GenericLaneBest {
    key: ApexKey,
    predicted: Vec<u8>,
    score: GenericLaneScore,
}

#[derive(Clone, Debug)]
struct GenericLaneChunkBest {
    chunk_index: usize,
    start: usize,
    end: usize,
    key: ApexKey,
    patch_entries: usize,
    patch_bytes: usize,
}

#[derive(Clone, Debug)]
struct GenericLaneChunkedBest {
    chunk_bytes: usize,
    chunk_key_bytes_exact: usize,
    predicted: Vec<u8>,
    chunks: Vec<GenericLaneChunkBest>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct ChunkSearchSummary {
    mean_balanced_accuracy_pct: f64,
    mean_macro_f1_pct: f64,
    mean_non_majority_macro_f1_pct: f64,
    mean_abs_non_majority_delta: f64,
    majority_flip_count: usize,
    collapse_90_count: usize,
}

#[derive(Clone, Debug)]
struct ApexMapPunctKindReport {
    input: String,
    normalized_len: usize,
    punct_positions: usize,
    kind_len: usize,

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
    chunk_mean_non_majority_macro_f1_pct: f64,
    chunk_mean_abs_non_majority_delta: f64,
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
    term_margin_add: u64,
    pause_margin_add: u64,
    wrap_margin_add: u64,
    term_share_ppm_min: u32,
    pause_share_ppm_min: u32,
    wrap_share_ppm_min: u32,

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

    field_precision_term_pct: f64,
    field_recall_term_pct: f64,
    field_f1_term_pct: f64,
    field_precision_pause_pct: f64,
    field_recall_pause_pct: f64,
    field_f1_pause_pct: f64,
    field_precision_wrap_pct: f64,
    field_recall_wrap_pct: f64,
    field_f1_wrap_pct: f64,

    field_overrides: usize,
    field_boundary_count: usize,
    field_touched_positions: usize,
    field_term_applied: usize,
    field_pause_applied: usize,
    field_wrap_applied: usize,
    diag_rows: usize,

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
struct ApexMapPunctKindRun {
    report: ApexMapPunctKindReport,
    global: GenericLaneBest,
    chunked: GenericLaneChunkedBest,
    field_predicted: Vec<u8>,
    boundaries: Vec<usize>,
    map: ApexMap,
    compact_manifest: CompactChunkManifest,
    compact_manifest_bytes: Vec<u8>,
    diag_rows: Vec<ApexMapDiagRow>,
}

#[derive(Clone, Debug)]
struct ApexMapDiagRow {
    chunk_bytes: usize,
    boundary: usize,
    side: &'static str,
    pos: usize,
    target: u8,
    current: u8,
    desired: u8,
    pair_left: u8,
    pair_right: u8,
    allow_mask: u8,
    desired_score: u64,
    current_score: u64,
    needed_margin: u64,
    share_ppm: u32,
    share_floor: u32,
    decision: &'static str,
    applied: bool,
}

pub fn run_apex_map_punct_kind(args: ApexMapPunctKindArgs) -> Result<()> {
    let chunk_values = parse_usize_sweep_values(args.chunk_sweep.as_deref(), args.chunk_bytes, "chunk")?;
    let boundary_band_values = parse_usize_sweep_values(
        args.boundary_band_sweep.as_deref(),
        args.boundary_band,
        "boundary_band",
    )?;
    let field_margin_values = parse_u64_sweep_values(
        args.field_margin_sweep.as_deref(),
        args.field_margin,
        "field_margin",
    )?;

    let input = std::fs::read(&args.r#in).with_context(|| format!("read {}", args.r#in))?;
    let norm = text_norm::normalize_newlines(&input);
    let punct = PunctLanes::split(&norm);
    let kinds = PunctKindLanes::from_punct_bytes(&punct.punct_lane)
        .ok_or_else(|| anyhow!("apex-map-punct-kind: punct_lane contains non-punctuation bytes"))?;
    if kinds.kind_lane.is_empty() {
        return Err(anyhow!("apex-map-punct-kind: input contains no punctuation positions"));
    }

    let cfg = SearchCfg {
        seed_from: args.seed_from,
        seed_count: args.seed_count,
        seed_step: args.seed_step,
        recipe_seed: args.recipe_seed,
    };

    let global = brute_force_best_symbol_lane(&kinds.kind_lane, cfg, CLASS_COUNT)?;
    let global_patch = PatchList::from_pred_actual(&global.predicted, &kinds.kind_lane)
        .map_err(|e| anyhow!("apex-map-punct-kind global patch build failed: {e}"))?;
    let global_patch_bytes = global_patch.encode();
    let global_total_payload_exact = global_patch_bytes.len().saturating_add(APEX_KEY_BYTES_EXACT);
    let global_patch_entries = global_patch.entries.len();

    let mut runs = Vec::with_capacity(
        chunk_values
            .len()
            .saturating_mul(boundary_band_values.len())
            .saturating_mul(field_margin_values.len()),
    );

    for chunk_bytes in chunk_values {
        for &boundary_band in &boundary_band_values {
            for &field_margin in &field_margin_values {
                runs.push(run_apex_map_punct_kind_once(
                    &args,
                    &norm,
                    &punct,
                    &kinds,
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
        return Err(anyhow!("apex-map-punct-kind: no chunk sizes to run"));
    }

    let best_idx = best_run_index(&runs);
    let best = &runs[best_idx];

    if args.out_key.is_some() || args.out_pred.is_some() {
        if runs.len() > 1 {
            eprintln!(
                "apex-map-punct-kind sweep selected best output run: chunk_bytes={} boundary_band={} field_margin={} compact_field_total_payload_exact={} field_match_pct={:.6}",
                best.report.chunk_bytes,
                best.report.boundary_band,
                best.report.field_margin,
                best.report.compact_field_total_payload_exact,
                best.report.field_match_pct,
            );
        }
        save_outputs(
            args.out_key.as_deref(),
            args.out_pred.as_deref(),
            &best.global,
            &best.chunked,
            &best.field_predicted,
            &best.boundaries,
            &best.map,
            args.boundary_delta,
            &best.compact_manifest,
            &best.compact_manifest_bytes,
        )?;
    }

    if let Some(path) = args.out_diag.as_deref() {
        let diag = render_diag_csv(&runs);
        write_or_print(Some(path), &diag)?;
        eprintln!(
            "apextrace apex-map-punct-kind diagnostics saved: {} rows={}",
            path,
            runs.iter().map(|run| run.diag_rows.len()).sum::<usize>()
        );
    }

    let body = match args.format {
        RenderFormat::Csv => render_reports_csv(&runs),
        RenderFormat::Txt => render_reports_txt(&runs),
    };
    write_or_print(args.out.as_deref(), &body)?;
    print_summary(args.out.as_deref(), args.format, &runs, best_idx);
    Ok(())
}

fn run_apex_map_punct_kind_once(
    args: &ApexMapPunctKindArgs,
    norm: &[u8],
    punct: &PunctLanes,
    kinds: &PunctKindLanes,
    global: &GenericLaneBest,
    global_patch_entries: usize,
    global_patch_bytes: &[u8],
    global_total_payload_exact: usize,
    cfg: SearchCfg,
    chunk_bytes: usize,
    boundary_band: usize,
    field_margin: u64,
) -> Result<ApexMapPunctKindRun> {
    let target_metrics = compute_symbol_metrics(&kinds.kind_lane, &kinds.kind_lane, CLASS_COUNT)?;

    let (chunked, chunk_summary) = brute_force_best_symbol_lane_chunked(
        &kinds.kind_lane,
        cfg,
        chunk_bytes,
        args.chunk_search_objective,
        args.chunk_raw_slack,
        CLASS_COUNT,
    )?;
    let chunk_patch = PatchList::from_pred_actual(&chunked.predicted, &kinds.kind_lane)
        .map_err(|e| anyhow!("apex-map-punct-kind chunked patch build failed: {e}"))?;
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
    refine_cfg.desired_margin_add[0] = args.term_margin_add;
    refine_cfg.desired_margin_add[1] = args.pause_margin_add;
    refine_cfg.desired_margin_add[2] = args.wrap_margin_add;
    refine_cfg.dominant_share_ppm_min[0] = args.term_share_ppm_min;
    refine_cfg.dominant_share_ppm_min[1] = args.pause_share_ppm_min;
    refine_cfg.dominant_share_ppm_min[2] = args.wrap_share_ppm_min;

    let (field_predicted, field_stats, diag_rows) = refine_boundaries_with_diag(
        &map,
        &chunked.predicted,
        &kinds.kind_lane,
        &boundaries,
        refine_cfg,
        chunk_bytes,
        args.diag_limit,
    )?;

    let global_metrics = compute_symbol_metrics(&kinds.kind_lane, &global.predicted, CLASS_COUNT)?;
    let chunk_metrics = compute_symbol_metrics(&kinds.kind_lane, &chunked.predicted, CLASS_COUNT)?;
    let field_metrics = compute_symbol_metrics(&kinds.kind_lane, &field_predicted, CLASS_COUNT)?;

    let field_patch = PatchList::from_pred_actual(&field_predicted, &kinds.kind_lane)
        .map_err(|e| anyhow!("apex-map-punct-kind field patch build failed: {e}"))?;
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
        return Err(anyhow!("apex-map-punct-kind compact manifest roundtrip mismatch"));
    }

    let chunk_total_payload_exact = chunk_patch_bytes.len().saturating_add(chunked.chunk_key_bytes_exact);
    let field_total_payload_exact = field_patch_bytes.len().saturating_add(chunked.chunk_key_bytes_exact);
    let compact_manifest_bytes_exact = compact_manifest_bytes.len();
    let compact_chunk_total_payload_exact = chunk_patch_bytes.len().saturating_add(compact_manifest_bytes_exact);
    let compact_field_total_payload_exact = field_patch_bytes.len().saturating_add(compact_manifest_bytes_exact);

    let report = ApexMapPunctKindReport {
        input: args.r#in.clone(),
        normalized_len: norm.len(),
        punct_positions: punct.punct_lane.len(),
        kind_len: kinds.kind_lane.len(),

        majority_class: target_metrics.majority_class,
        majority_class_label: punct_kind_label(target_metrics.majority_class),
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
        chunk_mean_non_majority_macro_f1_pct: chunk_summary.mean_non_majority_macro_f1_pct,
        chunk_mean_abs_non_majority_delta: chunk_summary.mean_abs_non_majority_delta,
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
        term_margin_add: args.term_margin_add,
        pause_margin_add: args.pause_margin_add,
        wrap_margin_add: args.wrap_margin_add,
        term_share_ppm_min: args.term_share_ppm_min,
        pause_share_ppm_min: args.pause_share_ppm_min,
        wrap_share_ppm_min: args.wrap_share_ppm_min,

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
        field_pred_dominant_label: punct_kind_label(field_metrics.pred_dominant_class),
        field_pred_dominant_share_ppm: field_metrics.pred_dominant_share_ppm,
        field_pred_dominant_share_pct: field_metrics.pred_dominant_share_pct,
        field_pred_collapse_90_flag: field_metrics.pred_collapse_90_flag,

        field_precision_term_pct: field_metrics.per_class[0].precision_pct,
        field_recall_term_pct: field_metrics.per_class[0].recall_pct,
        field_f1_term_pct: field_metrics.per_class[0].f1_pct,
        field_precision_pause_pct: field_metrics.per_class[1].precision_pct,
        field_recall_pause_pct: field_metrics.per_class[1].recall_pct,
        field_f1_pause_pct: field_metrics.per_class[1].f1_pct,
        field_precision_wrap_pct: field_metrics.per_class[2].precision_pct,
        field_recall_wrap_pct: field_metrics.per_class[2].recall_pct,
        field_f1_wrap_pct: field_metrics.per_class[2].f1_pct,

        field_overrides: field_stats.overrides,
        field_boundary_count: field_stats.boundary_count,
        field_touched_positions: field_stats.touched_positions,
        field_term_applied: field_stats.applied_by_desired[0],
        field_pause_applied: field_stats.applied_by_desired[1],
        field_wrap_applied: field_stats.applied_by_desired[2],
        diag_rows: diag_rows.len(),

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

    Ok(ApexMapPunctKindRun {
        report,
        global: global.clone(),
        chunked,
        field_predicted,
        boundaries,
        map,
        compact_manifest,
        compact_manifest_bytes,
        diag_rows,
    })
}

fn refine_boundaries_with_diag(
    map: &ApexMap,
    base: &[u8],
    target: &[u8],
    boundaries: &[usize],
    cfg: RefineCfg,
    chunk_bytes: usize,
    diag_limit: usize,
) -> Result<(Vec<u8>, RefineStats, Vec<ApexMapDiagRow>)> {
    if base.len() != map.len() {
        return Err(anyhow!("apex-map-punct-kind: refine base length mismatch"));
    }
    if target.len() != map.len() {
        return Err(anyhow!("apex-map-punct-kind: refine target length mismatch"));
    }

    let mut out = base.to_vec();
    let mut stats = RefineStats::default();
    let mut diag_rows = Vec::new();
    let mut applied_counts = [0usize; 4];

    for &boundary in boundaries {
        if boundary == 0 || boundary >= map.len() {
            continue;
        }

        let pair = map.boundary_pair(boundary, cfg.delta)?;
        stats.boundary_count = stats.boundary_count.saturating_add(1);

        let left_from = boundary.saturating_sub(cfg.band);
        for pos in left_from..boundary {
            stats.touched_positions = stats.touched_positions.saturating_add(1);
            let trace = map.evaluate_override_with_budget(pos, out[pos], pair.left, cfg, &applied_counts)?;
            maybe_push_diag_row(&mut diag_rows, diag_limit, chunk_bytes, boundary, "left", target[pos], pair.left, pair.right, trace);
            if trace.applied() {
                out[pos] = pair.left;
                stats.overrides = stats.overrides.saturating_add(1);
                let slot = pair.left as usize;
                applied_counts[slot] = applied_counts[slot].saturating_add(1);
                stats.applied_by_desired[slot] = stats.applied_by_desired[slot].saturating_add(1);
            } else if trace.decision == k8dnz_apextrace::OverrideDecision::BudgetExceeded {
                let slot = pair.left as usize;
                stats.blocked_by_budget[slot] = stats.blocked_by_budget[slot].saturating_add(1);
            }
        }

        let right_to = boundary.saturating_add(cfg.band).min(map.len());
        for pos in boundary..right_to {
            stats.touched_positions = stats.touched_positions.saturating_add(1);
            let trace = map.evaluate_override_with_budget(pos, out[pos], pair.right, cfg, &applied_counts)?;
            maybe_push_diag_row(&mut diag_rows, diag_limit, chunk_bytes, boundary, "right", target[pos], pair.left, pair.right, trace);
            if trace.applied() {
                out[pos] = pair.right;
                stats.overrides = stats.overrides.saturating_add(1);
                let slot = pair.right as usize;
                applied_counts[slot] = applied_counts[slot].saturating_add(1);
                stats.applied_by_desired[slot] = stats.applied_by_desired[slot].saturating_add(1);
            } else if trace.decision == k8dnz_apextrace::OverrideDecision::BudgetExceeded {
                let slot = pair.right as usize;
                stats.blocked_by_budget[slot] = stats.blocked_by_budget[slot].saturating_add(1);
            }
        }
    }

    Ok((out, stats, diag_rows))
}

fn maybe_push_diag_row(
    rows: &mut Vec<ApexMapDiagRow>,
    diag_limit: usize,
    chunk_bytes: usize,
    boundary: usize,
    side: &'static str,
    target: u8,
    pair_left: u8,
    pair_right: u8,
    trace: OverrideTrace,
) {
    if diag_limit != 0 && rows.len() >= diag_limit {
        return;
    }
    if !trace.applied() && trace.current == target && trace.current == trace.desired {
        return;
    }

    rows.push(ApexMapDiagRow {
        chunk_bytes,
        boundary,
        side,
        pos: trace.pos,
        target,
        current: trace.current,
        desired: trace.desired,
        pair_left,
        pair_right,
        allow_mask: trace.allow_mask,
        desired_score: trace.desired_score,
        current_score: trace.current_score,
        needed_margin: trace.needed_margin,
        share_ppm: trace.share_ppm,
        share_floor: trace.share_floor,
        decision: trace.decision.as_str(),
        applied: trace.applied(),
    });
}

fn best_run_index(runs: &[ApexMapPunctKindRun]) -> usize {
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
        let value = token.parse::<usize>().with_context(|| format!("parse {} sweep value {}", label, token))?;
        if !out.contains(&value) {
            out.push(value);
        }
    }
    if out.is_empty() {
        return Err(anyhow!("apex-map-punct-kind: {} sweep produced no values", label));
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
        let value = token.parse::<u64>().with_context(|| format!("parse {} sweep value {}", label, token))?;
        if !out.contains(&value) {
            out.push(value);
        }
    }
    if out.is_empty() {
        return Err(anyhow!("apex-map-punct-kind: {} sweep produced no values", label));
    }
    Ok(out)
}

fn brute_force_best_symbol_lane(target: &[u8], cfg: SearchCfg, class_count: u8) -> Result<GenericLaneBest> {
    if cfg.seed_step == 0 {
        return Err(anyhow!("apex-map-punct-kind: seed_step must be >= 1"));
    }

    let byte_len = target.len() as u64;
    let mut best: Option<GenericLaneBest> = None;

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
            let cand = GenericLaneBest { key, predicted, score };
            match &best {
                None => best = Some(cand),
                Some(cur) if cand.score.better_than(&cur.score) => best = Some(cand),
                Some(_) => {}
            }
            i = i.saturating_add(1);
        }
    }

    best.ok_or_else(|| anyhow!("apex-map-punct-kind: search produced no candidates"))
}

fn brute_force_best_symbol_lane_chunked(
    target: &[u8],
    cfg: SearchCfg,
    chunk_bytes: usize,
    objective: ChunkSearchObjective,
    raw_slack: u64,
    class_count: u8,
) -> Result<(GenericLaneChunkedBest, ChunkSearchSummary)> {
    if chunk_bytes == 0 {
        return Err(anyhow!("apex-map-punct-kind: chunk_bytes must be >= 1"));
    }

    let mut predicted = Vec::with_capacity(target.len());
    let mut chunks = Vec::new();
    let mut start = 0usize;
    let mut chunk_index = 0usize;
    let mut sum_balanced_accuracy_pct = 0.0;
    let mut sum_macro_f1_pct = 0.0;
    let mut sum_non_majority_macro_f1_pct = 0.0;
    let mut sum_abs_non_majority_delta = 0.0;
    let mut majority_flip_count = 0usize;
    let mut collapse_90_count = 0usize;

    while start < target.len() {
        let end = start.saturating_add(chunk_bytes).min(target.len());
        let slice = &target[start..end];
        let (best, metrics) = brute_force_best_symbol_lane_objective(slice, cfg, objective, raw_slack, class_count)?;
        let patch = PatchList::from_pred_actual(&best.predicted, slice)
            .map_err(|e| anyhow!("apex-map-punct-kind chunk patch build failed: {e}"))?;
        let patch_bytes = patch.encode();

        sum_balanced_accuracy_pct += metrics.balanced_accuracy_pct;
        sum_macro_f1_pct += metrics.macro_f1_pct;
        sum_non_majority_macro_f1_pct += non_majority_macro_f1(&metrics, class_count);
        sum_abs_non_majority_delta += non_majority_delta_abs(&metrics, class_count) as f64;
        if metrics.pred_dominant_class != metrics.majority_class {
            majority_flip_count = majority_flip_count.saturating_add(1);
        }
        if metrics.pred_collapse_90_flag {
            collapse_90_count = collapse_90_count.saturating_add(1);
        }

        predicted.extend_from_slice(&best.predicted);
        chunks.push(GenericLaneChunkBest {
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
        GenericLaneChunkedBest {
            chunk_bytes,
            chunk_key_bytes_exact: chunks.len().saturating_mul(APEX_KEY_BYTES_EXACT),
            predicted,
            chunks,
        },
        ChunkSearchSummary {
            mean_balanced_accuracy_pct: sum_balanced_accuracy_pct / denom,
            mean_macro_f1_pct: sum_macro_f1_pct / denom,
            mean_non_majority_macro_f1_pct: sum_non_majority_macro_f1_pct / denom,
            mean_abs_non_majority_delta: sum_abs_non_majority_delta / denom,
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
) -> Result<(GenericLaneBest, SymbolMetrics)> {
    if cfg.seed_step == 0 {
        return Err(anyhow!("apex-map-punct-kind: seed_step must be >= 1"));
    }

    if objective == ChunkSearchObjective::Raw {
        let best = brute_force_best_symbol_lane(target, cfg, class_count)?;
        let metrics = compute_symbol_metrics(target, &best.predicted, class_count)?;
        return Ok((best, metrics));
    }

    let raw_anchor = brute_force_best_symbol_lane(target, cfg, class_count)?;
    let raw_anchor_matches = raw_anchor.score.matches;
    let byte_len = target.len() as u64;
    let mut best: Option<(GenericLaneBest, SymbolMetrics)> = None;

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

            if objective == ChunkSearchObjective::RawGuarded && raw_anchor_matches.saturating_sub(score.matches) > raw_slack {
                i = i.saturating_add(1);
                continue;
            }

            let metrics = compute_symbol_metrics(target, &predicted, class_count)?;
            let cand = GenericLaneBest { key, predicted, score };
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
    cand: &GenericLaneBest,
    cand_metrics: &SymbolMetrics,
    cur: &GenericLaneBest,
    cur_metrics: &SymbolMetrics,
    objective: ChunkSearchObjective,
    class_count: u8,
) -> bool {
    let cand_non_majority_delta_abs = non_majority_delta_abs(cand_metrics, class_count);
    let cur_non_majority_delta_abs = non_majority_delta_abs(cur_metrics, class_count);
    let cand_max_non_majority_delta_abs = max_non_majority_delta_abs(cand_metrics, class_count);
    let cur_max_non_majority_delta_abs = max_non_majority_delta_abs(cur_metrics, class_count);
    let cand_non_majority_macro_f1 = (non_majority_macro_f1(cand_metrics, class_count) * 1_000_000.0) as i64;
    let cur_non_majority_macro_f1 = (non_majority_macro_f1(cur_metrics, class_count) * 1_000_000.0) as i64;
    let cand_majority_flip = cand_metrics.pred_dominant_class != cand_metrics.majority_class;
    let cur_majority_flip = cur_metrics.pred_dominant_class != cur_metrics.majority_class;

    let cand_key = (
        cand_metrics.pred_collapse_90_flag,
        cand_majority_flip,
        cand_non_majority_delta_abs,
        cand_max_non_majority_delta_abs,
        std::cmp::Reverse(cand_non_majority_macro_f1),
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
        cur_non_majority_delta_abs,
        cur_max_non_majority_delta_abs,
        std::cmp::Reverse(cur_non_majority_macro_f1),
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

fn chunk_search_objective_name(objective: ChunkSearchObjective) -> &'static str {
    match objective {
        ChunkSearchObjective::Raw => "raw",
        ChunkSearchObjective::RawGuarded => "raw-guarded",
        ChunkSearchObjective::Honest => "honest",
        ChunkSearchObjective::Newline => "newline-as-honest",
    }
}

fn non_majority_delta_abs(metrics: &SymbolMetrics, class_count: u8) -> i64 {
    let mut out = 0i64;
    for cls in 0..class_count as usize {
        if cls == metrics.majority_class as usize {
            continue;
        }
        out += ((metrics.pred_hist[cls] as i64) - (metrics.target_hist[cls] as i64)).abs();
    }
    out
}

fn max_non_majority_delta_abs(metrics: &SymbolMetrics, class_count: u8) -> i64 {
    let mut best = 0i64;
    for cls in 0..class_count as usize {
        if cls == metrics.majority_class as usize {
            continue;
        }
        let delta = ((metrics.pred_hist[cls] as i64) - (metrics.target_hist[cls] as i64)).abs();
        best = best.max(delta);
    }
    best
}

fn non_majority_macro_f1(metrics: &SymbolMetrics, class_count: u8) -> f64 {
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
    if count == 0 { 0.0 } else { sum / (count as f64) }
}

fn score_symbol_lane(target: &[u8], predicted: &[u8], class_count: u8) -> Result<GenericLaneScore> {
    if target.len() != predicted.len() {
        return Err(anyhow!(
            "apex-map-punct-kind: target len {} != predicted len {}",
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

    Ok(GenericLaneScore {
        matches,
        prefix,
        total: target.len() as u64,
        longest_run,
        longest_run_start,
    })
}

fn save_outputs(
    out_key: Option<&str>,
    out_pred: Option<&str>,
    global: &GenericLaneBest,
    chunked: &GenericLaneChunkedBest,
    field_predicted: &[u8],
    boundaries: &[usize],
    map: &ApexMap,
    boundary_delta: usize,
    compact_manifest: &CompactChunkManifest,
    compact_manifest_bytes: &[u8],
) -> Result<()> {
    if let Some(path) = out_key {
        let enc = global.key.encode()?;
        std::fs::write(path, enc).with_context(|| format!("write {}", path))?;
        eprintln!("saved apex-map-punct-kind global key: {}", path);

        let chunk_csv = render_chunk_keys_csv(chunked.chunk_bytes, &chunked.chunks);
        let chunk_path = format!("{}.chunks.csv", path);
        std::fs::write(&chunk_path, chunk_csv.as_bytes()).with_context(|| format!("write {}", chunk_path))?;
        eprintln!("saved apex-map-punct-kind chunk manifest: {}", chunk_path);

        let compact_csv = render_compact_manifest_csv(compact_manifest);
        let compact_csv_path = format!("{}.compact.csv", path);
        std::fs::write(&compact_csv_path, compact_csv.as_bytes())
            .with_context(|| format!("write {}", compact_csv_path))?;
        eprintln!("saved apex-map-punct-kind compact manifest csv: {}", compact_csv_path);

        let compact_bin_path = format!("{}.compact.bin", path);
        std::fs::write(&compact_bin_path, compact_manifest_bytes)
            .with_context(|| format!("write {}", compact_bin_path))?;
        eprintln!("saved apex-map-punct-kind compact manifest bin: {}", compact_bin_path);

        let mut boundary_csv = String::from("boundary,left,right,left_pos,right_pos,node_count,max_depth_seen\n");
        for &boundary in boundaries {
            let pair = map.boundary_pair(boundary, boundary_delta)?;
            let left_pos = boundary.saturating_sub(boundary_delta.max(1));
            let right_pos = boundary
                .saturating_add(boundary_delta.max(1).saturating_sub(1))
                .min(map.len().saturating_sub(1));
            boundary_csv.push_str(&format!(
                "{},{},{},{},{},{},{}\n",
                boundary,
                pair.left,
                pair.right,
                left_pos,
                right_pos,
                map.node_count(),
                map.max_depth_seen(),
            ));
        }
        let boundary_path = format!("{}.boundaries.csv", path);
        std::fs::write(&boundary_path, boundary_csv.as_bytes())
            .with_context(|| format!("write {}", boundary_path))?;
        eprintln!("saved apex-map-punct-kind boundary pairs: {}", boundary_path);
    }

    if let Some(path) = out_pred {
        let global_ascii = render_kind_ascii(&global.predicted);
        let global_path = format!("{}.global.txt", path);
        std::fs::write(&global_path, global_ascii.as_bytes()).with_context(|| format!("write {}", global_path))?;
        eprintln!("saved apex-map-punct-kind global kind lane: {}", global_path);

        let chunked_ascii = render_kind_ascii(&chunked.predicted);
        let chunked_path = format!("{}.chunked.txt", path);
        std::fs::write(&chunked_path, chunked_ascii.as_bytes()).with_context(|| format!("write {}", chunked_path))?;
        eprintln!("saved apex-map-punct-kind chunked kind lane: {}", chunked_path);

        let field_ascii = render_kind_ascii(field_predicted);
        let field_path = format!("{}.field.txt", path);
        std::fs::write(&field_path, field_ascii.as_bytes()).with_context(|| format!("write {}", field_path))?;
        eprintln!("saved apex-map-punct-kind field-refined kind lane: {}", field_path);
    }

    Ok(())
}

fn render_chunk_keys_csv(chunk_bytes: usize, chunks: &[GenericLaneChunkBest]) -> String {
    let mut out = String::from("chunk_bytes,chunk_index,start,end,root_quadrant,root_seed_hex,recipe_seed_hex,patch_entries,patch_bytes\n");
    for chunk in chunks {
        out.push_str(&format!(
            "{},{},{},{},{},0x{:016X},0x{:016X},{},{}\n",
            chunk_bytes,
            chunk.chunk_index,
            chunk.start,
            chunk.end,
            chunk.key.root_quadrant,
            chunk.key.root_seed,
            chunk.key.recipe_seed,
            chunk.patch_entries,
            chunk.patch_bytes,
        ));
    }
    out
}

fn render_kind_ascii(predicted: &[u8]) -> String {
    let mut out = String::with_capacity(predicted.len());
    for &v in predicted {
        let ch = match v {
            0 => 't',
            1 => 'p',
            2 => 'w',
            _ => '?',
        };
        out.push(ch);
    }
    out
}

fn render_reports_txt(runs: &[ApexMapPunctKindRun]) -> String {
    let mut out = String::new();
    for (idx, run) in runs.iter().enumerate() {
        if idx > 0 {
            out.push('\n');
        }
        out.push_str(&render_report_txt(&run.report));
    }
    out
}

fn render_report_txt(row: &ApexMapPunctKindReport) -> String {
    let mut out = String::new();
    out.push_str(&format!("input={}\n", row.input));
    out.push_str(&format!(
        "normalized_len={} punct_positions={} kind_len={}\n",
        row.normalized_len,
        row.punct_positions,
        row.kind_len,
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
        "chunk_bytes={} chunk_count={} chunk_patch_entries={} chunk_patch_bytes={} chunk_total_payload_exact={} compact_manifest_bytes_exact={} compact_chunk_total_payload_exact={} chunk_match_pct={:.6} chunk_match_vs_majority_pct={:.6} chunk_balanced_accuracy_pct={:.6} chunk_macro_f1_pct={:.6} chunk_search_objective={} chunk_raw_slack={} chunk_mean_balanced_accuracy_pct={:.6} chunk_mean_macro_f1_pct={:.6} chunk_mean_non_majority_macro_f1_pct={:.6} chunk_mean_abs_non_majority_delta={:.6} chunk_majority_flip_count={} chunk_collapse_90_count={}\n",
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
        row.chunk_mean_non_majority_macro_f1_pct,
        row.chunk_mean_abs_non_majority_delta,
        row.chunk_majority_flip_count,
        row.chunk_collapse_90_count,
    ));
    out.push_str(&format!(
        "field_source={} map_node_count={} map_depth_seen={} map_depth_shift={} map_max_depth_arg={} boundary_band={} boundary_delta={} field_margin={} term_margin_add={} pause_margin_add={} wrap_margin_add={} term_share_ppm_min={} pause_share_ppm_min={} wrap_share_ppm_min={}\n",
        row.field_source,
        row.map_node_count,
        row.map_depth_seen,
        row.map_depth_shift,
        row.map_max_depth_arg,
        row.boundary_band,
        row.boundary_delta,
        row.field_margin,
        row.term_margin_add,
        row.pause_margin_add,
        row.wrap_margin_add,
        row.term_share_ppm_min,
        row.pause_share_ppm_min,
        row.wrap_share_ppm_min,
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
        "field_precision_term_pct={:.6} field_recall_term_pct={:.6} field_f1_term_pct={:.6} field_precision_pause_pct={:.6} field_recall_pause_pct={:.6} field_f1_pause_pct={:.6} field_precision_wrap_pct={:.6} field_recall_wrap_pct={:.6} field_f1_wrap_pct={:.6}\n",
        row.field_precision_term_pct,
        row.field_recall_term_pct,
        row.field_f1_term_pct,
        row.field_precision_pause_pct,
        row.field_recall_pause_pct,
        row.field_f1_pause_pct,
        row.field_precision_wrap_pct,
        row.field_recall_wrap_pct,
        row.field_f1_wrap_pct,
    ));
    out.push_str(&format!(
        "field_overrides={} field_boundary_count={} field_touched_positions={} field_term_applied={} field_pause_applied={} field_wrap_applied={} diag_rows={}\n",
        row.field_overrides,
        row.field_boundary_count,
        row.field_touched_positions,
        row.field_term_applied,
        row.field_pause_applied,
        row.field_wrap_applied,
        row.diag_rows,
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

fn render_reports_csv(runs: &[ApexMapPunctKindRun]) -> String {
    let mut out = String::from(
        "input,normalized_len,punct_positions,kind_len,majority_class,majority_class_label,majority_count,majority_baseline_match_pct,target_entropy_bits,global_patch_entries,global_patch_bytes,global_total_payload_exact,global_match_pct,global_match_vs_majority_pct,global_balanced_accuracy_pct,global_macro_f1_pct,chunk_bytes,chunk_count,chunk_patch_entries,chunk_patch_bytes,chunk_total_payload_exact,compact_manifest_bytes_exact,compact_chunk_total_payload_exact,chunk_match_pct,chunk_match_vs_majority_pct,chunk_balanced_accuracy_pct,chunk_macro_f1_pct,chunk_search_objective,chunk_raw_slack,chunk_mean_balanced_accuracy_pct,chunk_mean_macro_f1_pct,chunk_mean_non_majority_macro_f1_pct,chunk_mean_abs_non_majority_delta,chunk_majority_flip_count,chunk_collapse_90_count,field_source,map_node_count,map_depth_seen,map_depth_shift,map_max_depth_arg,boundary_band,boundary_delta,field_margin,term_margin_add,pause_margin_add,wrap_margin_add,term_share_ppm_min,pause_share_ppm_min,wrap_share_ppm_min,field_patch_entries,field_patch_bytes,field_total_payload_exact,compact_field_total_payload_exact,field_match_pct,field_match_vs_majority_pct,field_balanced_accuracy_pct,field_macro_precision_pct,field_macro_recall_pct,field_macro_f1_pct,field_weighted_f1_pct,field_pred_entropy_bits,field_hist_l1_pct,field_pred_dominant_class,field_pred_dominant_label,field_pred_dominant_share_ppm,field_pred_dominant_share_pct,field_pred_collapse_90_flag,field_f1_term_pct,field_f1_pause_pct,field_f1_wrap_pct,field_overrides,field_boundary_count,field_touched_positions,field_term_applied,field_pause_applied,field_wrap_applied,diag_rows,delta_field_patch_vs_global,delta_field_patch_vs_chunked,delta_field_total_vs_global,delta_field_total_vs_chunked,delta_compact_chunk_total_vs_global,delta_compact_field_total_vs_global,delta_compact_field_total_vs_chunked,delta_compact_field_total_vs_compact_chunked,target_hist_0,target_hist_1,target_hist_2,target_hist_3,field_pred_hist_0,field_pred_hist_1,field_pred_hist_2,field_pred_hist_3\n"
    );
    for run in runs {
        let row = &run.report;
        let cells = vec![
            csv_escape(&row.input),
            row.normalized_len.to_string(),
            row.punct_positions.to_string(),
            row.kind_len.to_string(),
            row.majority_class.to_string(),
            punct_kind_label(row.majority_class).to_string(),
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
            fmt6(row.chunk_mean_non_majority_macro_f1_pct),
            fmt6(row.chunk_mean_abs_non_majority_delta),
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
            row.term_margin_add.to_string(),
            row.pause_margin_add.to_string(),
            row.wrap_margin_add.to_string(),
            row.term_share_ppm_min.to_string(),
            row.pause_share_ppm_min.to_string(),
            row.wrap_share_ppm_min.to_string(),
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
            punct_kind_label(row.field_pred_dominant_class).to_string(),
            row.field_pred_dominant_share_ppm.to_string(),
            fmt6(row.field_pred_dominant_share_pct),
            row.field_pred_collapse_90_flag.to_string(),
            fmt6(row.field_f1_term_pct),
            fmt6(row.field_f1_pause_pct),
            fmt6(row.field_f1_wrap_pct),
            row.field_overrides.to_string(),
            row.field_boundary_count.to_string(),
            row.field_touched_positions.to_string(),
            row.field_term_applied.to_string(),
            row.field_pause_applied.to_string(),
            row.field_wrap_applied.to_string(),
            row.diag_rows.to_string(),
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

fn render_diag_csv(runs: &[ApexMapPunctKindRun]) -> String {
    let mut out = String::from("input,chunk_bytes,boundary,side,pos,target,current,desired,pair_left,pair_right,allow_mask,desired_score,current_score,needed_margin,share_ppm,share_floor,decision,applied\n");
    for run in runs {
        for row in &run.diag_rows {
            out.push_str(&format!(
                "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n",
                csv_escape(&run.report.input),
                row.chunk_bytes,
                row.boundary,
                row.side,
                row.pos,
                row.target,
                row.current,
                row.desired,
                row.pair_left,
                row.pair_right,
                row.allow_mask,
                row.desired_score,
                row.current_score,
                row.needed_margin,
                row.share_ppm,
                row.share_floor,
                row.decision,
                row.applied,
            ));
        }
    }
    out
}

fn print_summary(out_path: Option<&str>, format: RenderFormat, runs: &[ApexMapPunctKindRun], best_idx: usize) {
    let best = &runs[best_idx].report;
    match out_path {
        Some(path) => {
            eprintln!(
                "apextrace apex-map-punct-kind {:?} saved: {} (runs={} best_chunk={} best_boundary_band={} best_field_margin={} compact_field_total_payload_exact={} field_match_pct={:.6})",
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
                "apextrace apex-map-punct-kind {:?}: runs={} best_chunk={} best_boundary_band={} best_field_margin={} compact_field_total_payload_exact={} field_match_pct={:.6}",
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
        Err(anyhow!("apex-map-punct-kind: invalid class symbol {}", v))
    }
}

#[inline]
fn bucket_u8_local(b: u8, k: u8) -> u8 {
    ((b as u16 * k as u16) >> 8) as u8
}