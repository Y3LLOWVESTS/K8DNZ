
use anyhow::{anyhow, Context, Result};
use k8dnz_apextrace::{
    generate_bytes, ApexKey, ApexMap, ApexMapCfg, OverrideTrace, RefineCfg, RefineStats, SearchCfg,
};
use k8dnz_core::lane;
use k8dnz_core::repr::{text_norm, ws_lanes::WsLanes};
use k8dnz_core::symbol::patch::PatchList;

use crate::cmd::apextrace::{ApexMapLaneArgs, ChunkSearchObjective, RenderFormat};
use crate::io::recipe_file;

use super::class_metrics::{class_label, compute_lane_class_metrics};
use super::common::{decode_k8l1_view_any, patch_count, write_or_print};
use super::compact_manifest::{render_compact_manifest_csv, CompactChunkManifest};
use super::ws_lane_render::{render_ws_class_ascii, render_ws_lane_chunk_keys_csv};
use super::ws_lane_types::{
    WsLaneBest, WsLaneChunkBest, WsLaneChunkedBest, WsLaneDiagnostics, WsLaneScore,
};

const APEX_KEY_BYTES_EXACT: usize = 48;

#[derive(Clone, Debug)]
struct ApexMapLaneReport {
    input: String,
    recipe: String,
    normalized_len: usize,
    class_len: usize,
    other_len: usize,

    baseline_artifact_bytes: usize,
    baseline_max_ticks_used: u64,
    baseline_class_mismatches: usize,
    baseline_class_patch_entries: usize,
    baseline_class_patch_bytes: usize,

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
    global_pred_entropy_bits: f64,
    global_hist_l1_pct: f64,

    chunk_bytes: usize,
    chunk_count: usize,
    chunk_patch_entries: usize,
    chunk_patch_bytes: usize,
    chunk_total_payload_exact: usize,
    chunk_match_pct: f64,
    chunk_match_vs_majority_pct: f64,
    chunk_balanced_accuracy_pct: f64,
    chunk_macro_f1_pct: f64,
    chunk_pred_entropy_bits: f64,
    chunk_hist_l1_pct: f64,
    chunk_search_objective: String,
    chunk_raw_slack: u64,
    chunk_mean_balanced_accuracy_pct: f64,
    chunk_mean_macro_f1_pct: f64,
    chunk_mean_newline_f1_pct: f64,
    chunk_mean_abs_newline_delta: f64,
    chunk_mean_abs_minority_delta: f64,
    chunk_majority_flip_count: usize,
    chunk_collapse_90_count: usize,

    compact_manifest_bytes_exact: usize,
    compact_chunk_total_payload_exact: usize,

    field_source: String,
    map_node_count: usize,
    map_depth_seen: u8,
    map_depth_shift: u8,
    map_max_depth_arg: u8,
    boundary_band: usize,
    boundary_delta: usize,
    field_margin: u64,
    newline_margin_add: u64,
    space_to_newline_margin_add: u64,
    newline_share_ppm_min: u32,
    newline_override_budget: usize,
    newline_demote_margin: u64,
    newline_demote_keep_ppm_min: u32,
    newline_demote_keep_min: usize,
    newline_only_from_spacelike: bool,

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

    field_precision_other_pct: f64,
    field_recall_other_pct: f64,
    field_f1_other_pct: f64,
    field_precision_space_pct: f64,
    field_recall_space_pct: f64,
    field_f1_space_pct: f64,
    field_precision_newline_pct: f64,
    field_recall_newline_pct: f64,
    field_f1_newline_pct: f64,

    field_conf_t0_p0: u64,
    field_conf_t0_p1: u64,
    field_conf_t0_p2: u64,
    field_conf_t1_p0: u64,
    field_conf_t1_p1: u64,
    field_conf_t1_p2: u64,
    field_conf_t2_p0: u64,
    field_conf_t2_p1: u64,
    field_conf_t2_p2: u64,

    field_overrides: usize,
    field_boundary_count: usize,
    field_touched_positions: usize,
    field_newline_applied: usize,
    field_newline_budget_blocked: usize,
    field_newline_demoted: usize,
    field_newline_before_demote: usize,
    field_newline_after_demote: usize,
    field_newline_floor_used: usize,
    field_newline_extinct_flag: bool,
    newline_diag_rows: usize,

    delta_field_patch_vs_baseline: i64,
    delta_field_patch_vs_global: i64,
    delta_field_patch_vs_chunked: i64,
    delta_field_total_vs_global: i64,
    delta_field_total_vs_chunked: i64,
    delta_compact_chunk_total_vs_global: i64,
    delta_compact_field_total_vs_global: i64,
    delta_compact_field_total_vs_chunked: i64,
    delta_compact_field_total_vs_compact_chunked: i64,

    target_hist: [u64; 3],
    global_pred_hist: [u64; 3],
    chunk_pred_hist: [u64; 3],
    field_pred_hist: [u64; 3],

    global_pred_newline_delta: i64,
    chunk_pred_newline_delta: i64,
    field_pred_newline_delta: i64,
}

#[derive(Clone, Debug)]
struct ApexMapLaneRun {
    report: ApexMapLaneReport,
    global: WsLaneBest,
    chunked: WsLaneChunkedBest,
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

#[derive(Clone, Copy, Debug)]
struct ChunkSearchSummary {
    mean_balanced_accuracy_pct: f64,
    mean_macro_f1_pct: f64,
    mean_newline_f1_pct: f64,
    mean_abs_newline_delta: f64,
    mean_abs_minority_delta: f64,
    majority_flip_count: usize,
    collapse_90_count: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct NewlineDemotionStats {
    predicted: Vec<u8>,
    before_count: usize,
    after_count: usize,
    floor_used: usize,
    demoted: usize,
    extinct_flag: bool,
}

pub fn run_apex_map_lane(args: ApexMapLaneArgs) -> Result<()> {
    let chunk_values = parse_chunk_values(args.chunk_sweep.as_deref(), args.chunk_bytes)?;
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
    let newline_demote_margin_values = parse_u64_sweep_values(
        args.newline_demote_margin_sweep.as_deref(),
        args.newline_demote_margin,
        "newline_demote_margin",
    )?;
    let input = std::fs::read(&args.r#in).with_context(|| format!("read {}", args.r#in))?;
    let recipe_bytes = recipe_file::load_k8r_bytes(&args.recipe)
        .with_context(|| format!("load recipe {}", args.recipe))?;

    let norm = text_norm::normalize_newlines(&input);
    let ws = WsLanes::split(&norm);

    let (artifact, baseline_stats, baseline_ticks_used) =
        run_baseline_k8l1(&input, &recipe_bytes, args.max_ticks)?;
    let view = decode_k8l1_view_any(&artifact)?;
    let baseline_class_patch_entries = patch_count(&view.class_patch)?;
    let baseline_class_patch_bytes = view.class_patch.len();

    let cfg = SearchCfg {
        seed_from: args.seed_from,
        seed_count: args.seed_count,
        seed_step: args.seed_step,
        recipe_seed: args.recipe_seed,
    };

    let global = brute_force_best_ws_lane(&ws.class_lane, cfg)?;
    let global_patch = PatchList::from_pred_actual(&global.predicted, &ws.class_lane)
        .map_err(|e| anyhow!("apex-map-lane global patch build failed: {e}"))?;
    let global_patch_bytes = global_patch.encode();
    let global_total_payload_exact = global_patch_bytes.len().saturating_add(APEX_KEY_BYTES_EXACT);
    let global_patch_entries = global_patch.entries.len();

    let mut runs = Vec::with_capacity(
        chunk_values
            .len()
            .saturating_mul(boundary_band_values.len())
            .saturating_mul(field_margin_values.len())
            .saturating_mul(newline_demote_margin_values.len()),
    );
    for chunk_bytes in chunk_values {
        for &boundary_band in &boundary_band_values {
            for &field_margin in &field_margin_values {
                for &newline_demote_margin in &newline_demote_margin_values {
                runs.push(run_apex_map_lane_once(
                    &args,
                    &norm,
                    &ws,
                    artifact.len(),
                    baseline_ticks_used,
                    baseline_stats.class_mismatches,
                    baseline_class_patch_entries,
                    baseline_class_patch_bytes,
                    &global,
                    global_patch_entries,
                    &global_patch_bytes,
                    global_total_payload_exact,
                    cfg,
                    chunk_bytes,
                    boundary_band,
                    field_margin,
                    newline_demote_margin,
                )?);
                }
            }
        }
    }

    if runs.is_empty() {
        return Err(anyhow!("apex-map-lane: no chunk sizes to run"));
    }

    let best_idx = best_run_index(&runs);
    let best = &runs[best_idx];

    if args.out_key.is_some() || args.out_pred.is_some() {
        if runs.len() > 1 {
            eprintln!(
                "apex-map-lane sweep selected best output run: chunk_bytes={} boundary_band={} field_margin={} compact_field_total_payload_exact={}",
                best.report.chunk_bytes,
                best.report.boundary_band,
                best.report.field_margin,
                best.report.compact_field_total_payload_exact
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
            "apextrace apex-map-lane newline diagnostics saved: {} rows={}",
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

fn run_apex_map_lane_once(
    args: &ApexMapLaneArgs,
    norm: &[u8],
    ws: &WsLanes,
    baseline_artifact_bytes: usize,
    baseline_max_ticks_used: u64,
    baseline_class_mismatches: usize,
    baseline_class_patch_entries: usize,
    baseline_class_patch_bytes: usize,
    global: &WsLaneBest,
    global_patch_entries: usize,
    global_patch_bytes: &[u8],
    global_total_payload_exact: usize,
    cfg: SearchCfg,
    chunk_bytes: usize,
    boundary_band: usize,
    field_margin: u64,
    newline_demote_margin: u64,
) -> Result<ApexMapLaneRun> {
    let target_metrics = compute_lane_class_metrics(&ws.class_lane, &ws.class_lane)?;

    let (chunked, chunk_summary) = brute_force_best_ws_lane_chunked(
        &ws.class_lane,
        cfg,
        chunk_bytes,
        args.chunk_search_objective,
        args.chunk_raw_slack,
    )?;
    let chunk_patch = PatchList::from_pred_actual(&chunked.predicted, &ws.class_lane)
        .map_err(|e| anyhow!("apex-map-lane chunked patch build failed: {e}"))?;
    let chunk_patch_bytes = chunk_patch.encode();

    let field_source = if args.field_from_global {
        global.predicted.clone()
    } else {
        chunked.predicted.clone()
    };

    let map = ApexMap::from_symbols(
        &field_source,
        ApexMapCfg {
            class_count: 3,
            max_depth: args.map_max_depth,
            depth_shift: args.map_depth_shift,
        },
    )?;

    let boundaries = chunked
        .chunks
        .iter()
        .skip(1)
        .map(|chunk| chunk.start)
        .collect::<Vec<_>>();

    let mut refine_cfg =
        RefineCfg::new(boundary_band, args.boundary_delta, field_margin);
    refine_cfg.desired_margin_add[2] = args.newline_margin_add;
    refine_cfg.transition_margin_add[1][2] = args.space_to_newline_margin_add;
    refine_cfg.dominant_share_ppm_min[2] = args.newline_share_ppm_min;
    refine_cfg.desired_apply_budget[2] = args.newline_override_budget;
    if args.newline_only_from_spacelike {
        refine_cfg.desired_from_mask[2] = (1 << 1) | (1 << 2);
    }

    let (field_predicted_refined, field_stats, diag_rows) = refine_boundaries_with_diag(
        &map,
        &chunked.predicted,
        &ws.class_lane,
        &boundaries,
        refine_cfg,
        chunk_bytes,
        args.diag_limit,
    )?;

    let demotion_stats = if newline_demote_margin == 0 {
        NewlineDemotionStats {
            predicted: field_predicted_refined.clone(),
            before_count: field_predicted_refined.iter().filter(|&&v| v == 2).count(),
            after_count: field_predicted_refined.iter().filter(|&&v| v == 2).count(),
            floor_used: 0,
            demoted: 0,
            extinct_flag: false,
        }
    } else {
        demote_surviving_newlines_capped(
            &map,
            &field_predicted_refined,
            newline_demote_margin,
            args.newline_demote_keep_ppm_min,
            args.newline_demote_keep_min,
        )?
    };
    let field_predicted = demotion_stats.predicted.clone();

    let global_metrics = compute_lane_class_metrics(&ws.class_lane, &global.predicted)?;
    let chunk_metrics = compute_lane_class_metrics(&ws.class_lane, &chunked.predicted)?;
    let field_metrics = compute_lane_class_metrics(&ws.class_lane, &field_predicted)?;

    let field_patch = PatchList::from_pred_actual(&field_predicted, &ws.class_lane)
        .map_err(|e| anyhow!("apex-map-lane field patch build failed: {e}"))?;
    let field_patch_bytes = field_patch.encode();

    let compact_manifest = CompactChunkManifest::from_chunked(&chunked)?;
    let compact_manifest_bytes = compact_manifest.encode();
    let compact_manifest_decoded = CompactChunkManifest::decode(&compact_manifest_bytes)?;
    if compact_manifest_decoded != compact_manifest {
        return Err(anyhow!("compact manifest roundtrip mismatch"));
    }

    let chunk_total_payload_exact = chunk_patch_bytes
        .len()
        .saturating_add(chunked.chunk_key_bytes_exact);
    let field_total_payload_exact = field_patch_bytes
        .len()
        .saturating_add(chunked.chunk_key_bytes_exact);

    let compact_manifest_bytes_exact = compact_manifest_bytes.len();
    let compact_chunk_total_payload_exact = chunk_patch_bytes
        .len()
        .saturating_add(compact_manifest_bytes_exact);
    let compact_field_total_payload_exact = field_patch_bytes
        .len()
        .saturating_add(compact_manifest_bytes_exact);

    let report = ApexMapLaneReport {
        input: args.r#in.clone(),
        recipe: args.recipe.clone(),
        normalized_len: norm.len(),
        class_len: ws.class_lane.len(),
        other_len: ws.other_lane.len(),

        baseline_artifact_bytes,
        baseline_max_ticks_used,
        baseline_class_mismatches,
        baseline_class_patch_entries,
        baseline_class_patch_bytes,

        majority_class: target_metrics.majority_class,
        majority_class_label: class_label(target_metrics.majority_class),
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
        global_pred_entropy_bits: global_metrics.pred_entropy_bits,
        global_hist_l1_pct: global_metrics.hist_l1_pct,

        chunk_bytes,
        chunk_count: chunked.chunks.len(),
        chunk_patch_entries: chunk_patch.entries.len(),
        chunk_patch_bytes: chunk_patch_bytes.len(),
        chunk_total_payload_exact,
        chunk_match_pct: chunk_metrics.raw_match_pct,
        chunk_match_vs_majority_pct: chunk_metrics.raw_match_vs_majority_pct,
        chunk_balanced_accuracy_pct: chunk_metrics.balanced_accuracy_pct,
        chunk_macro_f1_pct: chunk_metrics.macro_f1_pct,
        chunk_pred_entropy_bits: chunk_metrics.pred_entropy_bits,
        chunk_hist_l1_pct: chunk_metrics.hist_l1_pct,
        chunk_search_objective: chunk_search_objective_name(args.chunk_search_objective).to_string(),
        chunk_raw_slack: args.chunk_raw_slack,
        chunk_mean_balanced_accuracy_pct: chunk_summary.mean_balanced_accuracy_pct,
        chunk_mean_macro_f1_pct: chunk_summary.mean_macro_f1_pct,
        chunk_mean_newline_f1_pct: chunk_summary.mean_newline_f1_pct,
        chunk_mean_abs_newline_delta: chunk_summary.mean_abs_newline_delta,
        chunk_mean_abs_minority_delta: chunk_summary.mean_abs_minority_delta,
        chunk_majority_flip_count: chunk_summary.majority_flip_count,
        chunk_collapse_90_count: chunk_summary.collapse_90_count,

        compact_manifest_bytes_exact,
        compact_chunk_total_payload_exact,

        field_source: if args.field_from_global {
            "global".to_string()
        } else {
            "chunked".to_string()
        },
        map_node_count: map.node_count(),
        map_depth_seen: map.max_depth_seen(),
        map_depth_shift: args.map_depth_shift,
        map_max_depth_arg: args.map_max_depth,
        boundary_band,
        boundary_delta: args.boundary_delta,
        field_margin,
        newline_margin_add: args.newline_margin_add,
        space_to_newline_margin_add: args.space_to_newline_margin_add,
        newline_share_ppm_min: args.newline_share_ppm_min,
        newline_override_budget: args.newline_override_budget,
        newline_demote_margin,
        newline_demote_keep_ppm_min: args.newline_demote_keep_ppm_min,
        newline_demote_keep_min: args.newline_demote_keep_min,
        newline_only_from_spacelike: args.newline_only_from_spacelike,

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
        field_pred_dominant_label: class_label(field_metrics.pred_dominant_class),
        field_pred_dominant_share_ppm: field_metrics.pred_dominant_share_ppm,
        field_pred_dominant_share_pct: field_metrics.pred_dominant_share_pct,
        field_pred_collapse_90_flag: field_metrics.pred_collapse_90_flag,

        field_precision_other_pct: field_metrics.per_class[0].precision_pct,
        field_recall_other_pct: field_metrics.per_class[0].recall_pct,
        field_f1_other_pct: field_metrics.per_class[0].f1_pct,
        field_precision_space_pct: field_metrics.per_class[1].precision_pct,
        field_recall_space_pct: field_metrics.per_class[1].recall_pct,
        field_f1_space_pct: field_metrics.per_class[1].f1_pct,
        field_precision_newline_pct: field_metrics.per_class[2].precision_pct,
        field_recall_newline_pct: field_metrics.per_class[2].recall_pct,
        field_f1_newline_pct: field_metrics.per_class[2].f1_pct,

        field_conf_t0_p0: field_metrics.confusion[0][0],
        field_conf_t0_p1: field_metrics.confusion[0][1],
        field_conf_t0_p2: field_metrics.confusion[0][2],
        field_conf_t1_p0: field_metrics.confusion[1][0],
        field_conf_t1_p1: field_metrics.confusion[1][1],
        field_conf_t1_p2: field_metrics.confusion[1][2],
        field_conf_t2_p0: field_metrics.confusion[2][0],
        field_conf_t2_p1: field_metrics.confusion[2][1],
        field_conf_t2_p2: field_metrics.confusion[2][2],

        field_overrides: field_stats.overrides,
        field_boundary_count: field_stats.boundary_count,
        field_touched_positions: field_stats.touched_positions,
        field_newline_applied: field_stats.applied_by_desired[2],
        field_newline_budget_blocked: field_stats.blocked_by_budget[2],
        field_newline_demoted: demotion_stats.demoted,
        field_newline_before_demote: demotion_stats.before_count,
        field_newline_after_demote: demotion_stats.after_count,
        field_newline_floor_used: demotion_stats.floor_used,
        field_newline_extinct_flag: demotion_stats.extinct_flag,
        newline_diag_rows: diag_rows.len(),

        delta_field_patch_vs_baseline: (field_patch_bytes.len() as i64)
            - (baseline_class_patch_bytes as i64),
        delta_field_patch_vs_global: (field_patch_bytes.len() as i64)
            - (global_patch_bytes.len() as i64),
        delta_field_patch_vs_chunked: (field_patch_bytes.len() as i64)
            - (chunk_patch_bytes.len() as i64),
        delta_field_total_vs_global: (field_total_payload_exact as i64)
            - (global_total_payload_exact as i64),
        delta_field_total_vs_chunked: (field_total_payload_exact as i64)
            - (chunk_total_payload_exact as i64),
        delta_compact_chunk_total_vs_global: (compact_chunk_total_payload_exact as i64)
            - (global_total_payload_exact as i64),
        delta_compact_field_total_vs_global: (compact_field_total_payload_exact as i64)
            - (global_total_payload_exact as i64),
        delta_compact_field_total_vs_chunked: (compact_field_total_payload_exact as i64)
            - (chunk_total_payload_exact as i64),
        delta_compact_field_total_vs_compact_chunked: (compact_field_total_payload_exact as i64)
            - (compact_chunk_total_payload_exact as i64),

        target_hist: field_metrics.target_hist,
        global_pred_hist: global_metrics.pred_hist,
        chunk_pred_hist: chunk_metrics.pred_hist,
        field_pred_hist: field_metrics.pred_hist,

        global_pred_newline_delta: (global_metrics.pred_hist[2] as i64)
            - (field_metrics.target_hist[2] as i64),
        chunk_pred_newline_delta: (chunk_metrics.pred_hist[2] as i64)
            - (field_metrics.target_hist[2] as i64),
        field_pred_newline_delta: (field_metrics.pred_hist[2] as i64)
            - (field_metrics.target_hist[2] as i64),
    };

    Ok(ApexMapLaneRun {
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

fn parse_chunk_values(chunk_sweep: Option<&str>, chunk_bytes: usize) -> Result<Vec<usize>> {
    if chunk_bytes == 0 {
        return Err(anyhow!("apex-map-lane: chunk_bytes must be >= 1"));
    }

    let values = parse_usize_sweep_values(chunk_sweep, chunk_bytes, "chunk")?;
    if values.iter().any(|&v| v == 0) {
        return Err(anyhow!("apex-map-lane: chunk sweep values must be >= 1"));
    }
    Ok(values)
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
        return Err(anyhow!("apex-map-lane: {} sweep produced no values", label));
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
        return Err(anyhow!("apex-map-lane: {} sweep produced no values", label));
    }

    Ok(out)
}

fn best_run_index(runs: &[ApexMapLaneRun]) -> usize {
    let mut best_idx = 0usize;
    for idx in 1..runs.len() {
        let best = &runs[best_idx].report;
        let cand = &runs[idx].report;

        let better = (
            cand.field_newline_extinct_flag,
            cand.field_pred_collapse_90_flag,
            cand.compact_field_total_payload_exact,
            cand.field_patch_bytes,
            cand.field_pred_newline_delta.abs(),
            std::cmp::Reverse((cand.field_f1_newline_pct * 1_000_000.0) as i64),
            std::cmp::Reverse((cand.field_balanced_accuracy_pct * 1_000_000.0) as i64),
            std::cmp::Reverse((cand.field_macro_f1_pct * 1_000_000.0) as i64),
        ) < (
            best.field_newline_extinct_flag,
            best.field_pred_collapse_90_flag,
            best.compact_field_total_payload_exact,
            best.field_patch_bytes,
            best.field_pred_newline_delta.abs(),
            std::cmp::Reverse((best.field_f1_newline_pct * 1_000_000.0) as i64),
            std::cmp::Reverse((best.field_balanced_accuracy_pct * 1_000_000.0) as i64),
            std::cmp::Reverse((best.field_macro_f1_pct * 1_000_000.0) as i64),
        );

        if better {
            best_idx = idx;
        }
    }
    best_idx
}

fn demote_surviving_newlines_capped(
    map: &ApexMap,
    predicted: &[u8],
    margin: u64,
    keep_ppm_min: u32,
    keep_min: usize,
) -> Result<NewlineDemotionStats> {
    let before_count = predicted.iter().filter(|&&v| v == 2).count();
    if margin == 0 || before_count == 0 {
        return Ok(NewlineDemotionStats {
            predicted: predicted.to_vec(),
            before_count,
            after_count: before_count,
            floor_used: before_count.min(keep_min),
            demoted: 0,
            extinct_flag: false,
        });
    }

    let floor_from_ppm = (((before_count as u128) * (keep_ppm_min as u128)) + 999_999u128)
        / 1_000_000u128;
    let floor_used = keep_min
        .max(floor_from_ppm as usize)
        .min(before_count);

    let mut out = predicted.to_vec();
    let mut candidates = Vec::<(u64, usize, u8)>::new();

    for pos in 0..out.len() {
        if out[pos] != 2 {
            continue;
        }
        let scores = map.score_at(pos)?;
        let newline_score = scores[2];
        let (best_class, best_score) = if scores[0] >= scores[1] {
            (0u8, scores[0])
        } else {
            (1u8, scores[1])
        };
        if best_score >= newline_score.saturating_add(margin) {
            let gap = best_score.saturating_sub(newline_score);
            candidates.push((gap, pos, best_class));
        }
    }

    candidates.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));

    let demote_limit = before_count.saturating_sub(floor_used).min(candidates.len());
    for &(_, pos, best_class) in candidates.iter().take(demote_limit) {
        out[pos] = best_class;
    }

    let after_count = before_count.saturating_sub(demote_limit);
    Ok(NewlineDemotionStats {
        predicted: out,
        before_count,
        after_count,
        floor_used,
        demoted: demote_limit,
        extinct_flag: before_count > 0 && after_count == 0,
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
        return Err(anyhow!("apex-map-lane: refine base length mismatch"));
    }
    if target.len() != map.len() {
        return Err(anyhow!("apex-map-lane: refine target length mismatch"));
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
            maybe_push_diag_row(
                &mut diag_rows,
                diag_limit,
                chunk_bytes,
                boundary,
                "left",
                target[pos],
                pair,
                trace,
            );
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
            maybe_push_diag_row(
                &mut diag_rows,
                diag_limit,
                chunk_bytes,
                boundary,
                "right",
                target[pos],
                pair,
                trace,
            );
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
    pair: k8dnz_apextrace::BoundaryPair,
    trace: OverrideTrace,
) {
    if trace.current != 2 && trace.desired != 2 && target != 2 {
        return;
    }
    if diag_limit != 0 && rows.len() >= diag_limit {
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
        pair_left: pair.left,
        pair_right: pair.right,
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

fn run_baseline_k8l1(
    input: &[u8],
    recipe_bytes: &[u8],
    max_ticks: u64,
) -> Result<(Vec<u8>, lane::LaneEncodeStats, u64)> {
    let mut baseline_ticks_used = max_ticks.max(1);
    let baseline_ticks_cap = baseline_ticks_used
        .saturating_mul(8)
        .max(160_000_000)
        .min(1_280_000_000);

    let out = loop {
        match lane::encode_k8l1(input, recipe_bytes, baseline_ticks_used) {
            Ok(ok) => break ok,
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("insufficient emissions") && baseline_ticks_used < baseline_ticks_cap
                {
                    let next = baseline_ticks_used
                        .saturating_mul(2)
                        .min(baseline_ticks_cap);
                    if next == baseline_ticks_used {
                        return Err(anyhow!("baseline k8l1 encode failed: {e}"));
                    }
                    eprintln!(
                        "apex-map-lane baseline retry: max_ticks={} failed with insufficient emissions; retrying with max_ticks={}",
                        baseline_ticks_used, next
                    );
                    baseline_ticks_used = next;
                    continue;
                }
                return Err(anyhow!("baseline k8l1 encode failed: {e}"));
            }
        }
    };

    if baseline_ticks_used != max_ticks {
        eprintln!(
            "apex-map-lane baseline auto-ticks resolved: used max_ticks={}",
            baseline_ticks_used
        );
    }

    Ok((out.0, out.1, baseline_ticks_used))
}

fn brute_force_best_ws_lane(target: &[u8], cfg: SearchCfg) -> Result<WsLaneBest> {
    if cfg.seed_step == 0 {
        return Err(anyhow!("apex-map-lane: seed_step must be >= 1"));
    }

    let byte_len = target.len() as u64;
    let mut best: Option<WsLaneBest> = None;

    for quadrant in 0u8..=3 {
        let mut i = 0u64;
        while i < cfg.seed_count {
            let seed = cfg
                .seed_from
                .saturating_add(i.saturating_mul(cfg.seed_step));
            let key = ApexKey::new_dibit_v1(byte_len, quadrant, seed, cfg.recipe_seed)?;
            let predicted = generate_bytes(&key)?
                .into_iter()
                .map(|b| bucket_u8_local(b, 3))
                .collect::<Vec<_>>();
            let diag = score_ws_lane_symbols(target, &predicted)?;
            let cand = WsLaneBest {
                key,
                predicted,
                diag,
            };
            match &best {
                None => best = Some(cand),
                Some(cur) if cand.diag.score.better_than(&cur.diag.score) => best = Some(cand),
                Some(_) => {}
            }
            i = i.saturating_add(1);
        }
    }

    best.ok_or_else(|| anyhow!("apex-map-lane: search produced no candidates"))
}

fn brute_force_best_ws_lane_chunked(
    target: &[u8],
    cfg: SearchCfg,
    chunk_bytes: usize,
    objective: ChunkSearchObjective,
    raw_slack: u64,
) -> Result<(WsLaneChunkedBest, ChunkSearchSummary)> {
    if chunk_bytes == 0 {
        return Err(anyhow!("apex-map-lane: chunk_bytes must be >= 1"));
    }

    let mut predicted = Vec::with_capacity(target.len());
    let mut chunks = Vec::new();
    let mut start = 0usize;
    let mut chunk_index = 0usize;
    let mut sum_balanced_accuracy_pct = 0.0f64;
    let mut sum_macro_f1_pct = 0.0f64;
    let mut sum_newline_f1_pct = 0.0f64;
    let mut sum_abs_newline_delta = 0.0f64;
    let mut sum_abs_minority_delta = 0.0f64;
    let mut majority_flip_count = 0usize;
    let mut collapse_90_count = 0usize;

    while start < target.len() {
        let end = start.saturating_add(chunk_bytes).min(target.len());
        let slice = &target[start..end];
        let (best, metrics) = brute_force_best_ws_lane_objective(slice, cfg, objective, raw_slack)?;
        let patch = PatchList::from_pred_actual(&best.predicted, slice)
            .map_err(|e| anyhow!("apex-map-lane chunk patch build failed: {e}"))?;
        let patch_bytes = patch.encode();

        sum_balanced_accuracy_pct += metrics.balanced_accuracy_pct;
        sum_macro_f1_pct += metrics.macro_f1_pct;
        let space_delta_abs = ((metrics.pred_hist[1] as i64) - (metrics.target_hist[1] as i64)).abs() as f64;
        let newline_delta_abs = ((metrics.pred_hist[2] as i64) - (metrics.target_hist[2] as i64)).abs() as f64;

        sum_newline_f1_pct += metrics.per_class[2].f1_pct;
        sum_abs_newline_delta += newline_delta_abs;
        sum_abs_minority_delta += space_delta_abs + newline_delta_abs;
        if metrics.pred_dominant_class != metrics.majority_class {
            majority_flip_count = majority_flip_count.saturating_add(1);
        }
        if metrics.pred_collapse_90_flag {
            collapse_90_count = collapse_90_count.saturating_add(1);
        }

        predicted.extend_from_slice(&best.predicted);
        chunks.push(WsLaneChunkBest {
            chunk_index,
            start,
            end,
            key: best.key,
            diag: best.diag,
            patch_entries: patch.entries.len(),
            patch_bytes: patch_bytes.len(),
        });

        start = end;
        chunk_index = chunk_index.saturating_add(1);
    }

    let diag = score_ws_lane_symbols(target, &predicted)?;
    let denom = chunks.len().max(1) as f64;
    Ok((
        WsLaneChunkedBest {
            chunk_bytes,
            chunk_key_bytes_exact: chunks.len().saturating_mul(APEX_KEY_BYTES_EXACT),
            predicted,
            diag,
            chunks,
        },
        ChunkSearchSummary {
            mean_balanced_accuracy_pct: sum_balanced_accuracy_pct / denom,
            mean_macro_f1_pct: sum_macro_f1_pct / denom,
            mean_newline_f1_pct: sum_newline_f1_pct / denom,
            mean_abs_newline_delta: sum_abs_newline_delta / denom,
            mean_abs_minority_delta: sum_abs_minority_delta / denom,
            majority_flip_count,
            collapse_90_count,
        },
    ))
}

fn brute_force_best_ws_lane_objective(
    target: &[u8],
    cfg: SearchCfg,
    objective: ChunkSearchObjective,
    raw_slack: u64,
) -> Result<(WsLaneBest, super::class_metrics::LaneClassMetrics)> {
    if cfg.seed_step == 0 {
        return Err(anyhow!("apex-map-lane: seed_step must be >= 1"));
    }

    if objective == ChunkSearchObjective::Raw {
        let best = brute_force_best_ws_lane(target, cfg)?;
        let metrics = compute_lane_class_metrics(target, &best.predicted)?;
        return Ok((best, metrics));
    }

    let raw_anchor = brute_force_best_ws_lane(target, cfg)?;
    let raw_anchor_matches = raw_anchor.diag.score.matches;
    let byte_len = target.len() as u64;
    let mut best: Option<(WsLaneBest, super::class_metrics::LaneClassMetrics)> = None;

    for quadrant in 0u8..=3 {
        let mut i = 0u64;
        while i < cfg.seed_count {
            let seed = cfg
                .seed_from
                .saturating_add(i.saturating_mul(cfg.seed_step));
            let key = ApexKey::new_dibit_v1(byte_len, quadrant, seed, cfg.recipe_seed)?;
            let predicted = generate_bytes(&key)?
                .into_iter()
                .map(|b| bucket_u8_local(b, 3))
                .collect::<Vec<_>>();
            let diag = score_ws_lane_symbols(target, &predicted)?;

            if objective == ChunkSearchObjective::RawGuarded
                && raw_anchor_matches.saturating_sub(diag.score.matches) > raw_slack
            {
                i = i.saturating_add(1);
                continue;
            }

            let metrics = compute_lane_class_metrics(target, &predicted)?;
            let cand = WsLaneBest {
                key,
                predicted,
                diag,
            };
            match &best {
                None => best = Some((cand, metrics)),
                Some((cur, cur_metrics))
                    if chunk_candidate_better(&cand, &metrics, cur, cur_metrics, objective) =>
                {
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
        let metrics = compute_lane_class_metrics(target, &raw_anchor.predicted)?;
        Ok((raw_anchor, metrics))
    }
}

fn chunk_candidate_better(
    cand: &WsLaneBest,
    cand_metrics: &super::class_metrics::LaneClassMetrics,
    cur: &WsLaneBest,
    cur_metrics: &super::class_metrics::LaneClassMetrics,
    objective: ChunkSearchObjective,
) -> bool {
    let cand_newline_delta_abs = ((cand_metrics.pred_hist[2] as i64) - (cand_metrics.target_hist[2] as i64)).abs();
    let cur_newline_delta_abs = ((cur_metrics.pred_hist[2] as i64) - (cur_metrics.target_hist[2] as i64)).abs();
    let cand_space_delta_abs = ((cand_metrics.pred_hist[1] as i64) - (cand_metrics.target_hist[1] as i64)).abs();
    let cur_space_delta_abs = ((cur_metrics.pred_hist[1] as i64) - (cur_metrics.target_hist[1] as i64)).abs();
    let cand_minority_delta_abs = cand_space_delta_abs + cand_newline_delta_abs;
    let cur_minority_delta_abs = cur_space_delta_abs + cur_newline_delta_abs;
    let cand_majority_flip = cand_metrics.pred_dominant_class != cand_metrics.majority_class;
    let cur_majority_flip = cur_metrics.pred_dominant_class != cur_metrics.majority_class;

    let cand_raw_guarded = (
        cand_metrics.pred_collapse_90_flag,
        cand_majority_flip,
        cand_minority_delta_abs,
        cand_newline_delta_abs,
        std::cmp::Reverse((cand_metrics.per_class[2].f1_pct * 1_000_000.0) as i64),
        std::cmp::Reverse((cand_metrics.balanced_accuracy_pct * 1_000_000.0) as i64),
        std::cmp::Reverse((cand_metrics.macro_f1_pct * 1_000_000.0) as i64),
        cand_metrics.hist_l1,
        std::cmp::Reverse(cand.diag.score.matches),
        std::cmp::Reverse(cand.diag.score.longest_run),
        std::cmp::Reverse(cand.diag.score.prefix),
    );
    let cur_raw_guarded = (
        cur_metrics.pred_collapse_90_flag,
        cur_majority_flip,
        cur_minority_delta_abs,
        cur_newline_delta_abs,
        std::cmp::Reverse((cur_metrics.per_class[2].f1_pct * 1_000_000.0) as i64),
        std::cmp::Reverse((cur_metrics.balanced_accuracy_pct * 1_000_000.0) as i64),
        std::cmp::Reverse((cur_metrics.macro_f1_pct * 1_000_000.0) as i64),
        cur_metrics.hist_l1,
        std::cmp::Reverse(cur.diag.score.matches),
        std::cmp::Reverse(cur.diag.score.longest_run),
        std::cmp::Reverse(cur.diag.score.prefix),
    );

    let cand_honest = (
        cand_metrics.pred_collapse_90_flag,
        cand_majority_flip,
        cand_minority_delta_abs,
        cand_newline_delta_abs,
        std::cmp::Reverse((cand_metrics.per_class[2].f1_pct * 1_000_000.0) as i64),
        std::cmp::Reverse((cand_metrics.balanced_accuracy_pct * 1_000_000.0) as i64),
        std::cmp::Reverse((cand_metrics.macro_f1_pct * 1_000_000.0) as i64),
        cand_metrics.hist_l1,
        std::cmp::Reverse(cand.diag.score.matches),
        std::cmp::Reverse(cand.diag.score.longest_run),
        std::cmp::Reverse(cand.diag.score.prefix),
    );
    let cur_honest = (
        cur_metrics.pred_collapse_90_flag,
        cur_majority_flip,
        cur_minority_delta_abs,
        cur_newline_delta_abs,
        std::cmp::Reverse((cur_metrics.per_class[2].f1_pct * 1_000_000.0) as i64),
        std::cmp::Reverse((cur_metrics.balanced_accuracy_pct * 1_000_000.0) as i64),
        std::cmp::Reverse((cur_metrics.macro_f1_pct * 1_000_000.0) as i64),
        cur_metrics.hist_l1,
        std::cmp::Reverse(cur.diag.score.matches),
        std::cmp::Reverse(cur.diag.score.longest_run),
        std::cmp::Reverse(cur.diag.score.prefix),
    );

    let cand_newline = (
        cand_metrics.pred_collapse_90_flag,
        cand_majority_flip,
        cand_newline_delta_abs,
        cand_space_delta_abs,
        std::cmp::Reverse((cand_metrics.per_class[2].f1_pct * 1_000_000.0) as i64),
        cand_metrics.hist_l1,
        std::cmp::Reverse((cand_metrics.balanced_accuracy_pct * 1_000_000.0) as i64),
        std::cmp::Reverse(cand.diag.score.matches),
        std::cmp::Reverse(cand.diag.score.longest_run),
        std::cmp::Reverse(cand.diag.score.prefix),
    );
    let cur_newline = (
        cur_metrics.pred_collapse_90_flag,
        cur_majority_flip,
        cur_newline_delta_abs,
        cur_space_delta_abs,
        std::cmp::Reverse((cur_metrics.per_class[2].f1_pct * 1_000_000.0) as i64),
        cur_metrics.hist_l1,
        std::cmp::Reverse((cur_metrics.balanced_accuracy_pct * 1_000_000.0) as i64),
        std::cmp::Reverse(cur.diag.score.matches),
        std::cmp::Reverse(cur.diag.score.longest_run),
        std::cmp::Reverse(cur.diag.score.prefix),
    );

    match objective {
        ChunkSearchObjective::Raw => cand.diag.score.better_than(&cur.diag.score),
        ChunkSearchObjective::RawGuarded => cand_raw_guarded < cur_raw_guarded,
        ChunkSearchObjective::Honest => cand_honest < cur_honest,
        ChunkSearchObjective::Newline => cand_newline < cur_newline,
    }
}

fn chunk_search_objective_name(objective: ChunkSearchObjective) -> &'static str {
    match objective {
        ChunkSearchObjective::Raw => "raw",
        ChunkSearchObjective::RawGuarded => "raw-guarded",
        ChunkSearchObjective::Honest => "honest",
        ChunkSearchObjective::Newline => "newline",
    }
}

fn score_ws_lane_symbols(target: &[u8], predicted: &[u8]) -> Result<WsLaneDiagnostics> {
    if target.len() != predicted.len() {
        return Err(anyhow!(
            "apex-map-lane: target len {} != predicted len {}",
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
    let mut target_hist = [0u64; 3];
    let mut pred_hist = [0u64; 3];

    for (idx, (&t, &p)) in target.iter().zip(predicted.iter()).enumerate() {
        target_hist[ws_slot(t)?] += 1;
        pred_hist[ws_slot(p)?] += 1;

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

    Ok(WsLaneDiagnostics {
        score: WsLaneScore {
            matches,
            prefix,
            total: target.len() as u64,
            longest_run,
            longest_run_start,
        },
        target_hist,
        pred_hist,
    })
}

fn save_outputs(
    out_key: Option<&str>,
    out_pred: Option<&str>,
    global: &WsLaneBest,
    chunked: &WsLaneChunkedBest,
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
        eprintln!("saved apex-map-lane global key: {}", path);

        let manifest = render_ws_lane_chunk_keys_csv(chunked.chunk_bytes, &chunked.chunks);
        let chunk_path = format!("{}.chunks.csv", path);
        std::fs::write(&chunk_path, manifest.as_bytes())
            .with_context(|| format!("write {}", chunk_path))?;
        eprintln!("saved apex-map-lane chunk manifest: {}", chunk_path);

        let compact_csv = render_compact_manifest_csv(compact_manifest);
        let compact_csv_path = format!("{}.compact.csv", path);
        std::fs::write(&compact_csv_path, compact_csv.as_bytes())
            .with_context(|| format!("write {}", compact_csv_path))?;
        eprintln!("saved apex-map-lane compact manifest csv: {}", compact_csv_path);

        let compact_bin_path = format!("{}.compact.bin", path);
        std::fs::write(&compact_bin_path, compact_manifest_bytes)
            .with_context(|| format!("write {}", compact_bin_path))?;
        eprintln!("saved apex-map-lane compact manifest bin: {}", compact_bin_path);

        let mut boundary_csv =
            String::from("boundary,left,right,left_pos,right_pos,node_count,max_depth_seen\n");
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
        eprintln!("saved apex-map-lane boundary pairs: {}", boundary_path);
    }

    if let Some(path) = out_pred {
        let global_ascii = render_ws_class_ascii(&global.predicted);
        let global_path = format!("{}.global.txt", path);
        std::fs::write(&global_path, global_ascii.as_bytes())
            .with_context(|| format!("write {}", global_path))?;
        eprintln!("saved apex-map-lane global class lane: {}", global_path);

        let chunked_ascii = render_ws_class_ascii(&chunked.predicted);
        let chunked_path = format!("{}.chunked.txt", path);
        std::fs::write(&chunked_path, chunked_ascii.as_bytes())
            .with_context(|| format!("write {}", chunked_path))?;
        eprintln!("saved apex-map-lane chunked class lane: {}", chunked_path);

        let field_ascii = render_ws_class_ascii(field_predicted);
        let field_path = format!("{}.field.txt", path);
        std::fs::write(&field_path, field_ascii.as_bytes())
            .with_context(|| format!("write {}", field_path))?;
        eprintln!("saved apex-map-lane field-refined class lane: {}", field_path);
    }

    Ok(())
}

fn render_reports_txt(runs: &[ApexMapLaneRun]) -> String {
    let mut out = String::new();
    for (idx, run) in runs.iter().enumerate() {
        if idx != 0 {
            out.push_str("\n---\n");
        }
        out.push_str(&render_report_txt(&run.report));
    }
    out
}

fn render_report_txt(row: &ApexMapLaneReport) -> String {
    let mut out = String::new();
    macro_rules! line {
        ($k:expr, $v:expr) => {{
            out.push_str($k);
            out.push('=');
            out.push_str(&$v.to_string());
            out.push('\n');
        }};
    }

    line!("input", row.input.clone());
    line!("recipe", row.recipe.clone());
    line!("normalized_len", row.normalized_len);
    line!("class_len", row.class_len);
    line!("other_len", row.other_len);

    line!("baseline_artifact_bytes", row.baseline_artifact_bytes);
    line!("baseline_max_ticks_used", row.baseline_max_ticks_used);
    line!("baseline_class_mismatches", row.baseline_class_mismatches);
    line!("baseline_class_patch_entries", row.baseline_class_patch_entries);
    line!("baseline_class_patch_bytes", row.baseline_class_patch_bytes);

    line!("majority_class", row.majority_class);
    line!("majority_class_label", row.majority_class_label);
    line!("majority_count", row.majority_count);
    line!(
        "majority_baseline_match_pct",
        format!("{:.6}", row.majority_baseline_match_pct)
    );
    line!("target_entropy_bits", format!("{:.6}", row.target_entropy_bits));

    line!("global_patch_entries", row.global_patch_entries);
    line!("global_patch_bytes", row.global_patch_bytes);
    line!("global_total_payload_exact", row.global_total_payload_exact);
    line!("global_match_pct", format!("{:.6}", row.global_match_pct));
    line!(
        "global_match_vs_majority_pct",
        format!("{:.6}", row.global_match_vs_majority_pct)
    );
    line!(
        "global_balanced_accuracy_pct",
        format!("{:.6}", row.global_balanced_accuracy_pct)
    );
    line!("global_macro_f1_pct", format!("{:.6}", row.global_macro_f1_pct));
    line!(
        "global_pred_entropy_bits",
        format!("{:.6}", row.global_pred_entropy_bits)
    );
    line!("global_hist_l1_pct", format!("{:.6}", row.global_hist_l1_pct));

    line!("chunk_bytes", row.chunk_bytes);
    line!("chunk_count", row.chunk_count);
    line!("chunk_patch_entries", row.chunk_patch_entries);
    line!("chunk_patch_bytes", row.chunk_patch_bytes);
    line!("chunk_total_payload_exact", row.chunk_total_payload_exact);
    line!("chunk_match_pct", format!("{:.6}", row.chunk_match_pct));
    line!(
        "chunk_match_vs_majority_pct",
        format!("{:.6}", row.chunk_match_vs_majority_pct)
    );
    line!(
        "chunk_balanced_accuracy_pct",
        format!("{:.6}", row.chunk_balanced_accuracy_pct)
    );
    line!("chunk_macro_f1_pct", format!("{:.6}", row.chunk_macro_f1_pct));
    line!(
        "chunk_pred_entropy_bits",
        format!("{:.6}", row.chunk_pred_entropy_bits)
    );
    line!("chunk_hist_l1_pct", format!("{:.6}", row.chunk_hist_l1_pct));
    line!("chunk_search_objective", row.chunk_search_objective.clone());
    line!("chunk_raw_slack", row.chunk_raw_slack);
    line!(
        "chunk_mean_balanced_accuracy_pct",
        format!("{:.6}", row.chunk_mean_balanced_accuracy_pct)
    );
    line!(
        "chunk_mean_macro_f1_pct",
        format!("{:.6}", row.chunk_mean_macro_f1_pct)
    );
    line!(
        "chunk_mean_newline_f1_pct",
        format!("{:.6}", row.chunk_mean_newline_f1_pct)
    );
    line!(
        "chunk_mean_abs_newline_delta",
        format!("{:.6}", row.chunk_mean_abs_newline_delta)
    );
    line!(
        "chunk_mean_abs_minority_delta",
        format!("{:.6}", row.chunk_mean_abs_minority_delta)
    );
    line!("chunk_majority_flip_count", row.chunk_majority_flip_count);
    line!("chunk_collapse_90_count", row.chunk_collapse_90_count);

    line!("compact_manifest_bytes_exact", row.compact_manifest_bytes_exact);
    line!(
        "compact_chunk_total_payload_exact",
        row.compact_chunk_total_payload_exact
    );

    line!("field_source", row.field_source.clone());
    line!("map_node_count", row.map_node_count);
    line!("map_depth_seen", row.map_depth_seen);
    line!("map_max_depth_arg", row.map_max_depth_arg);
    line!("map_depth_shift", row.map_depth_shift);
    line!("boundary_band", row.boundary_band);
    line!("boundary_delta", row.boundary_delta);
    line!("field_margin", row.field_margin);
    line!("newline_margin_add", row.newline_margin_add);
    line!("space_to_newline_margin_add", row.space_to_newline_margin_add);
    line!("newline_share_ppm_min", row.newline_share_ppm_min);
    line!("newline_override_budget", row.newline_override_budget);
    line!("newline_demote_margin", row.newline_demote_margin);
    line!("newline_demote_keep_ppm_min", row.newline_demote_keep_ppm_min);
    line!("newline_demote_keep_min", row.newline_demote_keep_min);
    line!(
        "newline_only_from_spacelike",
        row.newline_only_from_spacelike
    );

    line!("field_patch_entries", row.field_patch_entries);
    line!("field_patch_bytes", row.field_patch_bytes);
    line!("field_total_payload_exact", row.field_total_payload_exact);
    line!(
        "compact_field_total_payload_exact",
        row.compact_field_total_payload_exact
    );
    line!("field_match_pct", format!("{:.6}", row.field_match_pct));
    line!(
        "field_match_vs_majority_pct",
        format!("{:.6}", row.field_match_vs_majority_pct)
    );
    line!(
        "field_balanced_accuracy_pct",
        format!("{:.6}", row.field_balanced_accuracy_pct)
    );
    line!(
        "field_macro_precision_pct",
        format!("{:.6}", row.field_macro_precision_pct)
    );
    line!(
        "field_macro_recall_pct",
        format!("{:.6}", row.field_macro_recall_pct)
    );
    line!("field_macro_f1_pct", format!("{:.6}", row.field_macro_f1_pct));
    line!(
        "field_weighted_f1_pct",
        format!("{:.6}", row.field_weighted_f1_pct)
    );
    line!(
        "field_pred_entropy_bits",
        format!("{:.6}", row.field_pred_entropy_bits)
    );
    line!("field_hist_l1_pct", format!("{:.6}", row.field_hist_l1_pct));
    line!("field_pred_dominant_class", row.field_pred_dominant_class);
    line!("field_pred_dominant_label", row.field_pred_dominant_label);
    line!(
        "field_pred_dominant_share_ppm",
        row.field_pred_dominant_share_ppm
    );
    line!(
        "field_pred_dominant_share_pct",
        format!("{:.6}", row.field_pred_dominant_share_pct)
    );
    line!("field_pred_collapse_90_flag", row.field_pred_collapse_90_flag);

    line!(
        "field_precision_other_pct",
        format!("{:.6}", row.field_precision_other_pct)
    );
    line!(
        "field_recall_other_pct",
        format!("{:.6}", row.field_recall_other_pct)
    );
    line!("field_f1_other_pct", format!("{:.6}", row.field_f1_other_pct));
    line!(
        "field_precision_space_pct",
        format!("{:.6}", row.field_precision_space_pct)
    );
    line!(
        "field_recall_space_pct",
        format!("{:.6}", row.field_recall_space_pct)
    );
    line!("field_f1_space_pct", format!("{:.6}", row.field_f1_space_pct));
    line!(
        "field_precision_newline_pct",
        format!("{:.6}", row.field_precision_newline_pct)
    );
    line!(
        "field_recall_newline_pct",
        format!("{:.6}", row.field_recall_newline_pct)
    );
    line!(
        "field_f1_newline_pct",
        format!("{:.6}", row.field_f1_newline_pct)
    );

    line!("field_conf_t0_p0", row.field_conf_t0_p0);
    line!("field_conf_t0_p1", row.field_conf_t0_p1);
    line!("field_conf_t0_p2", row.field_conf_t0_p2);
    line!("field_conf_t1_p0", row.field_conf_t1_p0);
    line!("field_conf_t1_p1", row.field_conf_t1_p1);
    line!("field_conf_t1_p2", row.field_conf_t1_p2);
    line!("field_conf_t2_p0", row.field_conf_t2_p0);
    line!("field_conf_t2_p1", row.field_conf_t2_p1);
    line!("field_conf_t2_p2", row.field_conf_t2_p2);

    line!("field_overrides", row.field_overrides);
    line!("field_boundary_count", row.field_boundary_count);
    line!("field_touched_positions", row.field_touched_positions);
    line!("field_newline_applied", row.field_newline_applied);
    line!("field_newline_budget_blocked", row.field_newline_budget_blocked);
    line!("field_newline_demoted", row.field_newline_demoted);
    line!("field_newline_before_demote", row.field_newline_before_demote);
    line!("field_newline_after_demote", row.field_newline_after_demote);
    line!("field_newline_floor_used", row.field_newline_floor_used);
    line!("field_newline_extinct_flag", row.field_newline_extinct_flag);
    line!("newline_diag_rows", row.newline_diag_rows);

    line!(
        "delta_field_patch_vs_baseline",
        row.delta_field_patch_vs_baseline
    );
    line!("delta_field_patch_vs_global", row.delta_field_patch_vs_global);
    line!("delta_field_patch_vs_chunked", row.delta_field_patch_vs_chunked);
    line!("delta_field_total_vs_global", row.delta_field_total_vs_global);
    line!("delta_field_total_vs_chunked", row.delta_field_total_vs_chunked);
    line!(
        "delta_compact_chunk_total_vs_global",
        row.delta_compact_chunk_total_vs_global
    );
    line!(
        "delta_compact_field_total_vs_global",
        row.delta_compact_field_total_vs_global
    );
    line!(
        "delta_compact_field_total_vs_chunked",
        row.delta_compact_field_total_vs_chunked
    );
    line!(
        "delta_compact_field_total_vs_compact_chunked",
        row.delta_compact_field_total_vs_compact_chunked
    );

    line!("target_hist_other", row.target_hist[0]);
    line!("target_hist_space", row.target_hist[1]);
    line!("target_hist_newline", row.target_hist[2]);

    line!("global_pred_hist_other", row.global_pred_hist[0]);
    line!("global_pred_hist_space", row.global_pred_hist[1]);
    line!("global_pred_hist_newline", row.global_pred_hist[2]);

    line!("chunk_pred_hist_other", row.chunk_pred_hist[0]);
    line!("chunk_pred_hist_space", row.chunk_pred_hist[1]);
    line!("chunk_pred_hist_newline", row.chunk_pred_hist[2]);

    line!("field_pred_hist_other", row.field_pred_hist[0]);
    line!("field_pred_hist_space", row.field_pred_hist[1]);
    line!("field_pred_hist_newline", row.field_pred_hist[2]);

    line!("global_pred_newline_delta", row.global_pred_newline_delta);
    line!("chunk_pred_newline_delta", row.chunk_pred_newline_delta);
    line!("field_pred_newline_delta", row.field_pred_newline_delta);

    out
}

fn render_reports_csv(runs: &[ApexMapLaneRun]) -> String {
    let mut out = String::new();
    out.push_str(&csv_header());
    out.push('\n');
    for run in runs {
        out.push_str(&report_csv_row(&run.report));
        out.push('\n');
    }
    out
}

fn csv_header() -> String {
    [
        "input",
        "recipe",
        "normalized_len",
        "class_len",
        "other_len",
        "baseline_artifact_bytes",
        "baseline_max_ticks_used",
        "baseline_class_mismatches",
        "baseline_class_patch_entries",
        "baseline_class_patch_bytes",
        "majority_class",
        "majority_class_label",
        "majority_count",
        "majority_baseline_match_pct",
        "target_entropy_bits",
        "global_patch_entries",
        "global_patch_bytes",
        "global_total_payload_exact",
        "global_match_pct",
        "global_match_vs_majority_pct",
        "global_balanced_accuracy_pct",
        "global_macro_f1_pct",
        "global_pred_entropy_bits",
        "global_hist_l1_pct",
        "chunk_bytes",
        "chunk_count",
        "chunk_patch_entries",
        "chunk_patch_bytes",
        "chunk_total_payload_exact",
        "chunk_match_pct",
        "chunk_match_vs_majority_pct",
        "chunk_balanced_accuracy_pct",
        "chunk_macro_f1_pct",
        "chunk_pred_entropy_bits",
        "chunk_hist_l1_pct",
        "chunk_search_objective",
        "chunk_raw_slack",
        "chunk_mean_balanced_accuracy_pct",
        "chunk_mean_macro_f1_pct",
        "chunk_mean_newline_f1_pct",
        "chunk_mean_abs_newline_delta",
        "chunk_mean_abs_minority_delta",
        "chunk_majority_flip_count",
        "chunk_collapse_90_count",
        "compact_manifest_bytes_exact",
        "compact_chunk_total_payload_exact",
        "field_source",
        "map_node_count",
        "map_depth_seen",
        "map_max_depth_arg",
        "map_depth_shift",
        "boundary_band",
        "boundary_delta",
        "field_margin",
        "newline_margin_add",
        "space_to_newline_margin_add",
        "newline_share_ppm_min",
        "newline_override_budget",
        "newline_demote_margin",
        "newline_demote_keep_ppm_min",
        "newline_demote_keep_min",
        "newline_only_from_spacelike",
        "field_patch_entries",
        "field_patch_bytes",
        "field_total_payload_exact",
        "compact_field_total_payload_exact",
        "field_match_pct",
        "field_match_vs_majority_pct",
        "field_balanced_accuracy_pct",
        "field_macro_precision_pct",
        "field_macro_recall_pct",
        "field_macro_f1_pct",
        "field_weighted_f1_pct",
        "field_pred_entropy_bits",
        "field_hist_l1_pct",
        "field_pred_dominant_class",
        "field_pred_dominant_label",
        "field_pred_dominant_share_ppm",
        "field_pred_dominant_share_pct",
        "field_pred_collapse_90_flag",
        "field_precision_other_pct",
        "field_recall_other_pct",
        "field_f1_other_pct",
        "field_precision_space_pct",
        "field_recall_space_pct",
        "field_f1_space_pct",
        "field_precision_newline_pct",
        "field_recall_newline_pct",
        "field_f1_newline_pct",
        "field_conf_t0_p0",
        "field_conf_t0_p1",
        "field_conf_t0_p2",
        "field_conf_t1_p0",
        "field_conf_t1_p1",
        "field_conf_t1_p2",
        "field_conf_t2_p0",
        "field_conf_t2_p1",
        "field_conf_t2_p2",
        "field_overrides",
        "field_boundary_count",
        "field_touched_positions",
        "field_newline_applied",
        "field_newline_budget_blocked",
        "field_newline_demoted",
        "field_newline_before_demote",
        "field_newline_after_demote",
        "field_newline_floor_used",
        "field_newline_extinct_flag",
        "newline_diag_rows",
        "delta_field_patch_vs_baseline",
        "delta_field_patch_vs_global",
        "delta_field_patch_vs_chunked",
        "delta_field_total_vs_global",
        "delta_field_total_vs_chunked",
        "delta_compact_chunk_total_vs_global",
        "delta_compact_field_total_vs_global",
        "delta_compact_field_total_vs_chunked",
        "delta_compact_field_total_vs_compact_chunked",
        "target_hist_other",
        "target_hist_space",
        "target_hist_newline",
        "global_pred_hist_other",
        "global_pred_hist_space",
        "global_pred_hist_newline",
        "chunk_pred_hist_other",
        "chunk_pred_hist_space",
        "chunk_pred_hist_newline",
        "field_pred_hist_other",
        "field_pred_hist_space",
        "field_pred_hist_newline",
        "global_pred_newline_delta",
        "chunk_pred_newline_delta",
        "field_pred_newline_delta",
    ]
    .join(",")
}

fn report_csv_row(row: &ApexMapLaneReport) -> String {
    [
        row.input.clone(),
        row.recipe.clone(),
        row.normalized_len.to_string(),
        row.class_len.to_string(),
        row.other_len.to_string(),
        row.baseline_artifact_bytes.to_string(),
        row.baseline_max_ticks_used.to_string(),
        row.baseline_class_mismatches.to_string(),
        row.baseline_class_patch_entries.to_string(),
        row.baseline_class_patch_bytes.to_string(),
        row.majority_class.to_string(),
        row.majority_class_label.to_string(),
        row.majority_count.to_string(),
        format!("{:.6}", row.majority_baseline_match_pct),
        format!("{:.6}", row.target_entropy_bits),
        row.global_patch_entries.to_string(),
        row.global_patch_bytes.to_string(),
        row.global_total_payload_exact.to_string(),
        format!("{:.6}", row.global_match_pct),
        format!("{:.6}", row.global_match_vs_majority_pct),
        format!("{:.6}", row.global_balanced_accuracy_pct),
        format!("{:.6}", row.global_macro_f1_pct),
        format!("{:.6}", row.global_pred_entropy_bits),
        format!("{:.6}", row.global_hist_l1_pct),
        row.chunk_bytes.to_string(),
        row.chunk_count.to_string(),
        row.chunk_patch_entries.to_string(),
        row.chunk_patch_bytes.to_string(),
        row.chunk_total_payload_exact.to_string(),
        format!("{:.6}", row.chunk_match_pct),
        format!("{:.6}", row.chunk_match_vs_majority_pct),
        format!("{:.6}", row.chunk_balanced_accuracy_pct),
        format!("{:.6}", row.chunk_macro_f1_pct),
        format!("{:.6}", row.chunk_pred_entropy_bits),
        format!("{:.6}", row.chunk_hist_l1_pct),
        row.chunk_search_objective.clone(),
        row.chunk_raw_slack.to_string(),
        format!("{:.6}", row.chunk_mean_balanced_accuracy_pct),
        format!("{:.6}", row.chunk_mean_macro_f1_pct),
        format!("{:.6}", row.chunk_mean_newline_f1_pct),
        format!("{:.6}", row.chunk_mean_abs_newline_delta),
        format!("{:.6}", row.chunk_mean_abs_minority_delta),
        row.chunk_majority_flip_count.to_string(),
        row.chunk_collapse_90_count.to_string(),
        row.compact_manifest_bytes_exact.to_string(),
        row.compact_chunk_total_payload_exact.to_string(),
        row.field_source.clone(),
        row.map_node_count.to_string(),
        row.map_depth_seen.to_string(),
        row.map_max_depth_arg.to_string(),
        row.map_depth_shift.to_string(),
        row.boundary_band.to_string(),
        row.boundary_delta.to_string(),
        row.field_margin.to_string(),
        row.newline_margin_add.to_string(),
        row.space_to_newline_margin_add.to_string(),
        row.newline_share_ppm_min.to_string(),
        row.newline_override_budget.to_string(),
        row.newline_demote_margin.to_string(),
        row.newline_demote_keep_ppm_min.to_string(),
        row.newline_demote_keep_min.to_string(),
        row.newline_only_from_spacelike.to_string(),
        row.field_patch_entries.to_string(),
        row.field_patch_bytes.to_string(),
        row.field_total_payload_exact.to_string(),
        row.compact_field_total_payload_exact.to_string(),
        format!("{:.6}", row.field_match_pct),
        format!("{:.6}", row.field_match_vs_majority_pct),
        format!("{:.6}", row.field_balanced_accuracy_pct),
        format!("{:.6}", row.field_macro_precision_pct),
        format!("{:.6}", row.field_macro_recall_pct),
        format!("{:.6}", row.field_macro_f1_pct),
        format!("{:.6}", row.field_weighted_f1_pct),
        format!("{:.6}", row.field_pred_entropy_bits),
        format!("{:.6}", row.field_hist_l1_pct),
        row.field_pred_dominant_class.to_string(),
        row.field_pred_dominant_label.to_string(),
        row.field_pred_dominant_share_ppm.to_string(),
        format!("{:.6}", row.field_pred_dominant_share_pct),
        row.field_pred_collapse_90_flag.to_string(),
        format!("{:.6}", row.field_precision_other_pct),
        format!("{:.6}", row.field_recall_other_pct),
        format!("{:.6}", row.field_f1_other_pct),
        format!("{:.6}", row.field_precision_space_pct),
        format!("{:.6}", row.field_recall_space_pct),
        format!("{:.6}", row.field_f1_space_pct),
        format!("{:.6}", row.field_precision_newline_pct),
        format!("{:.6}", row.field_recall_newline_pct),
        format!("{:.6}", row.field_f1_newline_pct),
        row.field_conf_t0_p0.to_string(),
        row.field_conf_t0_p1.to_string(),
        row.field_conf_t0_p2.to_string(),
        row.field_conf_t1_p0.to_string(),
        row.field_conf_t1_p1.to_string(),
        row.field_conf_t1_p2.to_string(),
        row.field_conf_t2_p0.to_string(),
        row.field_conf_t2_p1.to_string(),
        row.field_conf_t2_p2.to_string(),
        row.field_overrides.to_string(),
        row.field_boundary_count.to_string(),
        row.field_touched_positions.to_string(),
        row.field_newline_applied.to_string(),
        row.field_newline_budget_blocked.to_string(),
        row.field_newline_demoted.to_string(),
        row.field_newline_before_demote.to_string(),
        row.field_newline_after_demote.to_string(),
        row.field_newline_floor_used.to_string(),
        row.field_newline_extinct_flag.to_string(),
        row.newline_diag_rows.to_string(),
        row.delta_field_patch_vs_baseline.to_string(),
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
        row.global_pred_hist[0].to_string(),
        row.global_pred_hist[1].to_string(),
        row.global_pred_hist[2].to_string(),
        row.chunk_pred_hist[0].to_string(),
        row.chunk_pred_hist[1].to_string(),
        row.chunk_pred_hist[2].to_string(),
        row.field_pred_hist[0].to_string(),
        row.field_pred_hist[1].to_string(),
        row.field_pred_hist[2].to_string(),
        row.global_pred_newline_delta.to_string(),
        row.chunk_pred_newline_delta.to_string(),
        row.field_pred_newline_delta.to_string(),
    ]
    .join(",")
}

fn render_diag_csv(runs: &[ApexMapLaneRun]) -> String {
    let mut out = String::from(
        "chunk_bytes,boundary,side,pos,target,target_label,current,current_label,desired,desired_label,pair_left,pair_left_label,pair_right,pair_right_label,allow_mask,desired_score,current_score,needed_margin,share_ppm,share_floor,decision,applied\n",
    );

    for run in runs {
        for row in &run.diag_rows {
            out.push_str(&format!(
                "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n",
                row.chunk_bytes,
                row.boundary,
                row.side,
                row.pos,
                row.target,
                class_label(row.target),
                row.current,
                class_label(row.current),
                row.desired,
                class_label(row.desired),
                row.pair_left,
                class_label(row.pair_left),
                row.pair_right,
                class_label(row.pair_right),
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

fn print_summary(
    out: Option<&str>,
    format: RenderFormat,
    runs: &[ApexMapLaneRun],
    best_idx: usize,
) {
    if let Some(path) = out {
        eprintln!(
            "apextrace apex-map-lane ok: out={} format={:?} rows={} best_chunk_bytes={} best_boundary_band={} best_field_margin={} best_newline_demote_margin={} best_compact_field_total_payload_exact={}",
            path,
            format,
            runs.len(),
            runs[best_idx].report.chunk_bytes,
            runs[best_idx].report.boundary_band,
            runs[best_idx].report.field_margin,
            runs[best_idx].report.newline_demote_margin,
            runs[best_idx].report.compact_field_total_payload_exact,
        );
        return;
    }

    for run in runs {
        let row = &run.report;
        eprintln!(
            "apextrace apex-map-lane row: chunk_bytes={} boundary_band={} field_margin={} newline_demote_margin={} chunk_search_objective={} chunk_raw_slack={} chunk_mean_abs_newline_delta={:.6} chunk_mean_abs_minority_delta={:.6} chunk_majority_flip_count={} chunk_mean_newline_f1_pct={:.6} compact_field_total_payload_exact={} field_patch_bytes={} field_match_pct={:.6} majority_baseline_match_pct={:.6} field_match_vs_majority_pct={:.6} field_balanced_accuracy_pct={:.6} field_macro_f1_pct={:.6} field_f1_newline_pct={:.6} field_pred_dominant_label={} field_pred_dominant_share_pct={:.6} field_pred_collapse_90_flag={} field_pred_newline_delta={} field_overrides={} field_newline_applied={} field_newline_budget_blocked={} field_newline_demoted={} field_newline_after_demote={} field_newline_floor_used={} field_newline_extinct_flag={} newline_diag_rows={}",
            row.chunk_bytes,
            row.boundary_band,
            row.field_margin,
            row.newline_demote_margin,
            row.chunk_search_objective,
            row.chunk_raw_slack,
            row.chunk_mean_abs_newline_delta,
            row.chunk_mean_abs_minority_delta,
            row.chunk_majority_flip_count,
            row.chunk_mean_newline_f1_pct,
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
            row.field_overrides,
            row.field_newline_applied,
            row.field_newline_budget_blocked,
            row.field_newline_demoted,
            row.field_newline_after_demote,
            row.field_newline_floor_used,
            row.field_newline_extinct_flag,
            row.newline_diag_rows,
        );
    }

    let best = &runs[best_idx].report;
    eprintln!(
        "apextrace apex-map-lane best: chunk_bytes={} boundary_band={} field_margin={} newline_demote_margin={} chunk_search_objective={} chunk_raw_slack={} chunk_mean_abs_newline_delta={:.6} chunk_mean_abs_minority_delta={:.6} chunk_majority_flip_count={} chunk_mean_newline_f1_pct={:.6} compact_field_total_payload_exact={} field_patch_bytes={} field_match_pct={:.6} majority_baseline_match_pct={:.6} field_match_vs_majority_pct={:.6} field_balanced_accuracy_pct={:.6} field_macro_f1_pct={:.6} field_f1_newline_pct={:.6} field_pred_dominant_label={} field_pred_dominant_share_pct={:.6} field_pred_collapse_90_flag={} field_pred_newline_delta={} field_newline_demoted={} field_newline_after_demote={} field_newline_floor_used={} field_newline_extinct_flag={}",
        best.chunk_bytes,
        best.boundary_band,
        best.field_margin,
        best.newline_demote_margin,
        best.chunk_search_objective,
        best.chunk_raw_slack,
        best.chunk_mean_abs_newline_delta,
        best.chunk_mean_abs_minority_delta,
        best.chunk_majority_flip_count,
        best.chunk_mean_newline_f1_pct,
        best.compact_field_total_payload_exact,
        best.field_patch_bytes,
        best.field_match_pct,
        best.majority_baseline_match_pct,
        best.field_match_vs_majority_pct,
        best.field_balanced_accuracy_pct,
        best.field_macro_f1_pct,
        best.field_f1_newline_pct,
        best.field_pred_dominant_label,
        best.field_pred_dominant_share_pct,
        best.field_pred_collapse_90_flag,
        best.field_pred_newline_delta,
        best.field_newline_demoted,
        best.field_newline_after_demote,
        best.field_newline_floor_used,
        best.field_newline_extinct_flag,
    );
}

fn ws_slot(v: u8) -> Result<usize> {
    match v {
        0..=2 => Ok(v as usize),
        _ => Err(anyhow!("apex-map-lane: invalid class symbol {}", v)),
    }
}

#[inline]
fn bucket_u8_local(b: u8, k: u8) -> u8 {
    ((b as u16 * k as u16) >> 8) as u8
}

#[cfg(test)]
mod tests {
    use super::{best_run_index, demote_surviving_newlines_capped, ApexMapLaneReport, ApexMapLaneRun, CompactChunkManifest, WsLaneBest, WsLaneChunkedBest, WsLaneDiagnostics, WsLaneScore};
    use k8dnz_apextrace::{ApexKey, ApexMap, ApexMapCfg};

    fn dummy_report() -> ApexMapLaneReport {
        ApexMapLaneReport {
            input: String::new(),
            recipe: String::new(),
            normalized_len: 0,
            class_len: 0,
            other_len: 0,
            baseline_artifact_bytes: 0,
            baseline_max_ticks_used: 0,
            baseline_class_mismatches: 0,
            baseline_class_patch_entries: 0,
            baseline_class_patch_bytes: 0,
            majority_class: 0,
            majority_class_label: "other",
            majority_count: 0,
            majority_baseline_match_pct: 0.0,
            target_entropy_bits: 0.0,
            global_patch_entries: 0,
            global_patch_bytes: 0,
            global_total_payload_exact: 0,
            global_match_pct: 0.0,
            global_match_vs_majority_pct: 0.0,
            global_balanced_accuracy_pct: 0.0,
            global_macro_f1_pct: 0.0,
            global_pred_entropy_bits: 0.0,
            global_hist_l1_pct: 0.0,
            chunk_bytes: 64,
            chunk_count: 0,
            chunk_patch_entries: 0,
            chunk_patch_bytes: 0,
            chunk_total_payload_exact: 0,
            chunk_match_pct: 0.0,
            chunk_match_vs_majority_pct: 0.0,
            chunk_balanced_accuracy_pct: 0.0,
            chunk_macro_f1_pct: 0.0,
            chunk_pred_entropy_bits: 0.0,
            chunk_hist_l1_pct: 0.0,
            chunk_search_objective: "raw".to_string(),
            chunk_raw_slack: 1,
            chunk_mean_balanced_accuracy_pct: 0.0,
            chunk_mean_macro_f1_pct: 0.0,
            chunk_mean_newline_f1_pct: 0.0,
            chunk_mean_abs_newline_delta: 0.0,
            chunk_mean_abs_minority_delta: 0.0,
            chunk_majority_flip_count: 0,
            chunk_collapse_90_count: 0,
            compact_manifest_bytes_exact: 0,
            compact_chunk_total_payload_exact: 0,
            field_source: "chunked".to_string(),
            map_node_count: 0,
            map_depth_seen: 0,
            map_depth_shift: 0,
            map_max_depth_arg: 0,
            boundary_band: 20,
            boundary_delta: 1,
            field_margin: 8,
            newline_margin_add: 96,
            space_to_newline_margin_add: 64,
            newline_share_ppm_min: 550_000,
            newline_override_budget: 0,
            newline_demote_margin: 0,
            newline_demote_keep_ppm_min: 150_000,
            newline_demote_keep_min: 1,
            newline_only_from_spacelike: true,
            field_patch_entries: 0,
            field_patch_bytes: 0,
            field_total_payload_exact: 0,
            compact_field_total_payload_exact: 0,
            field_match_pct: 0.0,
            field_match_vs_majority_pct: 0.0,
            field_balanced_accuracy_pct: 0.0,
            field_macro_precision_pct: 0.0,
            field_macro_recall_pct: 0.0,
            field_macro_f1_pct: 0.0,
            field_weighted_f1_pct: 0.0,
            field_pred_entropy_bits: 0.0,
            field_hist_l1_pct: 0.0,
            field_pred_dominant_class: 0,
            field_pred_dominant_label: "other",
            field_pred_dominant_share_ppm: 0,
            field_pred_dominant_share_pct: 0.0,
            field_pred_collapse_90_flag: false,
            field_precision_other_pct: 0.0,
            field_recall_other_pct: 0.0,
            field_f1_other_pct: 0.0,
            field_precision_space_pct: 0.0,
            field_recall_space_pct: 0.0,
            field_f1_space_pct: 0.0,
            field_precision_newline_pct: 0.0,
            field_recall_newline_pct: 0.0,
            field_f1_newline_pct: 0.0,
            field_conf_t0_p0: 0,
            field_conf_t0_p1: 0,
            field_conf_t0_p2: 0,
            field_conf_t1_p0: 0,
            field_conf_t1_p1: 0,
            field_conf_t1_p2: 0,
            field_conf_t2_p0: 0,
            field_conf_t2_p1: 0,
            field_conf_t2_p2: 0,
            field_overrides: 0,
            field_boundary_count: 0,
            field_touched_positions: 0,
            field_newline_applied: 0,
            field_newline_budget_blocked: 0,
            field_newline_demoted: 0,
            field_newline_before_demote: 0,
            field_newline_after_demote: 0,
            field_newline_floor_used: 0,
            field_newline_extinct_flag: false,
            newline_diag_rows: 0,
            delta_field_patch_vs_baseline: 0,
            delta_field_patch_vs_global: 0,
            delta_field_patch_vs_chunked: 0,
            delta_field_total_vs_global: 0,
            delta_field_total_vs_chunked: 0,
            delta_compact_chunk_total_vs_global: 0,
            delta_compact_field_total_vs_global: 0,
            delta_compact_field_total_vs_chunked: 0,
            delta_compact_field_total_vs_compact_chunked: 0,
            target_hist: [0; 3],
            global_pred_hist: [0; 3],
            chunk_pred_hist: [0; 3],
            field_pred_hist: [0; 3],
            global_pred_newline_delta: 0,
            chunk_pred_newline_delta: 0,
            field_pred_newline_delta: 0,
        }
    }

    #[test]
    fn capped_newline_demotion_keeps_floor() {
        let field_source = vec![0u8, 0, 0, 0, 2, 2, 2, 2];
        let map = ApexMap::from_symbols(
            &field_source,
            ApexMapCfg {
                class_count: 3,
                max_depth: 0,
                depth_shift: 1,
            },
        )
        .unwrap();
        let predicted = vec![2u8, 2, 2, 2, 2, 2, 2, 2];

        let stats = demote_surviving_newlines_capped(&map, &predicted, 1, 250_000, 1).unwrap();
        assert_eq!(stats.before_count, 8);
        assert_eq!(stats.floor_used, 2);
        assert_eq!(stats.after_count, 2);
        assert_eq!(stats.demoted, 6);
        assert!(!stats.extinct_flag);
        assert_eq!(stats.predicted.iter().filter(|&&v| v == 2).count(), 2);
    }

    #[test]
    fn selector_rejects_newline_extinction_fake_win() {
        let mut honest = dummy_report();
        honest.compact_field_total_payload_exact = 2498;
        honest.field_patch_bytes = 1886;
        honest.field_pred_newline_delta = 362;
        honest.field_f1_newline_pct = 7.053942;
        honest.field_balanced_accuracy_pct = 43.019964;
        honest.field_macro_f1_pct = 37.768858;
        honest.field_newline_after_demote = 422;

        let mut extinct = dummy_report();
        extinct.compact_field_total_payload_exact = 2177;
        extinct.field_patch_bytes = 1565;
        extinct.field_pred_newline_delta = -60;
        extinct.field_f1_newline_pct = 0.0;
        extinct.field_balanced_accuracy_pct = 36.944738;
        extinct.field_macro_f1_pct = 37.002520;
        extinct.field_newline_after_demote = 0;
        extinct.field_newline_extinct_flag = true;

        let runs = vec![
            ApexMapLaneRun {
                report: extinct,
                global: WsLaneBest {
                    key: ApexKey::new_dibit_v1(0, 0, 0, 1).unwrap(),
                    predicted: Vec::new(),
                    diag: WsLaneDiagnostics { score: WsLaneScore::default(), target_hist: [0;3], pred_hist:[0;3] },
                },
                chunked: WsLaneChunkedBest {
                    chunk_bytes: 64,
                    chunk_key_bytes_exact: 0,
                    predicted: Vec::new(),
                    diag: WsLaneDiagnostics { score: WsLaneScore::default(), target_hist: [0;3], pred_hist:[0;3] },
                    chunks: Vec::new(),
                },
                field_predicted: Vec::new(),
                boundaries: Vec::new(),
                map: ApexMap::from_symbols(&[0u8], ApexMapCfg { class_count: 1, max_depth: 0, depth_shift: 1 }).unwrap(),
                compact_manifest: CompactChunkManifest { total_len: 0, chunk_bytes: 64, recipe_seed: 1, keys: Vec::new() },
                compact_manifest_bytes: Vec::new(),
                diag_rows: Vec::new(),
            },
            ApexMapLaneRun {
                report: honest,
                global: WsLaneBest {
                    key: ApexKey::new_dibit_v1(0, 0, 0, 1).unwrap(),
                    predicted: Vec::new(),
                    diag: WsLaneDiagnostics { score: WsLaneScore::default(), target_hist: [0;3], pred_hist:[0;3] },
                },
                chunked: WsLaneChunkedBest {
                    chunk_bytes: 64,
                    chunk_key_bytes_exact: 0,
                    predicted: Vec::new(),
                    diag: WsLaneDiagnostics { score: WsLaneScore::default(), target_hist: [0;3], pred_hist:[0;3] },
                    chunks: Vec::new(),
                },
                field_predicted: Vec::new(),
                boundaries: Vec::new(),
                map: ApexMap::from_symbols(&[0u8], ApexMapCfg { class_count: 1, max_depth: 0, depth_shift: 1 }).unwrap(),
                compact_manifest: CompactChunkManifest { total_len: 0, chunk_bytes: 64, recipe_seed: 1, keys: Vec::new() },
                compact_manifest_bytes: Vec::new(),
                diag_rows: Vec::new(),
            },
        ];

        assert_eq!(best_run_index(&runs), 1);
    }
}
