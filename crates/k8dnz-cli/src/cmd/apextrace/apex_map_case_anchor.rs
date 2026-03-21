use anyhow::{anyhow, Context, Result};
use k8dnz_apextrace::{generate_bytes, ApexKey, ApexMap, ApexMapCfg, RefineCfg, SearchCfg};
use k8dnz_core::repr::{
    case_lanes::{case_label, CaseLanes},
    text_norm,
};
use k8dnz_core::symbol::patch::PatchList;
use std::collections::HashSet;

use crate::cmd::apextrace::{ApexMapCaseAnchorArgs, ChunkSearchObjective, RenderFormat};

use super::baselines::{anchored_consensus_prediction, baseline_symbol_lane};
use super::common::write_or_print;
use super::symbol_metrics::{compute_symbol_metrics, SymbolMetrics};

const APEX_KEY_BYTES_EXACT: usize = 48;
const CLASS_COUNT: u8 = 2;
const RAW_GUARDRAIL_PCT: f64 = 95.0;

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
    start: usize,
}

#[derive(Clone, Debug)]
struct LaneChunkedBest {
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
struct CaseAnchorReport {
    input: String,
    normalized_len: usize,
    letter_positions: usize,
    case_len: usize,
    majority_class: u8,
    majority_label: &'static str,
    majority_count: u64,
    majority_baseline_match_pct: f64,
    target_entropy_bits: f64,

    baseline_patch_entries: usize,
    baseline_patch_bytes: usize,
    baseline_total_payload_exact: usize,
    baseline_match_pct: f64,
    baseline_balanced_accuracy_pct: f64,
    baseline_macro_f1_pct: f64,
    baseline_f1_upper_pct: f64,

    global_patch_entries: usize,
    global_patch_bytes: usize,
    global_total_payload_exact: usize,
    global_match_pct: f64,
    global_balanced_accuracy_pct: f64,
    global_macro_f1_pct: f64,
    global_f1_upper_pct: f64,

    chunk_bytes: usize,
    chunk_count: usize,
    chunk_patch_entries: usize,
    chunk_patch_bytes: usize,
    chunk_total_payload_exact: usize,
    chunk_match_pct: f64,
    chunk_balanced_accuracy_pct: f64,
    chunk_macro_f1_pct: f64,
    chunk_f1_upper_pct: f64,
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
    field_match_pct: f64,
    field_balanced_accuracy_pct: f64,
    field_macro_f1_pct: f64,
    field_f1_upper_pct: f64,

    hybrid_upper_consensus_min: usize,
    hybrid_promoted_upper_count: usize,
    hybrid_patch_entries: usize,
    hybrid_patch_bytes: usize,
    hybrid_total_payload_exact: usize,
    hybrid_match_pct: f64,
    hybrid_match_vs_baseline_pct: f64,
    hybrid_balanced_accuracy_pct: f64,
    hybrid_macro_f1_pct: f64,
    hybrid_f1_upper_pct: f64,

    target_hist: [u64; 4],
    baseline_pred_hist: [u64; 4],
    field_pred_hist: [u64; 4],
    hybrid_pred_hist: [u64; 4],
}

#[derive(Clone, Debug)]
struct CaseAnchorRun {
    report: CaseAnchorReport,
    field_predicted: Vec<u8>,
    hybrid_predicted: Vec<u8>,
}

#[derive(Clone, Debug)]
struct StabilityCandidate {
    label: String,
    vote_threshold: usize,
    unique_prediction_count: usize,
    promoted_upper_count: usize,
    source_cost_exact: usize,
    patch_entries: usize,
    patch_bytes: usize,
    total_payload_exact: usize,
    raw_match_pct: f64,
    balanced_accuracy_pct: f64,
    macro_f1_pct: f64,
    upper_f1_pct: f64,
    pred_hist: [u64; 4],
    predicted: Vec<u8>,
}

#[derive(Clone, Debug)]
struct RecommendedStrategy {
    label: String,
    total_payload_exact: usize,
    raw_match_pct: f64,
    upper_f1_pct: f64,
    predicted: Vec<u8>,
}

pub fn run_apex_map_case_anchor(args: ApexMapCaseAnchorArgs) -> Result<()> {
    let chunk_values = parse_usize_sweep_values(args.chunk_sweep.as_deref(), args.chunk_bytes, "chunk")?;
    let boundary_band_values =
        parse_usize_sweep_values(args.boundary_band_sweep.as_deref(), args.boundary_band, "boundary_band")?;
    let field_margin_values =
        parse_u64_sweep_values(args.field_margin_sweep.as_deref(), args.field_margin, "field_margin")?;

    let input = std::fs::read(&args.r#in).with_context(|| format!("read {}", args.r#in))?;
    let norm = text_norm::normalize_newlines(&input);
    let cases = CaseLanes::split(&norm);
    if cases.case_lane.is_empty() {
        return Err(anyhow!("apex-map-case-anchor: input contains no ASCII letters"));
    }

    let target_metrics = compute_symbol_metrics(&cases.case_lane, &cases.case_lane, CLASS_COUNT)?;
    let baseline_predicted = baseline_symbol_lane(cases.case_lane.len(), CaseLanes::CASE_LOWER, CLASS_COUNT)?;
    let baseline_patch = PatchList::from_pred_actual(&baseline_predicted, &cases.case_lane)
        .map_err(|e| anyhow!("apex-map-case-anchor baseline patch build failed: {e}"))?;
    let baseline_patch_entries = baseline_patch.entries.len();
    let baseline_patch_bytes = baseline_patch.encode();
    let baseline_total_payload_exact = baseline_patch_bytes.len();
    let baseline_metrics = compute_symbol_metrics(&cases.case_lane, &baseline_predicted, CLASS_COUNT)?;

    let cfg = SearchCfg {
        seed_from: args.seed_from,
        seed_count: args.seed_count,
        seed_step: args.seed_step,
        recipe_seed: args.recipe_seed,
    };

    let global = brute_force_best_symbol_lane(&cases.case_lane, cfg, CLASS_COUNT)?;
    let global_patch = PatchList::from_pred_actual(&global.predicted, &cases.case_lane)
        .map_err(|e| anyhow!("apex-map-case-anchor global patch build failed: {e}"))?;
    let global_patch_bytes = global_patch.encode();
    let global_total_payload_exact = global_patch_bytes.len().saturating_add(APEX_KEY_BYTES_EXACT);
    let global_patch_entries = global_patch.entries.len();
    let global_metrics = compute_symbol_metrics(&cases.case_lane, &global.predicted, CLASS_COUNT)?;

    let mut runs = Vec::new();
    for chunk_bytes in chunk_values {
        for &boundary_band in &boundary_band_values {
            for &field_margin in &field_margin_values {
                runs.push(run_case_anchor_once(
                    &args,
                    &norm,
                    &cases,
                    &target_metrics,
                    baseline_patch_entries,
                    &baseline_patch_bytes,
                    baseline_total_payload_exact,
                    &baseline_metrics,
                    &global,
                    global_patch_entries,
                    &global_patch_bytes,
                    global_total_payload_exact,
                    &global_metrics,
                    cfg,
                    chunk_bytes,
                    boundary_band,
                    field_margin,
                )?);
            }
        }
    }

    if runs.is_empty() {
        return Err(anyhow!("apex-map-case-anchor: no runs executed"));
    }

    let best_idx = best_run_index(&runs);
    let best = &runs[best_idx].report;

    let best_field_chunk_bytes = select_best_field_chunk_bytes(&runs);
    let best_chunk_source_cost_exact = source_cost_exact_for_chunk(&runs, best_field_chunk_bytes)
        .ok_or_else(|| anyhow!("apex-map-case-anchor: could not derive source cost for best chunk"))?;

    let best_chunk_unique_fields = unique_field_predictions_for_chunk(&runs, best_field_chunk_bytes);
    let stability_candidates = build_stability_candidates(
        &cases.case_lane,
        &best_chunk_unique_fields,
        best_chunk_source_cost_exact,
        "field-best-chunk",
    )?;

    let recommended_codec = recommend_codec_strategy(
        &baseline_predicted,
        baseline_total_payload_exact,
        &baseline_metrics,
        &stability_candidates,
    );

    let recommended_north95 = recommend_north95_strategy(
        &baseline_predicted,
        baseline_total_payload_exact,
        &baseline_metrics,
        &stability_candidates,
    );

    if let Some(path) = args.out_pred.as_deref() {
        let body = render_prediction_ascii(&recommended_codec.predicted);
        write_or_print(Some(path), &body)?;
    }

    let body = match args.format {
        RenderFormat::Csv => render_reports_csv(
            &runs,
            best_field_chunk_bytes,
            &stability_candidates,
            &recommended_codec,
            &recommended_north95,
        ),
        RenderFormat::Txt => render_reports_txt(
            &runs,
            best_field_chunk_bytes,
            &stability_candidates,
            &recommended_codec,
            &recommended_north95,
        ),
    };
    write_or_print(args.out.as_deref(), &body)?;
    print_summary(
        args.out.as_deref(),
        args.format,
        &runs,
        best_idx,
        best_field_chunk_bytes,
        &stability_candidates,
        &recommended_codec,
        &recommended_north95,
    );

    eprintln!(
        "apex-map-case-anchor best-field: chunk={} boundary_band={} field_margin={} field_total_payload_exact={} field_match_pct={:.6}",
        best.chunk_bytes,
        best.boundary_band,
        best.field_margin,
        best.field_total_payload_exact,
        best.field_match_pct,
    );
    eprintln!(
        "apex-map-case-anchor codec-recommendation: label={} total_payload_exact={} raw_match_pct={:.6} upper_f1_pct={:.6}",
        recommended_codec.label,
        recommended_codec.total_payload_exact,
        recommended_codec.raw_match_pct,
        recommended_codec.upper_f1_pct,
    );
    eprintln!(
        "apex-map-case-anchor north95-recommendation: label={} total_payload_exact={} raw_match_pct={:.6} upper_f1_pct={:.6}",
        recommended_north95.label,
        recommended_north95.total_payload_exact,
        recommended_north95.raw_match_pct,
        recommended_north95.upper_f1_pct,
    );

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_case_anchor_once(
    args: &ApexMapCaseAnchorArgs,
    norm: &[u8],
    cases: &CaseLanes,
    target_metrics: &SymbolMetrics,
    baseline_patch_entries: usize,
    baseline_patch_bytes: &[u8],
    baseline_total_payload_exact: usize,
    baseline_metrics: &SymbolMetrics,
    global: &LaneBest,
    global_patch_entries: usize,
    global_patch_bytes: &[u8],
    global_total_payload_exact: usize,
    global_metrics: &SymbolMetrics,
    cfg: SearchCfg,
    chunk_bytes: usize,
    boundary_band: usize,
    field_margin: u64,
) -> Result<CaseAnchorRun> {
    let (chunked, chunk_summary) = brute_force_best_symbol_lane_chunked(
        &cases.case_lane,
        cfg,
        chunk_bytes,
        args.chunk_search_objective,
        args.chunk_raw_slack,
        CLASS_COUNT,
    )?;
    let chunk_patch = PatchList::from_pred_actual(&chunked.predicted, &cases.case_lane)
        .map_err(|e| anyhow!("apex-map-case-anchor chunked patch build failed: {e}"))?;
    let chunk_patch_bytes = chunk_patch.encode();
    let chunk_total_payload_exact = chunk_patch_bytes.len().saturating_add(chunked.chunk_key_bytes_exact);
    let chunk_metrics = compute_symbol_metrics(&cases.case_lane, &chunked.predicted, CLASS_COUNT)?;

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

    let (field_predicted, _) = map.refine_boundaries(&chunked.predicted, &boundaries, refine_cfg)?;
    let field_patch = PatchList::from_pred_actual(&field_predicted, &cases.case_lane)
        .map_err(|e| anyhow!("apex-map-case-anchor field patch build failed: {e}"))?;
    let field_patch_bytes = field_patch.encode();
    let field_total_payload_exact = field_patch_bytes.len().saturating_add(chunked.chunk_key_bytes_exact);
    let field_metrics = compute_symbol_metrics(&cases.case_lane, &field_predicted, CLASS_COUNT)?;

    let predictors: [&[u8]; 3] = [&global.predicted, &chunked.predicted, &field_predicted];
    let (hybrid_predicted, hybrid_promoted_upper_count) = anchored_consensus_prediction(
        cases.case_lane.len(),
        CaseLanes::CASE_LOWER,
        CaseLanes::CASE_UPPER,
        CLASS_COUNT,
        &predictors,
        args.hybrid_upper_consensus_min,
    )?;
    let hybrid_patch = PatchList::from_pred_actual(&hybrid_predicted, &cases.case_lane)
        .map_err(|e| anyhow!("apex-map-case-anchor hybrid patch build failed: {e}"))?;
    let hybrid_patch_entries = hybrid_patch.entries.len();
    let hybrid_patch_bytes = hybrid_patch.encode();
    let hybrid_total_payload_exact = hybrid_patch_bytes.len().saturating_add(chunked.chunk_key_bytes_exact);
    let hybrid_metrics = compute_symbol_metrics(&cases.case_lane, &hybrid_predicted, CLASS_COUNT)?;

    let report = CaseAnchorReport {
        input: args.r#in.clone(),
        normalized_len: norm.len(),
        letter_positions: cases.letter_len,
        case_len: cases.case_lane.len(),
        majority_class: target_metrics.majority_class,
        majority_label: case_label(target_metrics.majority_class),
        majority_count: target_metrics.target_hist[target_metrics.majority_class as usize],
        majority_baseline_match_pct: target_metrics.majority_baseline_match_pct,
        target_entropy_bits: target_metrics.target_entropy_bits,

        baseline_patch_entries,
        baseline_patch_bytes: baseline_patch_bytes.len(),
        baseline_total_payload_exact,
        baseline_match_pct: baseline_metrics.raw_match_pct,
        baseline_balanced_accuracy_pct: baseline_metrics.balanced_accuracy_pct,
        baseline_macro_f1_pct: baseline_metrics.macro_f1_pct,
        baseline_f1_upper_pct: baseline_metrics.per_class[CaseLanes::CASE_UPPER as usize].f1_pct,

        global_patch_entries,
        global_patch_bytes: global_patch_bytes.len(),
        global_total_payload_exact,
        global_match_pct: global_metrics.raw_match_pct,
        global_balanced_accuracy_pct: global_metrics.balanced_accuracy_pct,
        global_macro_f1_pct: global_metrics.macro_f1_pct,
        global_f1_upper_pct: global_metrics.per_class[CaseLanes::CASE_UPPER as usize].f1_pct,

        chunk_bytes,
        chunk_count: chunked.chunks.len(),
        chunk_patch_entries: chunk_patch.entries.len(),
        chunk_patch_bytes: chunk_patch_bytes.len(),
        chunk_total_payload_exact,
        chunk_match_pct: chunk_metrics.raw_match_pct,
        chunk_balanced_accuracy_pct: chunk_metrics.balanced_accuracy_pct,
        chunk_macro_f1_pct: chunk_metrics.macro_f1_pct,
        chunk_f1_upper_pct: chunk_metrics.per_class[CaseLanes::CASE_UPPER as usize].f1_pct,
        chunk_search_objective: chunk_search_objective_name(args.chunk_search_objective).to_string(),
        chunk_raw_slack: args.chunk_raw_slack,
        chunk_mean_balanced_accuracy_pct: chunk_summary.mean_balanced_accuracy_pct,
        chunk_mean_macro_f1_pct: chunk_summary.mean_macro_f1_pct,
        chunk_mean_minority_f1_pct: chunk_summary.mean_minority_f1_pct,
        chunk_mean_abs_minority_delta: chunk_summary.mean_abs_minority_delta,
        chunk_majority_flip_count: chunk_summary.majority_flip_count,
        chunk_collapse_90_count: chunk_summary.collapse_90_count,

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
        lower_margin_add: args.lower_margin_add,
        upper_margin_add: args.upper_margin_add,
        lower_share_ppm_min: args.lower_share_ppm_min,
        upper_share_ppm_min: args.upper_share_ppm_min,

        field_patch_entries: field_patch.entries.len(),
        field_patch_bytes: field_patch_bytes.len(),
        field_total_payload_exact,
        field_match_pct: field_metrics.raw_match_pct,
        field_balanced_accuracy_pct: field_metrics.balanced_accuracy_pct,
        field_macro_f1_pct: field_metrics.macro_f1_pct,
        field_f1_upper_pct: field_metrics.per_class[CaseLanes::CASE_UPPER as usize].f1_pct,

        hybrid_upper_consensus_min: args.hybrid_upper_consensus_min,
        hybrid_promoted_upper_count,
        hybrid_patch_entries,
        hybrid_patch_bytes: hybrid_patch_bytes.len(),
        hybrid_total_payload_exact,
        hybrid_match_pct: hybrid_metrics.raw_match_pct,
        hybrid_match_vs_baseline_pct: hybrid_metrics.raw_match_pct - baseline_metrics.raw_match_pct,
        hybrid_balanced_accuracy_pct: hybrid_metrics.balanced_accuracy_pct,
        hybrid_macro_f1_pct: hybrid_metrics.macro_f1_pct,
        hybrid_f1_upper_pct: hybrid_metrics.per_class[CaseLanes::CASE_UPPER as usize].f1_pct,

        target_hist: hist4(&target_metrics.target_hist),
        baseline_pred_hist: hist4(&baseline_metrics.pred_hist),
        field_pred_hist: hist4(&field_metrics.pred_hist),
        hybrid_pred_hist: hist4(&hybrid_metrics.pred_hist),
    };

    Ok(CaseAnchorRun {
        report,
        field_predicted,
        hybrid_predicted,
    })
}

fn hist4(src: &[u64]) -> [u64; 4] {
    let mut out = [0u64; 4];
    let take = src.len().min(4);
    out[..take].copy_from_slice(&src[..take]);
    out
}

fn best_run_index(runs: &[CaseAnchorRun]) -> usize {
    let mut best_idx = 0usize;
    for (idx, run) in runs.iter().enumerate().skip(1) {
        let best = &runs[best_idx].report;
        let cur = &run.report;
        let cur_key = (
            std::cmp::Reverse((cur.field_match_pct * 1_000_000.0) as i64),
            std::cmp::Reverse((cur.field_f1_upper_pct * 1_000_000.0) as i64),
            std::cmp::Reverse((cur.field_balanced_accuracy_pct * 1_000_000.0) as i64),
            cur.field_total_payload_exact,
            cur.boundary_band,
            cur.chunk_bytes,
        );
        let best_key = (
            std::cmp::Reverse((best.field_match_pct * 1_000_000.0) as i64),
            std::cmp::Reverse((best.field_f1_upper_pct * 1_000_000.0) as i64),
            std::cmp::Reverse((best.field_balanced_accuracy_pct * 1_000_000.0) as i64),
            best.field_total_payload_exact,
            best.boundary_band,
            best.chunk_bytes,
        );
        if cur_key < best_key {
            best_idx = idx;
        }
    }
    best_idx
}

fn select_best_field_chunk_bytes(runs: &[CaseAnchorRun]) -> usize {
    let mut best_chunk = runs[0].report.chunk_bytes;
    let mut best_key = (
        std::cmp::Reverse((runs[0].report.field_match_pct * 1_000_000.0) as i64),
        std::cmp::Reverse((runs[0].report.field_f1_upper_pct * 1_000_000.0) as i64),
        runs[0].report.field_total_payload_exact,
        runs[0].report.chunk_bytes,
    );

    for run in runs.iter().skip(1) {
        let key = (
            std::cmp::Reverse((run.report.field_match_pct * 1_000_000.0) as i64),
            std::cmp::Reverse((run.report.field_f1_upper_pct * 1_000_000.0) as i64),
            run.report.field_total_payload_exact,
            run.report.chunk_bytes,
        );
        if key < best_key {
            best_key = key;
            best_chunk = run.report.chunk_bytes;
        }
    }

    best_chunk
}

fn source_cost_exact_for_chunk(runs: &[CaseAnchorRun], chunk_bytes: usize) -> Option<usize> {
    runs.iter()
        .find(|run| run.report.chunk_bytes == chunk_bytes)
        .map(|run| run.report.chunk_total_payload_exact.saturating_sub(run.report.chunk_patch_bytes))
}

fn unique_field_predictions_for_chunk(runs: &[CaseAnchorRun], chunk_bytes: usize) -> Vec<Vec<u8>> {
    let mut seen = HashSet::<Vec<u8>>::new();
    let mut out = Vec::<Vec<u8>>::new();
    for run in runs.iter().filter(|run| run.report.chunk_bytes == chunk_bytes) {
        if seen.insert(run.field_predicted.clone()) {
            out.push(run.field_predicted.clone());
        }
    }
    out
}

fn build_stability_candidates(
    target: &[u8],
    unique_predictions: &[Vec<u8>],
    source_cost_exact: usize,
    label_prefix: &str,
) -> Result<Vec<StabilityCandidate>> {
    if unique_predictions.is_empty() {
        return Ok(Vec::new());
    }

    let predictors = unique_predictions.iter().map(|v| v.as_slice()).collect::<Vec<_>>();
    let mut out = Vec::new();

    for vote_threshold in (1..=predictors.len()).rev() {
        let (predicted, promoted_upper_count) = anchored_consensus_prediction(
            target.len(),
            CaseLanes::CASE_LOWER,
            CaseLanes::CASE_UPPER,
            CLASS_COUNT,
            &predictors,
            vote_threshold,
        )?;
        let patch = PatchList::from_pred_actual(&predicted, target)
            .map_err(|e| anyhow!("apex-map-case-anchor stability patch build failed: {e}"))?;
        let patch_bytes = patch.encode();
        let metrics = compute_symbol_metrics(target, &predicted, CLASS_COUNT)?;

        out.push(StabilityCandidate {
            label: format!("{}-votes{}", label_prefix, vote_threshold),
            vote_threshold,
            unique_prediction_count: predictors.len(),
            promoted_upper_count,
            source_cost_exact,
            patch_entries: patch.entries.len(),
            patch_bytes: patch_bytes.len(),
            total_payload_exact: source_cost_exact.saturating_add(patch_bytes.len()),
            raw_match_pct: metrics.raw_match_pct,
            balanced_accuracy_pct: metrics.balanced_accuracy_pct,
            macro_f1_pct: metrics.macro_f1_pct,
            upper_f1_pct: metrics.per_class[CaseLanes::CASE_UPPER as usize].f1_pct,
            pred_hist: hist4(&metrics.pred_hist),
            predicted,
        });
    }

    Ok(out)
}

fn recommend_codec_strategy(
    baseline_predicted: &[u8],
    baseline_total_payload_exact: usize,
    baseline_metrics: &SymbolMetrics,
    stability_candidates: &[StabilityCandidate],
) -> RecommendedStrategy {
    let mut best = RecommendedStrategy {
        label: "baseline-all-lower".to_string(),
        total_payload_exact: baseline_total_payload_exact,
        raw_match_pct: baseline_metrics.raw_match_pct,
        upper_f1_pct: baseline_metrics.per_class[CaseLanes::CASE_UPPER as usize].f1_pct,
        predicted: baseline_predicted.to_vec(),
    };

    for cand in stability_candidates.iter().filter(|cand| cand.raw_match_pct >= RAW_GUARDRAIL_PCT) {
        let cand_key = (
            cand.total_payload_exact,
            std::cmp::Reverse((cand.upper_f1_pct * 1_000_000.0) as i64),
            std::cmp::Reverse((cand.raw_match_pct * 1_000_000.0) as i64),
        );
        let best_key = (
            best.total_payload_exact,
            std::cmp::Reverse((best.upper_f1_pct * 1_000_000.0) as i64),
            std::cmp::Reverse((best.raw_match_pct * 1_000_000.0) as i64),
        );
        if cand_key < best_key {
            best = RecommendedStrategy {
                label: cand.label.clone(),
                total_payload_exact: cand.total_payload_exact,
                raw_match_pct: cand.raw_match_pct,
                upper_f1_pct: cand.upper_f1_pct,
                predicted: cand.predicted.clone(),
            };
        }
    }

    best
}

fn recommend_north95_strategy(
    baseline_predicted: &[u8],
    baseline_total_payload_exact: usize,
    baseline_metrics: &SymbolMetrics,
    stability_candidates: &[StabilityCandidate],
) -> RecommendedStrategy {
    let mut best = RecommendedStrategy {
        label: "baseline-all-lower".to_string(),
        total_payload_exact: baseline_total_payload_exact,
        raw_match_pct: baseline_metrics.raw_match_pct,
        upper_f1_pct: baseline_metrics.per_class[CaseLanes::CASE_UPPER as usize].f1_pct,
        predicted: baseline_predicted.to_vec(),
    };

    for cand in stability_candidates.iter().filter(|cand| cand.raw_match_pct >= RAW_GUARDRAIL_PCT) {
        let cand_key = (
            std::cmp::Reverse((cand.upper_f1_pct * 1_000_000.0) as i64),
            std::cmp::Reverse((cand.raw_match_pct * 1_000_000.0) as i64),
            cand.total_payload_exact,
        );
        let best_key = (
            std::cmp::Reverse((best.upper_f1_pct * 1_000_000.0) as i64),
            std::cmp::Reverse((best.raw_match_pct * 1_000_000.0) as i64),
            best.total_payload_exact,
        );
        if cand_key < best_key {
            best = RecommendedStrategy {
                label: cand.label.clone(),
                total_payload_exact: cand.total_payload_exact,
                raw_match_pct: cand.raw_match_pct,
                upper_f1_pct: cand.upper_f1_pct,
                predicted: cand.predicted.clone(),
            };
        }
    }

    best
}

fn parse_usize_sweep_values(spec: Option<&str>, fallback: usize, label: &str) -> Result<Vec<usize>> {
    if let Some(spec) = spec {
        let mut out = Vec::new();
        for part in spec.split(',') {
            let trimmed = part.trim();
            if trimmed.is_empty() {
                continue;
            }
            out.push(
                trimmed
                    .parse::<usize>()
                    .map_err(|e| anyhow!("apex-map-case-anchor invalid {} value '{}': {}", label, trimmed, e))?,
            );
        }
        if out.is_empty() {
            return Err(anyhow!("apex-map-case-anchor empty {} sweep", label));
        }
        out.sort_unstable();
        out.dedup();
        Ok(out)
    } else {
        Ok(vec![fallback])
    }
}

fn parse_u64_sweep_values(spec: Option<&str>, fallback: u64, label: &str) -> Result<Vec<u64>> {
    if let Some(spec) = spec {
        let mut out = Vec::new();
        for part in spec.split(',') {
            let trimmed = part.trim();
            if trimmed.is_empty() {
                continue;
            }
            out.push(
                trimmed
                    .parse::<u64>()
                    .map_err(|e| anyhow!("apex-map-case-anchor invalid {} value '{}': {}", label, trimmed, e))?,
            );
        }
        if out.is_empty() {
            return Err(anyhow!("apex-map-case-anchor empty {} sweep", label));
        }
        out.sort_unstable();
        out.dedup();
        Ok(out)
    } else {
        Ok(vec![fallback])
    }
}

fn brute_force_best_symbol_lane(target: &[u8], cfg: SearchCfg, class_count: u8) -> Result<LaneBest> {
    if cfg.seed_step == 0 {
        return Err(anyhow!("apex-map-case-anchor: seed_step must be >= 1"));
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
                Some(current) if cand.score.better_than(&current.score) => best = Some(cand),
                Some(_) => {}
            }
            i = i.saturating_add(1);
        }
    }

    best.ok_or_else(|| anyhow!("apex-map-case-anchor: no apex key candidates evaluated"))
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
        return Err(anyhow!("apex-map-case-anchor: chunk_bytes must be >= 1"));
    }

    let mut predicted = Vec::with_capacity(target.len());
    let mut chunks = Vec::new();
    let mut start = 0usize;

    let mut sum_balanced_accuracy_pct = 0.0;
    let mut sum_macro_f1_pct = 0.0;
    let mut sum_minority_f1_pct = 0.0;
    let mut sum_abs_minority_delta = 0.0;
    let mut majority_flip_count = 0usize;
    let mut collapse_90_count = 0usize;

    while start < target.len() {
        let end = (start + chunk_bytes).min(target.len());
        let slice = &target[start..end];
        let (best, metrics) = brute_force_best_symbol_lane_objective(slice, cfg, objective, raw_slack, class_count)?;

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
        chunks.push(LaneChunkBest { start });
        start = end;
    }

    let denom = chunks.len().max(1) as f64;

    Ok((
        LaneChunkedBest {
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
        return Err(anyhow!("apex-map-case-anchor: seed_step must be >= 1"));
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
                Some((cur, cur_metrics))
                    if chunk_candidate_better(&cand, &metrics, cur, cur_metrics, objective, class_count) =>
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
            "apex-map-case-anchor: target len {} != predicted len {}",
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

fn render_reports_txt(
    runs: &[CaseAnchorRun],
    best_field_chunk_bytes: usize,
    stability_candidates: &[StabilityCandidate],
    recommended_codec: &RecommendedStrategy,
    recommended_north95: &RecommendedStrategy,
) -> String {
    let mut out = String::new();
    for (idx, run) in runs.iter().enumerate() {
        let r = &run.report;
        if idx > 0 {
            out.push('\n');
        }
        out.push_str(&format!(
            "input={} normalized_len={} letter_positions={} case_len={}\n",
            r.input, r.normalized_len, r.letter_positions, r.case_len
        ));
        out.push_str(&format!(
            "majority_class={} majority_label={} majority_count={} majority_baseline_match_pct={} target_entropy_bits={}\n",
            r.majority_class,
            r.majority_label,
            r.majority_count,
            fmt6(r.majority_baseline_match_pct),
            fmt6(r.target_entropy_bits),
        ));
        out.push_str(&format!(
            "baseline_patch_entries={} baseline_patch_bytes={} baseline_total_payload_exact={} baseline_match_pct={} baseline_balanced_accuracy_pct={} baseline_macro_f1_pct={} baseline_f1_upper_pct={}\n",
            r.baseline_patch_entries,
            r.baseline_patch_bytes,
            r.baseline_total_payload_exact,
            fmt6(r.baseline_match_pct),
            fmt6(r.baseline_balanced_accuracy_pct),
            fmt6(r.baseline_macro_f1_pct),
            fmt6(r.baseline_f1_upper_pct),
        ));
        out.push_str(&format!(
            "global_patch_entries={} global_patch_bytes={} global_total_payload_exact={} global_match_pct={} global_balanced_accuracy_pct={} global_macro_f1_pct={} global_f1_upper_pct={}\n",
            r.global_patch_entries,
            r.global_patch_bytes,
            r.global_total_payload_exact,
            fmt6(r.global_match_pct),
            fmt6(r.global_balanced_accuracy_pct),
            fmt6(r.global_macro_f1_pct),
            fmt6(r.global_f1_upper_pct),
        ));
        out.push_str(&format!(
            "chunk_bytes={} chunk_count={} chunk_patch_entries={} chunk_patch_bytes={} chunk_total_payload_exact={} chunk_match_pct={} chunk_balanced_accuracy_pct={} chunk_macro_f1_pct={} chunk_f1_upper_pct={} chunk_search_objective={} chunk_raw_slack={} chunk_mean_balanced_accuracy_pct={} chunk_mean_macro_f1_pct={} chunk_mean_minority_f1_pct={} chunk_mean_abs_minority_delta={} chunk_majority_flip_count={} chunk_collapse_90_count={}\n",
            r.chunk_bytes,
            r.chunk_count,
            r.chunk_patch_entries,
            r.chunk_patch_bytes,
            r.chunk_total_payload_exact,
            fmt6(r.chunk_match_pct),
            fmt6(r.chunk_balanced_accuracy_pct),
            fmt6(r.chunk_macro_f1_pct),
            fmt6(r.chunk_f1_upper_pct),
            r.chunk_search_objective,
            r.chunk_raw_slack,
            fmt6(r.chunk_mean_balanced_accuracy_pct),
            fmt6(r.chunk_mean_macro_f1_pct),
            fmt6(r.chunk_mean_minority_f1_pct),
            fmt6(r.chunk_mean_abs_minority_delta),
            r.chunk_majority_flip_count,
            r.chunk_collapse_90_count,
        ));
        out.push_str(&format!(
            "field_source={} map_node_count={} map_depth_seen={} map_depth_shift={} map_max_depth_arg={} boundary_band={} boundary_delta={} field_margin={} lower_margin_add={} upper_margin_add={} lower_share_ppm_min={} upper_share_ppm_min={}\n",
            r.field_source,
            r.map_node_count,
            r.map_depth_seen,
            r.map_depth_shift,
            r.map_max_depth_arg,
            r.boundary_band,
            r.boundary_delta,
            r.field_margin,
            r.lower_margin_add,
            r.upper_margin_add,
            r.lower_share_ppm_min,
            r.upper_share_ppm_min,
        ));
        out.push_str(&format!(
            "field_patch_entries={} field_patch_bytes={} field_total_payload_exact={} field_match_pct={} field_balanced_accuracy_pct={} field_macro_f1_pct={} field_f1_upper_pct={}\n",
            r.field_patch_entries,
            r.field_patch_bytes,
            r.field_total_payload_exact,
            fmt6(r.field_match_pct),
            fmt6(r.field_balanced_accuracy_pct),
            fmt6(r.field_macro_f1_pct),
            fmt6(r.field_f1_upper_pct),
        ));
        out.push_str(&format!(
            "hybrid_upper_consensus_min={} hybrid_promoted_upper_count={} hybrid_patch_entries={} hybrid_patch_bytes={} hybrid_total_payload_exact={} hybrid_match_pct={} hybrid_match_vs_baseline_pct={} hybrid_balanced_accuracy_pct={} hybrid_macro_f1_pct={} hybrid_f1_upper_pct={}\n",
            r.hybrid_upper_consensus_min,
            r.hybrid_promoted_upper_count,
            r.hybrid_patch_entries,
            r.hybrid_patch_bytes,
            r.hybrid_total_payload_exact,
            fmt6(r.hybrid_match_pct),
            fmt6(r.hybrid_match_vs_baseline_pct),
            fmt6(r.hybrid_balanced_accuracy_pct),
            fmt6(r.hybrid_macro_f1_pct),
            fmt6(r.hybrid_f1_upper_pct),
        ));
        out.push_str(&format!(
            "target_hist=[{},{},{},{}] baseline_pred_hist=[{},{},{},{}] field_pred_hist=[{},{},{},{}] hybrid_pred_hist=[{},{},{},{}]\n",
            r.target_hist[0],
            r.target_hist[1],
            r.target_hist[2],
            r.target_hist[3],
            r.baseline_pred_hist[0],
            r.baseline_pred_hist[1],
            r.baseline_pred_hist[2],
            r.baseline_pred_hist[3],
            r.field_pred_hist[0],
            r.field_pred_hist[1],
            r.field_pred_hist[2],
            r.field_pred_hist[3],
            r.hybrid_pred_hist[0],
            r.hybrid_pred_hist[1],
            r.hybrid_pred_hist[2],
            r.hybrid_pred_hist[3],
        ));
    }

    out.push('\n');
    out.push_str(&format!(
        "stability_source=field-best-chunk best_field_chunk_bytes={} raw_guardrail_pct={}\n",
        best_field_chunk_bytes,
        fmt6(RAW_GUARDRAIL_PCT),
    ));
    if stability_candidates.is_empty() {
        out.push_str("stability_candidates=0\n");
    } else {
        for cand in stability_candidates {
            out.push_str(&format!(
                "stability_label={} vote_threshold={} unique_prediction_count={} promoted_upper_count={} source_cost_exact={} patch_entries={} patch_bytes={} total_payload_exact={} raw_match_pct={} balanced_accuracy_pct={} macro_f1_pct={} upper_f1_pct={} pred_hist=[{},{},{},{}]\n",
                cand.label,
                cand.vote_threshold,
                cand.unique_prediction_count,
                cand.promoted_upper_count,
                cand.source_cost_exact,
                cand.patch_entries,
                cand.patch_bytes,
                cand.total_payload_exact,
                fmt6(cand.raw_match_pct),
                fmt6(cand.balanced_accuracy_pct),
                fmt6(cand.macro_f1_pct),
                fmt6(cand.upper_f1_pct),
                cand.pred_hist[0],
                cand.pred_hist[1],
                cand.pred_hist[2],
                cand.pred_hist[3],
            ));
        }
    }

    out.push_str(&format!(
        "recommended_codec_strategy={} total_payload_exact={} raw_match_pct={} upper_f1_pct={}\n",
        recommended_codec.label,
        recommended_codec.total_payload_exact,
        fmt6(recommended_codec.raw_match_pct),
        fmt6(recommended_codec.upper_f1_pct),
    ));
    out.push_str(&format!(
        "recommended_north95_strategy={} total_payload_exact={} raw_match_pct={} upper_f1_pct={}\n",
        recommended_north95.label,
        recommended_north95.total_payload_exact,
        fmt6(recommended_north95.raw_match_pct),
        fmt6(recommended_north95.upper_f1_pct),
    ));

    out
}

fn render_reports_csv(
    runs: &[CaseAnchorRun],
    best_field_chunk_bytes: usize,
    stability_candidates: &[StabilityCandidate],
    recommended_codec: &RecommendedStrategy,
    recommended_north95: &RecommendedStrategy,
) -> String {
    let mut out = String::new();
    out.push_str("row_type,input,normalized_len,letter_positions,case_len,majority_class,majority_label,majority_count,majority_baseline_match_pct,target_entropy_bits,baseline_patch_entries,baseline_patch_bytes,baseline_total_payload_exact,baseline_match_pct,baseline_balanced_accuracy_pct,baseline_macro_f1_pct,baseline_f1_upper_pct,global_patch_entries,global_patch_bytes,global_total_payload_exact,global_match_pct,global_balanced_accuracy_pct,global_macro_f1_pct,global_f1_upper_pct,chunk_bytes,chunk_count,chunk_patch_entries,chunk_patch_bytes,chunk_total_payload_exact,chunk_match_pct,chunk_balanced_accuracy_pct,chunk_macro_f1_pct,chunk_f1_upper_pct,chunk_search_objective,chunk_raw_slack,chunk_mean_balanced_accuracy_pct,chunk_mean_macro_f1_pct,chunk_mean_minority_f1_pct,chunk_mean_abs_minority_delta,chunk_majority_flip_count,chunk_collapse_90_count,field_source,map_node_count,map_depth_seen,map_depth_shift,map_max_depth_arg,boundary_band,boundary_delta,field_margin,lower_margin_add,upper_margin_add,lower_share_ppm_min,upper_share_ppm_min,field_patch_entries,field_patch_bytes,field_total_payload_exact,field_match_pct,field_balanced_accuracy_pct,field_macro_f1_pct,field_f1_upper_pct,hybrid_upper_consensus_min,hybrid_promoted_upper_count,hybrid_patch_entries,hybrid_patch_bytes,hybrid_total_payload_exact,hybrid_match_pct,hybrid_match_vs_baseline_pct,hybrid_balanced_accuracy_pct,hybrid_macro_f1_pct,hybrid_f1_upper_pct,target_hist_0,target_hist_1,target_hist_2,target_hist_3,baseline_pred_hist_0,baseline_pred_hist_1,baseline_pred_hist_2,baseline_pred_hist_3,field_pred_hist_0,field_pred_hist_1,field_pred_hist_2,field_pred_hist_3,hybrid_pred_hist_0,hybrid_pred_hist_1,hybrid_pred_hist_2,hybrid_pred_hist_3,stability_label,vote_threshold,unique_prediction_count,promoted_upper_count,source_cost_exact,patch_entries,patch_bytes,total_payload_exact,raw_match_pct,balanced_accuracy_pct,macro_f1_pct,upper_f1_pct,pred_hist_0,pred_hist_1,pred_hist_2,pred_hist_3,best_field_chunk_bytes,recommended_codec_strategy,recommended_codec_total_payload_exact,recommended_codec_raw_match_pct,recommended_codec_upper_f1_pct,recommended_north95_strategy,recommended_north95_total_payload_exact,recommended_north95_raw_match_pct,recommended_north95_upper_f1_pct\n");

    for run in runs {
        let r = &run.report;
        let cells = vec![
            "run".to_string(),
            csv_escape(&r.input),
            r.normalized_len.to_string(),
            r.letter_positions.to_string(),
            r.case_len.to_string(),
            r.majority_class.to_string(),
            csv_escape(r.majority_label),
            r.majority_count.to_string(),
            fmt6(r.majority_baseline_match_pct),
            fmt6(r.target_entropy_bits),
            r.baseline_patch_entries.to_string(),
            r.baseline_patch_bytes.to_string(),
            r.baseline_total_payload_exact.to_string(),
            fmt6(r.baseline_match_pct),
            fmt6(r.baseline_balanced_accuracy_pct),
            fmt6(r.baseline_macro_f1_pct),
            fmt6(r.baseline_f1_upper_pct),
            r.global_patch_entries.to_string(),
            r.global_patch_bytes.to_string(),
            r.global_total_payload_exact.to_string(),
            fmt6(r.global_match_pct),
            fmt6(r.global_balanced_accuracy_pct),
            fmt6(r.global_macro_f1_pct),
            fmt6(r.global_f1_upper_pct),
            r.chunk_bytes.to_string(),
            r.chunk_count.to_string(),
            r.chunk_patch_entries.to_string(),
            r.chunk_patch_bytes.to_string(),
            r.chunk_total_payload_exact.to_string(),
            fmt6(r.chunk_match_pct),
            fmt6(r.chunk_balanced_accuracy_pct),
            fmt6(r.chunk_macro_f1_pct),
            fmt6(r.chunk_f1_upper_pct),
            csv_escape(&r.chunk_search_objective),
            r.chunk_raw_slack.to_string(),
            fmt6(r.chunk_mean_balanced_accuracy_pct),
            fmt6(r.chunk_mean_macro_f1_pct),
            fmt6(r.chunk_mean_minority_f1_pct),
            fmt6(r.chunk_mean_abs_minority_delta),
            r.chunk_majority_flip_count.to_string(),
            r.chunk_collapse_90_count.to_string(),
            csv_escape(&r.field_source),
            r.map_node_count.to_string(),
            r.map_depth_seen.to_string(),
            r.map_depth_shift.to_string(),
            r.map_max_depth_arg.to_string(),
            r.boundary_band.to_string(),
            r.boundary_delta.to_string(),
            r.field_margin.to_string(),
            r.lower_margin_add.to_string(),
            r.upper_margin_add.to_string(),
            r.lower_share_ppm_min.to_string(),
            r.upper_share_ppm_min.to_string(),
            r.field_patch_entries.to_string(),
            r.field_patch_bytes.to_string(),
            r.field_total_payload_exact.to_string(),
            fmt6(r.field_match_pct),
            fmt6(r.field_balanced_accuracy_pct),
            fmt6(r.field_macro_f1_pct),
            fmt6(r.field_f1_upper_pct),
            r.hybrid_upper_consensus_min.to_string(),
            r.hybrid_promoted_upper_count.to_string(),
            r.hybrid_patch_entries.to_string(),
            r.hybrid_patch_bytes.to_string(),
            r.hybrid_total_payload_exact.to_string(),
            fmt6(r.hybrid_match_pct),
            fmt6(r.hybrid_match_vs_baseline_pct),
            fmt6(r.hybrid_balanced_accuracy_pct),
            fmt6(r.hybrid_macro_f1_pct),
            fmt6(r.hybrid_f1_upper_pct),
            r.target_hist[0].to_string(),
            r.target_hist[1].to_string(),
            r.target_hist[2].to_string(),
            r.target_hist[3].to_string(),
            r.baseline_pred_hist[0].to_string(),
            r.baseline_pred_hist[1].to_string(),
            r.baseline_pred_hist[2].to_string(),
            r.baseline_pred_hist[3].to_string(),
            r.field_pred_hist[0].to_string(),
            r.field_pred_hist[1].to_string(),
            r.field_pred_hist[2].to_string(),
            r.field_pred_hist[3].to_string(),
            r.hybrid_pred_hist[0].to_string(),
            r.hybrid_pred_hist[1].to_string(),
            r.hybrid_pred_hist[2].to_string(),
            r.hybrid_pred_hist[3].to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            best_field_chunk_bytes.to_string(),
            csv_escape(&recommended_codec.label),
            recommended_codec.total_payload_exact.to_string(),
            fmt6(recommended_codec.raw_match_pct),
            fmt6(recommended_codec.upper_f1_pct),
            csv_escape(&recommended_north95.label),
            recommended_north95.total_payload_exact.to_string(),
            fmt6(recommended_north95.raw_match_pct),
            fmt6(recommended_north95.upper_f1_pct),
        ];
        out.push_str(&cells.join(","));
        out.push('\n');
    }

    for cand in stability_candidates {
        let cells = vec![
            "stability".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            csv_escape(&cand.label),
            cand.vote_threshold.to_string(),
            cand.unique_prediction_count.to_string(),
            cand.promoted_upper_count.to_string(),
            cand.source_cost_exact.to_string(),
            cand.patch_entries.to_string(),
            cand.patch_bytes.to_string(),
            cand.total_payload_exact.to_string(),
            fmt6(cand.raw_match_pct),
            fmt6(cand.balanced_accuracy_pct),
            fmt6(cand.macro_f1_pct),
            fmt6(cand.upper_f1_pct),
            cand.pred_hist[0].to_string(),
            cand.pred_hist[1].to_string(),
            cand.pred_hist[2].to_string(),
            cand.pred_hist[3].to_string(),
            best_field_chunk_bytes.to_string(),
            csv_escape(&recommended_codec.label),
            recommended_codec.total_payload_exact.to_string(),
            fmt6(recommended_codec.raw_match_pct),
            fmt6(recommended_codec.upper_f1_pct),
            csv_escape(&recommended_north95.label),
            recommended_north95.total_payload_exact.to_string(),
            fmt6(recommended_north95.raw_match_pct),
            fmt6(recommended_north95.upper_f1_pct),
        ];
        out.push_str(&cells.join(","));
        out.push('\n');
    }

    out
}

fn print_summary(
    out_path: Option<&str>,
    format: RenderFormat,
    runs: &[CaseAnchorRun],
    best_idx: usize,
    best_field_chunk_bytes: usize,
    stability_candidates: &[StabilityCandidate],
    recommended_codec: &RecommendedStrategy,
    recommended_north95: &RecommendedStrategy,
) {
    let best = &runs[best_idx].report;
    let stability_count = stability_candidates.len();
    match out_path {
        Some(path) => {
            eprintln!(
                "apextrace apex-map-case-anchor {:?}: out={} runs={} best_field_chunk={} best_boundary_band={} best_field_margin={} best_field_total_payload_exact={} best_field_match_pct={:.6} stability_candidates={} codec_recommendation={} north95_recommendation={}",
                format,
                path,
                runs.len(),
                best_field_chunk_bytes,
                best.boundary_band,
                best.field_margin,
                best.field_total_payload_exact,
                best.field_match_pct,
                stability_count,
                recommended_codec.label,
                recommended_north95.label,
            );
        }
        None => {
            eprintln!(
                "apextrace apex-map-case-anchor {:?}: runs={} best_field_chunk={} best_boundary_band={} best_field_margin={} best_field_total_payload_exact={} best_field_match_pct={:.6} stability_candidates={} codec_recommendation={} north95_recommendation={}",
                format,
                runs.len(),
                best_field_chunk_bytes,
                best.boundary_band,
                best.field_margin,
                best.field_total_payload_exact,
                best.field_match_pct,
                stability_count,
                recommended_codec.label,
                recommended_north95.label,
            );
        }
    }
}

fn render_prediction_ascii(predicted: &[u8]) -> String {
    let mut out = String::with_capacity(predicted.len().saturating_add(predicted.len() / 64));
    for (idx, &sym) in predicted.iter().enumerate() {
        if idx > 0 && idx % 64 == 0 {
            out.push('\n');
        }
        out.push(match sym {
            0 => 'l',
            1 => 'U',
            _ => '?',
        });
    }
    out.push('\n');
    out
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
        Err(anyhow!("apex-map-case-anchor: invalid class symbol {}", v))
    }
}

#[inline]
fn bucket_u8_local(b: u8, k: u8) -> u8 {
    ((b as u16 * k as u16) >> 8) as u8
}