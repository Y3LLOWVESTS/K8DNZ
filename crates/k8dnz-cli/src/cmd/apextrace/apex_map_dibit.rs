use anyhow::{anyhow, Context, Result};
use k8dnz_apextrace::{
    bytes_to_quats, generate_quats, quats_to_bytes, ApexKey, ApexMap, ApexMapCfg, RefineCfg,
    SearchCfg,
};
use k8dnz_core::symbol::patch::PatchList;

use crate::cmd::apextrace::{ApexMapDibitArgs, ChunkSearchObjective, RenderFormat};

use super::common::write_or_print;

const APEX_KEY_BYTES_EXACT: usize = 48;
const DIBIT_CLASS_COUNT: usize = 4;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct DibitScore {
    matches: u64,
    prefix: u64,
    total: u64,
    longest_run: u64,
    longest_run_start: u64,
}

impl DibitScore {
    fn better_than(&self, other: &Self) -> bool {
        (self.matches, self.longest_run, self.prefix) > (other.matches, other.longest_run, other.prefix)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct DibitDiagnostics {
    score: DibitScore,
    target_hist: [u64; DIBIT_CLASS_COUNT],
    pred_hist: [u64; DIBIT_CLASS_COUNT],
    byte_matches: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct PerClassMetrics {
    support: u64,
    predicted: u64,
    tp: u64,
    fp: u64,
    fn_: u64,
    precision_pct: f64,
    recall_pct: f64,
    f1_pct: f64,
}

#[derive(Clone, Debug, PartialEq)]
struct DibitMetrics {
    total: u64,
    target_hist: [u64; DIBIT_CLASS_COUNT],
    pred_hist: [u64; DIBIT_CLASS_COUNT],
    confusion: [[u64; DIBIT_CLASS_COUNT]; DIBIT_CLASS_COUNT],
    raw_match_pct: f64,
    majority_class: u8,
    majority_count: u64,
    majority_baseline_match_pct: f64,
    raw_match_vs_majority_pct: f64,
    pred_dominant_class: u8,
    pred_dominant_count: u64,
    pred_dominant_share_ppm: u64,
    pred_dominant_share_pct: f64,
    pred_collapse_90_flag: bool,
    target_entropy_bits: f64,
    pred_entropy_bits: f64,
    hist_l1: u64,
    hist_l1_pct: f64,
    balanced_accuracy_pct: f64,
    macro_precision_pct: f64,
    macro_recall_pct: f64,
    macro_f1_pct: f64,
    weighted_f1_pct: f64,
    per_class: [PerClassMetrics; DIBIT_CLASS_COUNT],
}

#[derive(Clone, Debug)]
struct DibitBest {
    key: ApexKey,
    predicted: Vec<u8>,
    diag: DibitDiagnostics,
}

#[derive(Clone, Debug)]
struct DibitChunkBest {
    chunk_index: usize,
    start_byte: usize,
    end_byte: usize,
    start_symbol: usize,
    end_symbol: usize,
    key: ApexKey,
    diag: DibitDiagnostics,
    patch_entries: usize,
    patch_bytes: usize,
}

#[derive(Clone, Debug)]
struct DibitChunkedBest {
    chunk_bytes: usize,
    chunk_key_bytes_exact: usize,
    predicted: Vec<u8>,
    diag: DibitDiagnostics,
    chunks: Vec<DibitChunkBest>,
}

#[derive(Clone, Copy, Debug, Default)]
struct DibitChunkSummary {
    mean_balanced_accuracy_pct: f64,
    mean_macro_f1_pct: f64,
    mean_abs_hist_l1_pct: f64,
    majority_flip_count: usize,
    collapse_90_count: usize,
}

#[derive(Clone, Debug)]
struct DibitRunRow {
    chunk_bytes: usize,
    boundary_band: usize,
    field_margin: u64,
    global_match_pct: f64,
    global_byte_match_pct: f64,
    global_total_payload_exact: usize,
    global_match_vs_majority_pct: f64,
    global_balanced_accuracy_pct: f64,
    global_macro_f1_pct: f64,
    global_pred_dominant_label: &'static str,
    global_pred_dominant_share_pct: f64,
    global_pred_collapse_90_flag: bool,
    chunk_match_pct: f64,
    chunk_byte_match_pct: f64,
    chunk_total_payload_exact: usize,
    chunk_match_vs_majority_pct: f64,
    chunk_balanced_accuracy_pct: f64,
    chunk_macro_f1_pct: f64,
    chunk_pred_dominant_label: &'static str,
    chunk_pred_dominant_share_pct: f64,
    chunk_pred_collapse_90_flag: bool,
    chunk_search_objective: &'static str,
    chunk_raw_slack: u64,
    chunk_mean_balanced_accuracy_pct: f64,
    chunk_mean_macro_f1_pct: f64,
    chunk_mean_abs_hist_l1_pct: f64,
    chunk_majority_flip_count: usize,
    chunk_collapse_90_count: usize,
    compact_manifest_bytes_exact: usize,
    compact_chunk_total_payload_exact: usize,
    map_node_count: usize,
    map_depth_seen: u8,
    map_max_depth_arg: u8,
    map_depth_shift: u8,
    field_patch_entries: usize,
    field_patch_bytes: usize,
    field_total_payload_exact: usize,
    compact_field_total_payload_exact: usize,
    field_match_pct: f64,
    field_byte_match_pct: f64,
    field_match_vs_majority_pct: f64,
    field_balanced_accuracy_pct: f64,
    field_macro_precision_pct: f64,
    field_macro_recall_pct: f64,
    field_macro_f1_pct: f64,
    field_weighted_f1_pct: f64,
    field_pred_entropy_bits: f64,
    field_hist_l1_pct: f64,
    field_pred_dominant_label: &'static str,
    field_pred_dominant_share_pct: f64,
    field_pred_collapse_90_flag: bool,
    field_precision_d00_pct: f64,
    field_recall_d00_pct: f64,
    field_f1_d00_pct: f64,
    field_precision_d01_pct: f64,
    field_recall_d01_pct: f64,
    field_f1_d01_pct: f64,
    field_precision_d10_pct: f64,
    field_recall_d10_pct: f64,
    field_f1_d10_pct: f64,
    field_precision_d11_pct: f64,
    field_recall_d11_pct: f64,
    field_f1_d11_pct: f64,
    field_conf_t0_p0: u64,
    field_conf_t0_p1: u64,
    field_conf_t0_p2: u64,
    field_conf_t0_p3: u64,
    field_conf_t1_p0: u64,
    field_conf_t1_p1: u64,
    field_conf_t1_p2: u64,
    field_conf_t1_p3: u64,
    field_conf_t2_p0: u64,
    field_conf_t2_p1: u64,
    field_conf_t2_p2: u64,
    field_conf_t2_p3: u64,
    field_conf_t3_p0: u64,
    field_conf_t3_p1: u64,
    field_conf_t3_p2: u64,
    field_conf_t3_p3: u64,
    target_hist: [u64; DIBIT_CLASS_COUNT],
    global_pred_hist: [u64; DIBIT_CLASS_COUNT],
    chunk_pred_hist: [u64; DIBIT_CLASS_COUNT],
    field_pred_hist: [u64; DIBIT_CLASS_COUNT],
}

#[derive(Clone, Debug)]
struct DibitRunArtifacts {
    row: DibitRunRow,
    chunked: DibitChunkedBest,
    field_predicted: Vec<u8>,
}

pub fn run_apex_map_dibit(args: ApexMapDibitArgs) -> Result<()> {
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

    let input = std::fs::read(&args.r#in).with_context(|| format!("read {}", args.r#in))?;
    if input.is_empty() {
        return Err(anyhow!("apex-map-dibit: input is empty"));
    }
    let target_symbols = bytes_to_dibit_symbols(&input)?;

    let cfg = SearchCfg {
        seed_from: args.seed_from,
        seed_count: args.seed_count,
        seed_step: args.seed_step,
        recipe_seed: args.recipe_seed,
    };

    let global = brute_force_best_dibit(&input, cfg)?;
    let global_metrics = compute_dibit_metrics(&target_symbols, &global.predicted)?;
    let global_patch = PatchList::from_pred_actual(&global.predicted, &target_symbols)
        .map_err(|e| anyhow!("apex-map-dibit global patch build failed: {e}"))?;
    let global_patch_bytes = global_patch.encode();
    let global_total_payload_exact = global_patch_bytes.len().saturating_add(APEX_KEY_BYTES_EXACT);

    let mut rows = Vec::<DibitRunArtifacts>::new();

    for &chunk_bytes in &chunk_values {
        let (chunked, chunk_summary) = brute_force_best_dibit_chunked(
            &input,
            cfg,
            chunk_bytes,
            args.chunk_search_objective,
            args.chunk_raw_slack,
        )?;
        let chunk_metrics = compute_dibit_metrics(&target_symbols, &chunked.predicted)?;
        let chunk_patch = PatchList::from_pred_actual(&chunked.predicted, &target_symbols)
            .map_err(|e| anyhow!("apex-map-dibit chunk patch build failed: {e}"))?;
        let chunk_patch_bytes = chunk_patch.encode();
        let compact_manifest = LocalCompactManifest::from_chunked(&chunked)?;
        let compact_manifest_bytes = compact_manifest.encode();
        let compact_chunk_total_payload_exact = compact_manifest_bytes.len().saturating_add(chunk_patch_bytes.len());

        let map = ApexMap::from_symbols(
            &chunked.predicted,
            ApexMapCfg {
                class_count: 4,
                max_depth: args.map_max_depth,
                depth_shift: args.map_depth_shift.max(1),
            },
        )?;
        let boundaries = chunked
            .chunks
            .iter()
            .filter(|chunk| chunk.end_symbol < chunked.predicted.len())
            .map(|chunk| chunk.end_symbol)
            .collect::<Vec<_>>();

        for &boundary_band in &boundary_band_values {
            for &field_margin in &field_margin_values {
                let refine_cfg = RefineCfg {
                    band: boundary_band,
                    delta: args.boundary_delta,
                    base_margin: field_margin,
                    ..RefineCfg::default()
                };
                let (field_predicted, field_stats) = map.refine_boundaries(&chunked.predicted, &boundaries, refine_cfg)?;
                let field_metrics = compute_dibit_metrics(&target_symbols, &field_predicted)?;
                let field_patch = PatchList::from_pred_actual(&field_predicted, &target_symbols)
                    .map_err(|e| anyhow!("apex-map-dibit field patch build failed: {e}"))?;
                let field_patch_bytes = field_patch.encode();
                let compact_field_total_payload_exact = compact_manifest_bytes
                    .len()
                    .saturating_add(field_patch_bytes.len());
                let field_total_payload_exact = field_patch_bytes.len().saturating_add(chunked.chunk_key_bytes_exact);

                rows.push(DibitRunArtifacts {
                    row: DibitRunRow {
                        chunk_bytes,
                        boundary_band,
                        field_margin,
                        global_match_pct: global_metrics.raw_match_pct,
                        global_byte_match_pct: byte_match_pct(global.diag.byte_matches, input.len() as u64),
                        global_total_payload_exact,
                        global_match_vs_majority_pct: global_metrics.raw_match_vs_majority_pct,
                        global_balanced_accuracy_pct: global_metrics.balanced_accuracy_pct,
                        global_macro_f1_pct: global_metrics.macro_f1_pct,
                        global_pred_dominant_label: dibit_label(global_metrics.pred_dominant_class),
                        global_pred_dominant_share_pct: global_metrics.pred_dominant_share_pct,
                        global_pred_collapse_90_flag: global_metrics.pred_collapse_90_flag,
                        chunk_match_pct: chunk_metrics.raw_match_pct,
                        chunk_byte_match_pct: byte_match_pct(chunked.diag.byte_matches, input.len() as u64),
                        chunk_total_payload_exact: chunk_patch_bytes.len().saturating_add(chunked.chunk_key_bytes_exact),
                        chunk_match_vs_majority_pct: chunk_metrics.raw_match_vs_majority_pct,
                        chunk_balanced_accuracy_pct: chunk_metrics.balanced_accuracy_pct,
                        chunk_macro_f1_pct: chunk_metrics.macro_f1_pct,
                        chunk_pred_dominant_label: dibit_label(chunk_metrics.pred_dominant_class),
                        chunk_pred_dominant_share_pct: chunk_metrics.pred_dominant_share_pct,
                        chunk_pred_collapse_90_flag: chunk_metrics.pred_collapse_90_flag,
                        chunk_search_objective: dibit_chunk_objective_name(args.chunk_search_objective),
                        chunk_raw_slack: args.chunk_raw_slack,
                        chunk_mean_balanced_accuracy_pct: chunk_summary.mean_balanced_accuracy_pct,
                        chunk_mean_macro_f1_pct: chunk_summary.mean_macro_f1_pct,
                        chunk_mean_abs_hist_l1_pct: chunk_summary.mean_abs_hist_l1_pct,
                        chunk_majority_flip_count: chunk_summary.majority_flip_count,
                        chunk_collapse_90_count: chunk_summary.collapse_90_count,
                        compact_manifest_bytes_exact: compact_manifest_bytes.len(),
                        compact_chunk_total_payload_exact,
                        map_node_count: map.node_count(),
                        map_depth_seen: map.max_depth_seen(),
                        map_max_depth_arg: args.map_max_depth,
                        map_depth_shift: args.map_depth_shift.max(1),
                        field_patch_entries: field_patch.entries.len(),
                        field_patch_bytes: field_patch_bytes.len(),
                        field_total_payload_exact,
                        compact_field_total_payload_exact,
                        field_match_pct: field_metrics.raw_match_pct,
                        field_byte_match_pct: byte_match_pct(count_byte_matches(&target_symbols, &field_predicted)?, input.len() as u64),
                        field_match_vs_majority_pct: field_metrics.raw_match_vs_majority_pct,
                        field_balanced_accuracy_pct: field_metrics.balanced_accuracy_pct,
                        field_macro_precision_pct: field_metrics.macro_precision_pct,
                        field_macro_recall_pct: field_metrics.macro_recall_pct,
                        field_macro_f1_pct: field_metrics.macro_f1_pct,
                        field_weighted_f1_pct: field_metrics.weighted_f1_pct,
                        field_pred_entropy_bits: field_metrics.pred_entropy_bits,
                        field_hist_l1_pct: field_metrics.hist_l1_pct,
                        field_pred_dominant_label: dibit_label(field_metrics.pred_dominant_class),
                        field_pred_dominant_share_pct: field_metrics.pred_dominant_share_pct,
                        field_pred_collapse_90_flag: field_metrics.pred_collapse_90_flag,
                        field_precision_d00_pct: field_metrics.per_class[0].precision_pct,
                        field_recall_d00_pct: field_metrics.per_class[0].recall_pct,
                        field_f1_d00_pct: field_metrics.per_class[0].f1_pct,
                        field_precision_d01_pct: field_metrics.per_class[1].precision_pct,
                        field_recall_d01_pct: field_metrics.per_class[1].recall_pct,
                        field_f1_d01_pct: field_metrics.per_class[1].f1_pct,
                        field_precision_d10_pct: field_metrics.per_class[2].precision_pct,
                        field_recall_d10_pct: field_metrics.per_class[2].recall_pct,
                        field_f1_d10_pct: field_metrics.per_class[2].f1_pct,
                        field_precision_d11_pct: field_metrics.per_class[3].precision_pct,
                        field_recall_d11_pct: field_metrics.per_class[3].recall_pct,
                        field_f1_d11_pct: field_metrics.per_class[3].f1_pct,
                        field_conf_t0_p0: field_metrics.confusion[0][0],
                        field_conf_t0_p1: field_metrics.confusion[0][1],
                        field_conf_t0_p2: field_metrics.confusion[0][2],
                        field_conf_t0_p3: field_metrics.confusion[0][3],
                        field_conf_t1_p0: field_metrics.confusion[1][0],
                        field_conf_t1_p1: field_metrics.confusion[1][1],
                        field_conf_t1_p2: field_metrics.confusion[1][2],
                        field_conf_t1_p3: field_metrics.confusion[1][3],
                        field_conf_t2_p0: field_metrics.confusion[2][0],
                        field_conf_t2_p1: field_metrics.confusion[2][1],
                        field_conf_t2_p2: field_metrics.confusion[2][2],
                        field_conf_t2_p3: field_metrics.confusion[2][3],
                        field_conf_t3_p0: field_metrics.confusion[3][0],
                        field_conf_t3_p1: field_metrics.confusion[3][1],
                        field_conf_t3_p2: field_metrics.confusion[3][2],
                        field_conf_t3_p3: field_metrics.confusion[3][3],
                        target_hist: field_metrics.target_hist,
                        global_pred_hist: global_metrics.pred_hist,
                        chunk_pred_hist: chunk_metrics.pred_hist,
                        field_pred_hist: field_metrics.pred_hist,
                    },
                    chunked: chunked.clone(),
                    field_predicted,
                });
            }
        }
    }

    if rows.is_empty() {
        return Err(anyhow!("apex-map-dibit: no rows produced"));
    }

    let best_idx = select_best_row(&rows).ok_or_else(|| anyhow!("apex-map-dibit: no best row selected"))?;
    let best = &rows[best_idx];

    if let Some(path) = args.out_key.as_deref() {
        std::fs::write(path, render_chunk_keys_csv(&best.chunked))
            .with_context(|| format!("write {}", path))?;
    }
    if let Some(path) = args.out_pred.as_deref() {
        let bytes = dibit_symbols_to_bytes(&best.field_predicted)?;
        std::fs::write(path, bytes).with_context(|| format!("write {}", path))?;
    }
    if let Some(path) = args.out_diag.as_deref() {
        std::fs::write(path, render_rows_csv(&rows)).with_context(|| format!("write {}", path))?;
        eprintln!(
            "apextrace apex-map-dibit sweep diagnostics saved: {} rows={}",
            path,
            rows.len()
        );
    }

    let body = match args.format {
        RenderFormat::Txt => render_rows_txt(&input, &target_symbols, &rows, best_idx),
        RenderFormat::Csv => render_rows_csv(&rows),
    };
    write_or_print(args.out.as_deref(), &body)
}

fn select_best_row(rows: &[DibitRunArtifacts]) -> Option<usize> {
    rows.iter()
        .enumerate()
        .min_by_key(|(_, row)| {
            let r = &row.row;
            (
                r.field_pred_collapse_90_flag,
                r.chunk_majority_flip_count > 0,
                r.compact_field_total_payload_exact,
                r.field_patch_bytes,
                std::cmp::Reverse(scale_pct(r.field_byte_match_pct)),
                std::cmp::Reverse(scale_pct(r.field_match_pct)),
                std::cmp::Reverse(scale_pct(r.field_balanced_accuracy_pct)),
                std::cmp::Reverse(scale_pct(r.field_macro_f1_pct)),
                scale_pct(r.field_hist_l1_pct),
            )
        })
        .map(|(idx, _)| idx)
}

fn bytes_to_dibit_symbols(bytes: &[u8]) -> Result<Vec<u8>> {
    bytes_to_quats(bytes)
        .map_err(|e| anyhow!("apex-map-dibit bytes_to_quats failed: {e}"))?
        .into_iter()
        .map(|q| {
            q.checked_sub(1)
                .ok_or_else(|| anyhow!("apex-map-dibit invalid quat {}", q))
        })
        .collect()
}

fn dibit_symbols_to_bytes(symbols: &[u8]) -> Result<Vec<u8>> {
    let quats = symbols
        .iter()
        .map(|&sym| {
            if sym > 3 {
                Err(anyhow!("apex-map-dibit invalid dibit symbol {}", sym))
            } else {
                Ok(sym + 1)
            }
        })
        .collect::<Result<Vec<_>>>()?;
    quats_to_bytes(&quats).map_err(|e| anyhow!("apex-map-dibit quats_to_bytes failed: {e}"))
}

fn brute_force_best_dibit(input_bytes: &[u8], cfg: SearchCfg) -> Result<DibitBest> {
    if cfg.seed_step == 0 {
        return Err(anyhow!("apex-map-dibit: seed_step must be >= 1"));
    }
    let target = bytes_to_dibit_symbols(input_bytes)?;
    let mut best: Option<DibitBest> = None;
    let byte_len = input_bytes.len() as u64;

    for quadrant in 0u8..=3 {
        let mut i = 0u64;
        while i < cfg.seed_count {
            let seed = cfg.seed_from.saturating_add(i.saturating_mul(cfg.seed_step));
            let key = ApexKey::new_dibit_v1(byte_len, quadrant, seed, cfg.recipe_seed)?;
            let predicted = generate_quats(&key)
                .map_err(|e| anyhow!("apex-map-dibit generate_quats failed: {e}"))?
                .into_iter()
                .map(|q| q - 1)
                .collect::<Vec<_>>();
            let diag = score_dibit_symbols(&target, &predicted)?;
            let cand = DibitBest { key, predicted, diag };
            match &best {
                None => best = Some(cand),
                Some(cur) if cand.diag.score.better_than(&cur.diag.score) => best = Some(cand),
                Some(_) => {}
            }
            i = i.saturating_add(1);
        }
    }

    best.ok_or_else(|| anyhow!("apex-map-dibit: search produced no candidates"))
}

fn brute_force_best_dibit_chunked(
    input_bytes: &[u8],
    cfg: SearchCfg,
    chunk_bytes: usize,
    objective: ChunkSearchObjective,
    raw_slack: u64,
) -> Result<(DibitChunkedBest, DibitChunkSummary)> {
    if chunk_bytes == 0 {
        return Err(anyhow!("apex-map-dibit: chunk_bytes must be >= 1"));
    }

    let mut predicted = Vec::with_capacity(input_bytes.len().saturating_mul(4));
    let mut chunks = Vec::new();
    let mut start_byte = 0usize;
    let mut chunk_index = 0usize;
    let mut sum_balanced_accuracy_pct = 0.0;
    let mut sum_macro_f1_pct = 0.0;
    let mut sum_abs_hist_l1_pct = 0.0;
    let mut majority_flip_count = 0usize;
    let mut collapse_90_count = 0usize;

    while start_byte < input_bytes.len() {
        let end_byte = start_byte.saturating_add(chunk_bytes).min(input_bytes.len());
        let slice = &input_bytes[start_byte..end_byte];
        let target = bytes_to_dibit_symbols(slice)?;
        let (best, metrics) = brute_force_best_dibit_objective(slice, cfg, objective, raw_slack)?;
        let patch = PatchList::from_pred_actual(&best.predicted, &target)
            .map_err(|e| anyhow!("apex-map-dibit chunk patch build failed: {e}"))?;
        let patch_bytes = patch.encode();

        sum_balanced_accuracy_pct += metrics.balanced_accuracy_pct;
        sum_macro_f1_pct += metrics.macro_f1_pct;
        sum_abs_hist_l1_pct += metrics.hist_l1_pct;
        if metrics.pred_dominant_class != metrics.majority_class {
            majority_flip_count = majority_flip_count.saturating_add(1);
        }
        if metrics.pred_collapse_90_flag {
            collapse_90_count = collapse_90_count.saturating_add(1);
        }

        let start_symbol = predicted.len();
        predicted.extend_from_slice(&best.predicted);
        let end_symbol = predicted.len();

        chunks.push(DibitChunkBest {
            chunk_index,
            start_byte,
            end_byte,
            start_symbol,
            end_symbol,
            key: best.key,
            diag: best.diag,
            patch_entries: patch.entries.len(),
            patch_bytes: patch_bytes.len(),
        });

        start_byte = end_byte;
        chunk_index = chunk_index.saturating_add(1);
    }

    let whole_target = bytes_to_dibit_symbols(input_bytes)?;
    let diag = score_dibit_symbols(&whole_target, &predicted)?;
    let denom = chunks.len().max(1) as f64;

    Ok((
        DibitChunkedBest {
            chunk_bytes,
            chunk_key_bytes_exact: chunks.len().saturating_mul(APEX_KEY_BYTES_EXACT),
            predicted,
            diag,
            chunks,
        },
        DibitChunkSummary {
            mean_balanced_accuracy_pct: sum_balanced_accuracy_pct / denom,
            mean_macro_f1_pct: sum_macro_f1_pct / denom,
            mean_abs_hist_l1_pct: sum_abs_hist_l1_pct / denom,
            majority_flip_count,
            collapse_90_count,
        },
    ))
}

fn brute_force_best_dibit_objective(
    input_bytes: &[u8],
    cfg: SearchCfg,
    objective: ChunkSearchObjective,
    raw_slack: u64,
) -> Result<(DibitBest, DibitMetrics)> {
    let target = bytes_to_dibit_symbols(input_bytes)?;
    if objective == ChunkSearchObjective::Raw {
        let best = brute_force_best_dibit(input_bytes, cfg)?;
        let metrics = compute_dibit_metrics(&target, &best.predicted)?;
        return Ok((best, metrics));
    }

    let raw_anchor = brute_force_best_dibit(input_bytes, cfg)?;
    let raw_anchor_matches = raw_anchor.diag.score.matches;
    let byte_len = input_bytes.len() as u64;
    let mut best: Option<(DibitBest, DibitMetrics)> = None;

    for quadrant in 0u8..=3 {
        let mut i = 0u64;
        while i < cfg.seed_count {
            let seed = cfg.seed_from.saturating_add(i.saturating_mul(cfg.seed_step));
            let key = ApexKey::new_dibit_v1(byte_len, quadrant, seed, cfg.recipe_seed)?;
            let predicted = generate_quats(&key)
                .map_err(|e| anyhow!("apex-map-dibit generate_quats failed: {e}"))?
                .into_iter()
                .map(|q| q - 1)
                .collect::<Vec<_>>();
            let diag = score_dibit_symbols(&target, &predicted)?;

            if objective == ChunkSearchObjective::RawGuarded
                && raw_anchor_matches.saturating_sub(diag.score.matches) > raw_slack
            {
                i = i.saturating_add(1);
                continue;
            }

            let metrics = compute_dibit_metrics(&target, &predicted)?;
            let cand = DibitBest { key, predicted, diag };
            match &best {
                None => best = Some((cand, metrics)),
                Some((cur, cur_metrics))
                    if dibit_candidate_better(&cand, &metrics, cur, cur_metrics, objective) =>
                {
                    best = Some((cand, metrics));
                }
                Some(_) => {}
            }
            i = i.saturating_add(1);
        }
    }

    if let Some(best) = best {
        Ok(best)
    } else {
        let metrics = compute_dibit_metrics(&target, &raw_anchor.predicted)?;
        Ok((raw_anchor, metrics))
    }
}

fn dibit_candidate_better(
    cand: &DibitBest,
    cand_metrics: &DibitMetrics,
    cur: &DibitBest,
    cur_metrics: &DibitMetrics,
    objective: ChunkSearchObjective,
) -> bool {
    let cand_majority_flip = cand_metrics.pred_dominant_class != cand_metrics.majority_class;
    let cur_majority_flip = cur_metrics.pred_dominant_class != cur_metrics.majority_class;

    let cand_tuple = (
        cand_metrics.pred_collapse_90_flag,
        cand_majority_flip,
        scale_pct(cand_metrics.hist_l1_pct),
        std::cmp::Reverse(scale_pct(cand_metrics.balanced_accuracy_pct)),
        std::cmp::Reverse(scale_pct(cand_metrics.macro_f1_pct)),
        std::cmp::Reverse(cand.diag.score.matches),
        std::cmp::Reverse(cand.diag.score.longest_run),
        std::cmp::Reverse(cand.diag.score.prefix),
    );
    let cur_tuple = (
        cur_metrics.pred_collapse_90_flag,
        cur_majority_flip,
        scale_pct(cur_metrics.hist_l1_pct),
        std::cmp::Reverse(scale_pct(cur_metrics.balanced_accuracy_pct)),
        std::cmp::Reverse(scale_pct(cur_metrics.macro_f1_pct)),
        std::cmp::Reverse(cur.diag.score.matches),
        std::cmp::Reverse(cur.diag.score.longest_run),
        std::cmp::Reverse(cur.diag.score.prefix),
    );

    match objective {
        ChunkSearchObjective::Raw => cand.diag.score.better_than(&cur.diag.score),
        ChunkSearchObjective::RawGuarded | ChunkSearchObjective::Honest | ChunkSearchObjective::Newline => {
            cand_tuple < cur_tuple
        }
    }
}

fn score_dibit_symbols(target: &[u8], predicted: &[u8]) -> Result<DibitDiagnostics> {
    if target.len() != predicted.len() {
        return Err(anyhow!(
            "apex-map-dibit: target len {} != predicted len {}",
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
    let mut target_hist = [0u64; DIBIT_CLASS_COUNT];
    let mut pred_hist = [0u64; DIBIT_CLASS_COUNT];

    for (idx, (&t, &p)) in target.iter().zip(predicted.iter()).enumerate() {
        let ti = dibit_slot(t)?;
        let pi = dibit_slot(p)?;
        target_hist[ti] = target_hist[ti].saturating_add(1);
        pred_hist[pi] = pred_hist[pi].saturating_add(1);

        if ti == pi {
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

    let byte_matches = count_byte_matches(target, predicted)?;

    Ok(DibitDiagnostics {
        score: DibitScore {
            matches,
            prefix,
            total: target.len() as u64,
            longest_run,
            longest_run_start,
        },
        target_hist,
        pred_hist,
        byte_matches,
    })
}

fn compute_dibit_metrics(target: &[u8], predicted: &[u8]) -> Result<DibitMetrics> {
    if target.len() != predicted.len() {
        return Err(anyhow!(
            "apex-map-dibit metrics: target len {} != predicted len {}",
            target.len(),
            predicted.len()
        ));
    }

    let mut target_hist = [0u64; DIBIT_CLASS_COUNT];
    let mut pred_hist = [0u64; DIBIT_CLASS_COUNT];
    let mut confusion = [[0u64; DIBIT_CLASS_COUNT]; DIBIT_CLASS_COUNT];
    let mut matches = 0u64;

    for (&t, &p) in target.iter().zip(predicted.iter()) {
        let ti = dibit_slot(t)?;
        let pi = dibit_slot(p)?;
        target_hist[ti] = target_hist[ti].saturating_add(1);
        pred_hist[pi] = pred_hist[pi].saturating_add(1);
        confusion[ti][pi] = confusion[ti][pi].saturating_add(1);
        if ti == pi {
            matches = matches.saturating_add(1);
        }
    }

    let total = target.len() as u64;
    let raw_match_pct = pct(matches, total);
    let (majority_class, majority_count) = argmax_hist(&target_hist);
    let majority_baseline_match_pct = pct(majority_count, total);
    let raw_match_vs_majority_pct = raw_match_pct - majority_baseline_match_pct;

    let (pred_dominant_class, pred_dominant_count) = argmax_hist(&pred_hist);
    let pred_dominant_share_ppm = ppm(pred_dominant_count, total);
    let pred_dominant_share_pct = pct(pred_dominant_count, total);
    let pred_collapse_90_flag = pred_dominant_share_ppm >= 900_000;

    let target_entropy_bits = entropy_bits(&target_hist, total);
    let pred_entropy_bits = entropy_bits(&pred_hist, total);
    let hist_l1 = target_hist
        .iter()
        .zip(pred_hist.iter())
        .map(|(&a, &b)| a.abs_diff(b))
        .sum::<u64>();
    let hist_l1_pct = if total == 0 {
        0.0
    } else {
        (hist_l1 as f64) * 100.0 / ((2 * total) as f64)
    };

    let mut per_class: [PerClassMetrics; DIBIT_CLASS_COUNT] = std::array::from_fn(|_| PerClassMetrics::default());
    let mut balanced_accuracy_sum = 0.0;
    let mut macro_precision_sum = 0.0;
    let mut macro_recall_sum = 0.0;
    let mut macro_f1_sum = 0.0;
    let mut weighted_f1_sum = 0.0;
    let mut supported_recall_class_count = 0u64;
    let mut active_macro_class_count = 0u64;

    for cls in 0..DIBIT_CLASS_COUNT {
        let support = target_hist[cls];
        let predicted_count = pred_hist[cls];
        let tp = confusion[cls][cls];
        let fn_ = support.saturating_sub(tp);
        let fp = predicted_count.saturating_sub(tp);
        let precision_pct = pct(tp, predicted_count);
        let recall_pct = pct(tp, support);
        let f1_pct = f1_pct(precision_pct, recall_pct);

        per_class[cls] = PerClassMetrics {
            support,
            predicted: predicted_count,
            tp,
            fp,
            fn_,
            precision_pct,
            recall_pct,
            f1_pct,
        };

        if support > 0 {
            supported_recall_class_count = supported_recall_class_count.saturating_add(1);
            balanced_accuracy_sum += recall_pct;
            macro_recall_sum += recall_pct;
            weighted_f1_sum += f1_pct * (support as f64);
        }
        if support > 0 || predicted_count > 0 {
            active_macro_class_count = active_macro_class_count.saturating_add(1);
            macro_precision_sum += precision_pct;
            macro_f1_sum += f1_pct;
        }
    }

    let balanced_accuracy_pct = if supported_recall_class_count == 0 {
        0.0
    } else {
        balanced_accuracy_sum / (supported_recall_class_count as f64)
    };
    let macro_precision_pct = if active_macro_class_count == 0 {
        0.0
    } else {
        macro_precision_sum / (active_macro_class_count as f64)
    };
    let macro_recall_pct = if supported_recall_class_count == 0 {
        0.0
    } else {
        macro_recall_sum / (supported_recall_class_count as f64)
    };
    let macro_f1_pct = if active_macro_class_count == 0 {
        0.0
    } else {
        macro_f1_sum / (active_macro_class_count as f64)
    };
    let weighted_f1_pct = if total == 0 {
        0.0
    } else {
        weighted_f1_sum / (total as f64)
    };

    Ok(DibitMetrics {
        total,
        target_hist,
        pred_hist,
        confusion,
        raw_match_pct,
        majority_class,
        majority_count,
        majority_baseline_match_pct,
        raw_match_vs_majority_pct,
        pred_dominant_class,
        pred_dominant_count,
        pred_dominant_share_ppm,
        pred_dominant_share_pct,
        pred_collapse_90_flag,
        target_entropy_bits,
        pred_entropy_bits,
        hist_l1,
        hist_l1_pct,
        balanced_accuracy_pct,
        macro_precision_pct,
        macro_recall_pct,
        macro_f1_pct,
        weighted_f1_pct,
        per_class,
    })
}

fn count_byte_matches(target: &[u8], predicted: &[u8]) -> Result<u64> {
    if target.len() != predicted.len() {
        return Err(anyhow!(
            "apex-map-dibit byte match: target len {} != predicted len {}",
            target.len(),
            predicted.len()
        ));
    }
    if target.len() % 4 != 0 {
        return Err(anyhow!(
            "apex-map-dibit byte match: dibit len {} is not divisible by 4",
            target.len()
        ));
    }
    let mut matches = 0u64;
    for (t, p) in target.chunks_exact(4).zip(predicted.chunks_exact(4)) {
        if t == p {
            matches = matches.saturating_add(1);
        }
    }
    Ok(matches)
}

fn dibit_label(v: u8) -> &'static str {
    match v {
        0 => "00",
        1 => "01",
        2 => "10",
        3 => "11",
        _ => "invalid",
    }
}

fn dibit_slot(v: u8) -> Result<usize> {
    match v {
        0..=3 => Ok(v as usize),
        _ => Err(anyhow!("apex-map-dibit invalid dibit symbol {}", v)),
    }
}

fn dibit_chunk_objective_name(objective: ChunkSearchObjective) -> &'static str {
    match objective {
        ChunkSearchObjective::Raw => "raw",
        ChunkSearchObjective::RawGuarded => "raw-guarded",
        ChunkSearchObjective::Honest => "honest",
        ChunkSearchObjective::Newline => "honest",
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LocalCompactChunkKey {
    root_quadrant: u8,
    root_seed: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LocalCompactManifest {
    total_len: u64,
    chunk_bytes: u64,
    recipe_seed: u64,
    keys: Vec<LocalCompactChunkKey>,
}

impl LocalCompactManifest {
    fn from_chunked(chunked: &DibitChunkedBest) -> Result<Self> {
        let recipe_seed = chunked
            .chunks
            .first()
            .map(|chunk| chunk.key.recipe_seed)
            .unwrap_or(0);
        for chunk in &chunked.chunks {
            if chunk.key.recipe_seed != recipe_seed {
                return Err(anyhow!(
                    "apex-map-dibit compact manifest requires shared recipe_seed across chunks"
                ));
            }
        }
        Ok(Self {
            total_len: chunked.predicted.len() as u64,
            chunk_bytes: chunked.chunk_bytes as u64,
            recipe_seed,
            keys: chunked
                .chunks
                .iter()
                .map(|chunk| LocalCompactChunkKey {
                    root_quadrant: chunk.key.root_quadrant,
                    root_seed: chunk.key.root_seed,
                })
                .collect(),
        })
    }

    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(b"AKCM");
        out.push(1u8);
        out.push(0u8);
        put_varint(self.total_len, &mut out);
        put_varint(self.chunk_bytes, &mut out);
        put_varint(self.keys.len() as u64, &mut out);
        out.extend_from_slice(&self.recipe_seed.to_le_bytes());
        for key in &self.keys {
            out.push(key.root_quadrant);
            out.extend_from_slice(&key.root_seed.to_le_bytes());
        }
        out
    }
}

fn render_chunk_keys_csv(chunked: &DibitChunkedBest) -> String {
    let recipe_seed = chunked
        .chunks
        .first()
        .map(|chunk| chunk.key.recipe_seed)
        .unwrap_or(0);
    let mut out = String::from(
        "chunk_bytes,total_len_symbols,chunk_count,shared_recipe_seed_hex,chunk_index,start_byte,end_byte,start_symbol,end_symbol,root_quadrant,root_seed_hex\n",
    );
    for chunk in &chunked.chunks {
        out.push_str(&format!(
            "{},{},{},0x{:016X},{},{},{},{},{},{},0x{:016X}\n",
            chunked.chunk_bytes,
            chunked.predicted.len(),
            chunked.chunks.len(),
            recipe_seed,
            chunk.chunk_index,
            chunk.start_byte,
            chunk.end_byte,
            chunk.start_symbol,
            chunk.end_symbol,
            chunk.key.root_quadrant,
            chunk.key.root_seed,
        ));
    }
    out
}

fn render_rows_txt(input: &[u8], target_symbols: &[u8], rows: &[DibitRunArtifacts], best_idx: usize) -> String {
    let mut out = String::new();
    for (idx, row) in rows.iter().enumerate() {
        let r = &row.row;
        push_kv(&mut out, "input_bytes", input.len());
        push_kv(&mut out, "dibit_len", target_symbols.len());
        push_kv(&mut out, "chunk_bytes", r.chunk_bytes);
        push_kv(&mut out, "boundary_band", r.boundary_band);
        push_kv(&mut out, "field_margin", r.field_margin);
        push_kv(&mut out, "global_match_pct", format6(r.global_match_pct));
        push_kv(&mut out, "global_byte_match_pct", format6(r.global_byte_match_pct));
        push_kv(&mut out, "global_total_payload_exact", r.global_total_payload_exact);
        push_kv(&mut out, "chunk_match_pct", format6(r.chunk_match_pct));
        push_kv(&mut out, "chunk_byte_match_pct", format6(r.chunk_byte_match_pct));
        push_kv(&mut out, "chunk_total_payload_exact", r.chunk_total_payload_exact);
        push_kv(&mut out, "chunk_search_objective", r.chunk_search_objective);
        push_kv(&mut out, "chunk_raw_slack", r.chunk_raw_slack);
        push_kv(&mut out, "chunk_mean_balanced_accuracy_pct", format6(r.chunk_mean_balanced_accuracy_pct));
        push_kv(&mut out, "chunk_mean_macro_f1_pct", format6(r.chunk_mean_macro_f1_pct));
        push_kv(&mut out, "chunk_mean_abs_hist_l1_pct", format6(r.chunk_mean_abs_hist_l1_pct));
        push_kv(&mut out, "chunk_majority_flip_count", r.chunk_majority_flip_count);
        push_kv(&mut out, "chunk_collapse_90_count", r.chunk_collapse_90_count);
        push_kv(&mut out, "compact_manifest_bytes_exact", r.compact_manifest_bytes_exact);
        push_kv(&mut out, "compact_chunk_total_payload_exact", r.compact_chunk_total_payload_exact);
        push_kv(&mut out, "field_patch_entries", r.field_patch_entries);
        push_kv(&mut out, "field_patch_bytes", r.field_patch_bytes);
        push_kv(&mut out, "field_total_payload_exact", r.field_total_payload_exact);
        push_kv(&mut out, "compact_field_total_payload_exact", r.compact_field_total_payload_exact);
        push_kv(&mut out, "field_match_pct", format6(r.field_match_pct));
        push_kv(&mut out, "field_byte_match_pct", format6(r.field_byte_match_pct));
        push_kv(&mut out, "field_match_vs_majority_pct", format6(r.field_match_vs_majority_pct));
        push_kv(&mut out, "field_balanced_accuracy_pct", format6(r.field_balanced_accuracy_pct));
        push_kv(&mut out, "field_macro_precision_pct", format6(r.field_macro_precision_pct));
        push_kv(&mut out, "field_macro_recall_pct", format6(r.field_macro_recall_pct));
        push_kv(&mut out, "field_macro_f1_pct", format6(r.field_macro_f1_pct));
        push_kv(&mut out, "field_weighted_f1_pct", format6(r.field_weighted_f1_pct));
        push_kv(&mut out, "field_pred_entropy_bits", format6(r.field_pred_entropy_bits));
        push_kv(&mut out, "field_hist_l1_pct", format6(r.field_hist_l1_pct));
        push_kv(&mut out, "field_pred_dominant_label", r.field_pred_dominant_label);
        push_kv(&mut out, "field_pred_dominant_share_pct", format6(r.field_pred_dominant_share_pct));
        push_kv(&mut out, "field_pred_collapse_90_flag", r.field_pred_collapse_90_flag);
        push_kv(&mut out, "field_precision_d00_pct", format6(r.field_precision_d00_pct));
        push_kv(&mut out, "field_recall_d00_pct", format6(r.field_recall_d00_pct));
        push_kv(&mut out, "field_f1_d00_pct", format6(r.field_f1_d00_pct));
        push_kv(&mut out, "field_precision_d01_pct", format6(r.field_precision_d01_pct));
        push_kv(&mut out, "field_recall_d01_pct", format6(r.field_recall_d01_pct));
        push_kv(&mut out, "field_f1_d01_pct", format6(r.field_f1_d01_pct));
        push_kv(&mut out, "field_precision_d10_pct", format6(r.field_precision_d10_pct));
        push_kv(&mut out, "field_recall_d10_pct", format6(r.field_recall_d10_pct));
        push_kv(&mut out, "field_f1_d10_pct", format6(r.field_f1_d10_pct));
        push_kv(&mut out, "field_precision_d11_pct", format6(r.field_precision_d11_pct));
        push_kv(&mut out, "field_recall_d11_pct", format6(r.field_recall_d11_pct));
        push_kv(&mut out, "field_f1_d11_pct", format6(r.field_f1_d11_pct));
        push_kv(&mut out, "target_hist_d00", r.target_hist[0]);
        push_kv(&mut out, "target_hist_d01", r.target_hist[1]);
        push_kv(&mut out, "target_hist_d10", r.target_hist[2]);
        push_kv(&mut out, "target_hist_d11", r.target_hist[3]);
        push_kv(&mut out, "field_pred_hist_d00", r.field_pred_hist[0]);
        push_kv(&mut out, "field_pred_hist_d01", r.field_pred_hist[1]);
        push_kv(&mut out, "field_pred_hist_d10", r.field_pred_hist[2]);
        push_kv(&mut out, "field_pred_hist_d11", r.field_pred_hist[3]);
        if idx == best_idx {
            out.push_str("apextrace apex-map-dibit best_row=true\n");
        }
        if idx + 1 != rows.len() {
            out.push_str("\n---\n");
        }
    }

    let best = &rows[best_idx].row;
    out.push_str("\n---\n");
    out.push_str(&format!(
        "apextrace apex-map-dibit best: chunk_bytes={} boundary_band={} field_margin={} compact_field_total_payload_exact={} field_patch_bytes={} field_match_pct={} field_byte_match_pct={} majority_baseline_match_pct={} field_match_vs_majority_pct={} field_balanced_accuracy_pct={} field_macro_f1_pct={} field_pred_dominant_label={} field_pred_dominant_share_pct={} field_pred_collapse_90_flag={}\n",
        best.chunk_bytes,
        best.boundary_band,
        best.field_margin,
        best.compact_field_total_payload_exact,
        best.field_patch_bytes,
        format6(best.field_match_pct),
        format6(best.field_byte_match_pct),
        format6(best.global_match_pct - best.global_match_vs_majority_pct),
        format6(best.field_match_vs_majority_pct),
        format6(best.field_balanced_accuracy_pct),
        format6(best.field_macro_f1_pct),
        best.field_pred_dominant_label,
        format6(best.field_pred_dominant_share_pct),
        best.field_pred_collapse_90_flag,
    ));
    out
}

fn render_rows_csv(rows: &[DibitRunArtifacts]) -> String {
    let mut out = String::from(
        "chunk_bytes,boundary_band,field_margin,chunk_search_objective,chunk_raw_slack,compact_field_total_payload_exact,field_patch_bytes,field_match_pct,field_byte_match_pct,field_match_vs_majority_pct,field_balanced_accuracy_pct,field_macro_f1_pct,field_pred_dominant_label,field_pred_dominant_share_pct,field_pred_collapse_90_flag,chunk_mean_balanced_accuracy_pct,chunk_mean_macro_f1_pct,chunk_mean_abs_hist_l1_pct,chunk_majority_flip_count,chunk_collapse_90_count,target_hist_d00,target_hist_d01,target_hist_d10,target_hist_d11,field_pred_hist_d00,field_pred_hist_d01,field_pred_hist_d10,field_pred_hist_d11\n",
    );
    for row in rows {
        let r = &row.row;
        let cols = vec![
            r.chunk_bytes.to_string(),
            r.boundary_band.to_string(),
            r.field_margin.to_string(),
            r.chunk_search_objective.to_string(),
            r.chunk_raw_slack.to_string(),
            r.compact_field_total_payload_exact.to_string(),
            r.field_patch_bytes.to_string(),
            format!("{:.6}", r.field_match_pct),
            format!("{:.6}", r.field_byte_match_pct),
            format!("{:.6}", r.field_match_vs_majority_pct),
            format!("{:.6}", r.field_balanced_accuracy_pct),
            format!("{:.6}", r.field_macro_f1_pct),
            r.field_pred_dominant_label.to_string(),
            format!("{:.6}", r.field_pred_dominant_share_pct),
            if r.field_pred_collapse_90_flag { "1".into() } else { "0".into() },
            format!("{:.6}", r.chunk_mean_balanced_accuracy_pct),
            format!("{:.6}", r.chunk_mean_macro_f1_pct),
            format!("{:.6}", r.chunk_mean_abs_hist_l1_pct),
            r.chunk_majority_flip_count.to_string(),
            r.chunk_collapse_90_count.to_string(),
            r.target_hist[0].to_string(),
            r.target_hist[1].to_string(),
            r.target_hist[2].to_string(),
            r.target_hist[3].to_string(),
            r.field_pred_hist[0].to_string(),
            r.field_pred_hist[1].to_string(),
            r.field_pred_hist[2].to_string(),
            r.field_pred_hist[3].to_string(),
        ];
        out.push_str(&cols.join(","));
        out.push('\n');
    }
    out
}

fn push_kv(out: &mut String, key: &str, value: impl std::fmt::Display) {
    out.push_str(key);
    out.push('=');
    out.push_str(&value.to_string());
    out.push('\n');
}

fn format6(v: f64) -> String {
    format!("{v:.6}")
}

fn scale_pct(v: f64) -> i64 {
    (v * 1_000_000.0) as i64
}

fn pct(num: u64, den: u64) -> f64 {
    if den == 0 {
        0.0
    } else {
        (num as f64) * 100.0 / (den as f64)
    }
}

fn byte_match_pct(byte_matches: u64, byte_total: u64) -> f64 {
    pct(byte_matches, byte_total)
}

fn ppm(num: u64, den: u64) -> u64 {
    if den == 0 {
        0
    } else {
        num.saturating_mul(1_000_000) / den
    }
}

fn f1_pct(precision_pct: f64, recall_pct: f64) -> f64 {
    if precision_pct <= 0.0 || recall_pct <= 0.0 {
        0.0
    } else {
        2.0 * precision_pct * recall_pct / (precision_pct + recall_pct)
    }
}

fn argmax_hist<const N: usize>(hist: &[u64; N]) -> (u8, u64) {
    let mut best_idx = 0usize;
    let mut best_val = 0u64;
    for (idx, &val) in hist.iter().enumerate() {
        if val > best_val {
            best_idx = idx;
            best_val = val;
        }
    }
    (best_idx as u8, best_val)
}

fn entropy_bits<const N: usize>(hist: &[u64; N], total: u64) -> f64 {
    if total == 0 {
        return 0.0;
    }
    let total_f = total as f64;
    hist.iter()
        .filter(|&&count| count > 0)
        .map(|&count| {
            let p = (count as f64) / total_f;
            -p * p.log2()
        })
        .sum()
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

fn parse_chunk_values(chunk_sweep: Option<&str>, chunk_bytes: usize) -> Result<Vec<usize>> {
    let mut values = parse_usize_sweep_values(chunk_sweep, chunk_bytes, "chunk_bytes")?;
    values.sort_unstable();
    values.dedup();
    Ok(values)
}

fn parse_usize_sweep_values(raw: Option<&str>, fallback: usize, label: &str) -> Result<Vec<usize>> {
    match raw {
        None => Ok(vec![fallback]),
        Some(s) => {
            let mut out = Vec::new();
            for part in s.split(',') {
                let trimmed = part.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let value = trimmed
                    .parse::<usize>()
                    .with_context(|| format!("parse {} value '{}'", label, trimmed))?;
                if value == 0 {
                    return Err(anyhow!("{} must be >= 1", label));
                }
                out.push(value);
            }
            if out.is_empty() {
                Err(anyhow!("{} sweep produced no values", label))
            } else {
                Ok(out)
            }
        }
    }
}

fn parse_u64_sweep_values(raw: Option<&str>, fallback: u64, label: &str) -> Result<Vec<u64>> {
    match raw {
        None => Ok(vec![fallback]),
        Some(s) => {
            let mut out = Vec::new();
            for part in s.split(',') {
                let trimmed = part.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let value = trimmed
                    .parse::<u64>()
                    .with_context(|| format!("parse {} value '{}'", label, trimmed))?;
                out.push(value);
            }
            if out.is_empty() {
                Err(anyhow!("{} sweep produced no values", label))
            } else {
                Ok(out)
            }
        }
    }
}
