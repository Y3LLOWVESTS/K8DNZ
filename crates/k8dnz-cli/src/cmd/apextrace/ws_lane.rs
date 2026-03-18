use anyhow::{anyhow, Context, Result};
use k8dnz_apextrace::{generate_bytes, ApexKey, SearchCfg};
use k8dnz_core::lane;
use k8dnz_core::repr::{text_norm, ws_lanes::WsLanes};
use k8dnz_core::symbol::patch::PatchList;
use crate::cmd::apextrace::{RenderFormat, WsLaneArgs};
use crate::io::recipe_file;
use super::common::{decode_k8l1_view_any, match_pct, patch_count, write_or_print};
use super::ws_lane_render::{
    render_ws_class_ascii, render_ws_lane_chunk_keys_csv, render_ws_lane_csv, render_ws_lane_sweep_csv,
    render_ws_lane_sweep_txt, render_ws_lane_txt,
};
use super::ws_lane_types::{
    build_sweep_row, unique_counts, ChunkSnapshot, WsLaneBest, WsLaneChunkBest, WsLaneChunkReport,
    WsLaneChunkedBest, WsLaneDiagnostics, WsLaneReport, WsLaneScore,
};
pub fn run_ws_lane(args: WsLaneArgs) -> Result<()> {
    let input = std::fs::read(&args.r#in).with_context(|| format!("read {}", args.r#in))?;
    let recipe_bytes = recipe_file::load_k8r_bytes(&args.recipe)
        .with_context(|| format!("load recipe {}", args.recipe))?;
    let norm = text_norm::normalize_newlines(&input);
    let ws = WsLanes::split(&norm);
    let (artifact, baseline_stats, baseline_ticks_used) = run_baseline_k8l1(&input, &recipe_bytes, args.max_ticks)?;
    let view = decode_k8l1_view_any(&artifact)?;
    let baseline_class_patch_entries = patch_count(&view.class_patch)?;
    let baseline_class_patch_bytes = view.class_patch.len();
    let cfg = SearchCfg {
        seed_from: args.seed_from,
        seed_count: args.seed_count,
        seed_step: args.seed_step,
        recipe_seed: args.recipe_seed,
    };
    let best = brute_force_best_ws_lane(&ws.class_lane, cfg)?;
    let apex_patch = PatchList::from_pred_actual(&best.predicted, &ws.class_lane)
        .map_err(|e| anyhow!("apex ws patch build failed: {e}"))?;
    let apex_patch_bytes = apex_patch.encode();
    if let Some(spec) = args.chunk_sweep.as_deref() {
        let sizes = parse_chunk_sweep(spec, args.chunk_bytes)?;
        let mut rows = Vec::new();
        for size in sizes {
            let chunked = brute_force_best_ws_lane_chunked(&ws.class_lane, cfg, size)?;
            let snapshot = build_chunk_snapshot(&chunked, &ws.class_lane)?;
            rows.push(build_sweep_row(&snapshot, baseline_class_patch_bytes, apex_patch_bytes.len()));
        }
        let body = match args.format {
            RenderFormat::Csv => render_ws_lane_sweep_csv(&rows),
            RenderFormat::Txt => render_ws_lane_sweep_txt(&rows),
        };
        write_or_print(args.out.as_deref(), &body)?;
        print_ws_lane_sweep_summary(args.out.as_deref(), args.format, baseline_class_patch_bytes, apex_patch_bytes.len(), &rows);
        return Ok(());
    }
    let chunked = match args.chunk_bytes {
        Some(chunk_bytes) => Some(brute_force_best_ws_lane_chunked(&ws.class_lane, cfg, chunk_bytes)?),
        None => None,
    };
    let chunk_snapshot = chunked.as_ref().map(|c| build_chunk_snapshot(c, &ws.class_lane)).transpose()?;
    save_ws_lane_outputs(args.out_key.as_deref(), args.out_pred.as_deref(), &best, chunked.as_ref())?;
    let row = WsLaneReport::from_parts(
        &args,
        norm.len(),
        ws.class_lane.len(),
        ws.other_lane.len(),
        artifact.len(),
        baseline_ticks_used,
        baseline_stats.class_mismatches,
        baseline_class_patch_entries,
        baseline_class_patch_bytes,
        &best,
        apex_patch.entries.len(),
        apex_patch_bytes.len(),
        chunk_snapshot,
    );
    let body = match args.format {
        RenderFormat::Csv => render_ws_lane_csv(&row),
        RenderFormat::Txt => render_ws_lane_txt(&row),
    };
    write_or_print(args.out.as_deref(), &body)?;
    print_ws_lane_summary(args.out.as_deref(), args.format, &row);
    Ok(())
}
fn run_baseline_k8l1(input: &[u8], recipe_bytes: &[u8], max_ticks: u64) -> Result<(Vec<u8>, lane::LaneEncodeStats, u64)> {
    let mut baseline_ticks_used = max_ticks.max(1);
    let baseline_ticks_cap = baseline_ticks_used.saturating_mul(8).max(160_000_000).min(1_280_000_000);
    let out = loop {
        match lane::encode_k8l1(input, recipe_bytes, baseline_ticks_used) {
            Ok(ok) => break ok,
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("insufficient emissions") && baseline_ticks_used < baseline_ticks_cap {
                    let next = baseline_ticks_used.saturating_mul(2).min(baseline_ticks_cap);
                    if next == baseline_ticks_used {
                        return Err(anyhow!("baseline k8l1 encode failed: {e}"));
                    }
                    eprintln!(
                        "apextrace ws-lane baseline retry: max_ticks={} failed with insufficient emissions; retrying with max_ticks={}",
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
        eprintln!("apextrace ws-lane baseline auto-ticks resolved: used max_ticks={}", baseline_ticks_used);
    }
    Ok((out.0, out.1, baseline_ticks_used))
}
fn save_ws_lane_outputs(out_key: Option<&str>, out_pred: Option<&str>, best: &WsLaneBest, chunked: Option<&WsLaneChunkedBest>) -> Result<()> {
    if let Some(path) = out_key {
        let enc = best.key.encode()?;
        std::fs::write(path, enc).with_context(|| format!("write {}", path))?;
        eprintln!("saved apex ws-lane key: {}", path);
        if let Some(chunked_best) = chunked {
            let manifest = render_ws_lane_chunk_keys_csv(chunked_best.chunk_bytes, &chunked_best.chunks);
            let chunk_path = format!("{}.chunks.csv", path);
            std::fs::write(&chunk_path, manifest.as_bytes()).with_context(|| format!("write {}", chunk_path))?;
            eprintln!("saved apex ws-lane chunk manifest: {}", chunk_path);
        }
    }
    if let Some(path) = out_pred {
        let ascii = render_ws_class_ascii(&best.predicted);
        std::fs::write(path, ascii.as_bytes()).with_context(|| format!("write {}", path))?;
        eprintln!("saved apex ws-lane predicted class lane: {}", path);
        if let Some(chunked_best) = chunked {
            let ascii = render_ws_class_ascii(&chunked_best.predicted);
            let chunk_path = format!("{}.chunked.txt", path);
            std::fs::write(&chunk_path, ascii.as_bytes()).with_context(|| format!("write {}", chunk_path))?;
            eprintln!("saved apex ws-lane chunked predicted class lane: {}", chunk_path);
        }
    }
    Ok(())
}
fn print_ws_lane_summary(out: Option<&str>, format: RenderFormat, row: &WsLaneReport) {
    if let Some(path) = out {
        eprintln!("apextrace ws-lane ok: out={} format={:?}", path, format);
        return;
    }
    if let Some(chunk_total_payload_exact) = row.chunk_total_payload_exact {
        eprintln!(
            "apextrace ws-lane ok: baseline_class_patch_bytes={} apex_global_patch_bytes={} apex_global_total_payload_exact={} apex_chunked_patch_bytes={} apex_chunked_total_payload_exact={} delta_global_patch_vs_baseline={} delta_global_total_vs_baseline={} delta_chunked_patch_vs_baseline={} delta_chunked_total_vs_baseline={} delta_chunked_patch_vs_global={} delta_chunked_total_vs_global={} baseline_class_mismatches={} apex_global_patch_entries={} apex_chunked_patch_entries={} global_match_pct={:.6} chunked_match_pct={:.6} baseline_max_ticks_used={}",
            row.baseline_class_patch_bytes,
            row.apex_patch_bytes,
            row.apex_total_payload_exact,
            row.chunk_patch_bytes.unwrap_or(0),
            chunk_total_payload_exact,
            row.delta_patch_bytes,
            row.delta_total_payload_exact_vs_baseline,
            row.chunk_delta_patch_bytes_vs_baseline.unwrap_or(0),
            row.chunk_delta_total_payload_exact_vs_baseline.unwrap_or(0),
            row.chunk_delta_patch_bytes_vs_global.unwrap_or(0),
            row.chunk_delta_total_payload_exact_vs_global.unwrap_or(0),
            row.baseline_class_mismatches,
            row.apex_patch_entries,
            row.chunk_patch_entries.unwrap_or(0),
            row.apex_match_pct,
            row.chunk_match_pct.unwrap_or(0.0),
            row.baseline_max_ticks_used,
        );
    } else {
        eprintln!(
            "apextrace ws-lane ok: baseline_class_patch_bytes={} apex_patch_bytes={} apex_total_payload_exact={} delta_patch_vs_baseline={} delta_total_vs_baseline={} baseline_class_mismatches={} apex_patch_entries={} delta_patch_entries={} match_pct={:.6} baseline_max_ticks_used={}",
            row.baseline_class_patch_bytes,
            row.apex_patch_bytes,
            row.apex_total_payload_exact,
            row.delta_patch_bytes,
            row.delta_total_payload_exact_vs_baseline,
            row.baseline_class_mismatches,
            row.apex_patch_entries,
            row.delta_patch_entries,
            row.apex_match_pct,
            row.baseline_max_ticks_used,
        );
    }
}
fn print_ws_lane_sweep_summary(out: Option<&str>, format: RenderFormat, baseline_class_patch_bytes: usize, apex_patch_bytes: usize, rows: &[super::ws_lane_types::WsLaneSweepRow]) {
    if let Some(path) = out {
        eprintln!("apextrace ws-lane sweep ok: out={} format={:?}", path, format);
        return;
    }
    if let Some(best_patch) = rows.iter().min_by_key(|row| row.patch_bytes) {
        eprintln!(
            "apextrace ws-lane sweep best-patch: chunk_bytes={} patch_bytes={} total_payload_exact={} delta_patch_vs_baseline={} delta_total_vs_baseline={} unique_keys={} unique_seeds={} match_pct={:.6}",
            best_patch.chunk_bytes,
            best_patch.patch_bytes,
            best_patch.total_payload_exact,
            best_patch.delta_patch_vs_baseline,
            best_patch.delta_total_vs_baseline,
            best_patch.unique_key_count,
            best_patch.unique_seed_count,
            best_patch.match_pct,
        );
    }
    if let Some(best_total) = rows.iter().min_by_key(|row| row.total_payload_exact) {
        eprintln!(
            "apextrace ws-lane sweep best-total: baseline_class_patch_bytes={} apex_global_patch_bytes={} apex_global_total_payload_exact={} chunk_bytes={} patch_bytes={} total_payload_exact={} delta_patch_vs_baseline={} delta_total_vs_baseline={} delta_total_vs_global={} unique_keys={} unique_seeds={} match_pct={:.6}",
            baseline_class_patch_bytes,
            apex_patch_bytes,
            apex_patch_bytes + 48,
            best_total.chunk_bytes,
            best_total.patch_bytes,
            best_total.total_payload_exact,
            best_total.delta_patch_vs_baseline,
            best_total.delta_total_vs_baseline,
            best_total.delta_total_vs_global,
            best_total.unique_key_count,
            best_total.unique_seed_count,
            best_total.match_pct,
        );
    }
}
fn build_chunk_snapshot(chunked_best: &WsLaneChunkedBest, target: &[u8]) -> Result<ChunkSnapshot> {
    let patch = PatchList::from_pred_actual(&chunked_best.predicted, target)
        .map_err(|e| anyhow!("apex ws chunked patch build failed: {e}"))?;
    let patch_bytes = patch.encode();
    let chunk_reports = chunked_best.chunks.iter().map(|chunk| WsLaneChunkReport {
        chunk_index: chunk.chunk_index,
        start: chunk.start,
        end: chunk.end,
        len: chunk.end.saturating_sub(chunk.start),
        root_quadrant: chunk.key.root_quadrant,
        root_seed: chunk.key.root_seed,
        recipe_seed: chunk.key.recipe_seed,
        matches: chunk.diag.score.matches,
        prefix: chunk.diag.score.prefix,
        total: chunk.diag.score.total,
        match_pct: match_pct(chunk.diag.score.matches, chunk.diag.score.total),
        longest_run: chunk.diag.score.longest_run,
        longest_run_start: chunk.diag.score.longest_run_start,
        patch_entries: chunk.patch_entries,
        patch_bytes: chunk.patch_bytes,
    }).collect::<Vec<_>>();
    let (unique_key_count, unique_seed_count) = unique_counts(&chunked_best.chunks);
    Ok(ChunkSnapshot {
        chunk_bytes: chunked_best.chunk_bytes,
        chunk_key_bytes_exact: chunked_best.chunk_key_bytes_exact,
        patch_entries: patch.entries.len(),
        patch_bytes: patch_bytes.len(),
        total_payload_exact: patch_bytes.len().saturating_add(chunked_best.chunk_key_bytes_exact),
        diag: chunked_best.diag.clone(),
        unique_key_count,
        unique_seed_count,
        chunk_reports,
    })
}
fn brute_force_best_ws_lane(target: &[u8], cfg: SearchCfg) -> Result<WsLaneBest> {
    if cfg.seed_step == 0 {
        return Err(anyhow!("ws-lane: seed_step must be >= 1"));
    }
    let byte_len = target.len() as u64;
    let mut best: Option<WsLaneBest> = None;
    for quadrant in 0u8..=3 {
        let mut i = 0u64;
        while i < cfg.seed_count {
            let seed = cfg.seed_from.saturating_add(i.saturating_mul(cfg.seed_step));
            let key = ApexKey::new_dibit_v1(byte_len, quadrant, seed, cfg.recipe_seed)?;
            let predicted = generate_bytes(&key)?.into_iter().map(|b| bucket_u8_local(b, 3)).collect::<Vec<_>>();
            let diag = score_ws_lane_symbols(target, &predicted)?;
            let cand = WsLaneBest { key, predicted, diag };
            match &best {
                None => best = Some(cand),
                Some(cur) if cand.diag.score.better_than(&cur.diag.score) => best = Some(cand),
                Some(_) => {}
            }
            i = i.saturating_add(1);
        }
    }
    best.ok_or_else(|| anyhow!("ws-lane: search produced no candidates"))
}
fn brute_force_best_ws_lane_chunked(target: &[u8], cfg: SearchCfg, chunk_bytes: usize) -> Result<WsLaneChunkedBest> {
    if chunk_bytes == 0 {
        return Err(anyhow!("ws-lane: chunk_bytes must be >= 1"));
    }
    let mut predicted = Vec::with_capacity(target.len());
    let mut chunks = Vec::new();
    let mut start = 0usize;
    let mut chunk_index = 0usize;
    while start < target.len() {
        let end = start.saturating_add(chunk_bytes).min(target.len());
        let slice = &target[start..end];
        let best = brute_force_best_ws_lane(slice, cfg)?;
        let patch = PatchList::from_pred_actual(&best.predicted, slice)
            .map_err(|e| anyhow!("ws-lane chunk patch build failed: {e}"))?;
        let patch_bytes = patch.encode();
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
    Ok(WsLaneChunkedBest {
        chunk_bytes,
        chunk_key_bytes_exact: chunks.len().saturating_mul(48),
        predicted,
        diag,
        chunks,
    })
}
fn score_ws_lane_symbols(target: &[u8], predicted: &[u8]) -> Result<WsLaneDiagnostics> {
    if target.len() != predicted.len() {
        return Err(anyhow!("ws-lane: target len {} != predicted len {}", target.len(), predicted.len()));
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
        score: WsLaneScore { matches, prefix, total: target.len() as u64, longest_run, longest_run_start },
        target_hist,
        pred_hist,
    })
}
fn parse_chunk_sweep(spec: &str, extra: Option<usize>) -> Result<Vec<usize>> {
    let mut out = Vec::new();
    for raw in spec.split(',') {
        let s = raw.trim();
        if s.is_empty() {
            continue;
        }
        let v: usize = s.parse().map_err(|_| anyhow!("ws-lane: invalid chunk size '{}'", s))?;
        if v == 0 {
            return Err(anyhow!("ws-lane: chunk sizes must be >= 1"));
        }
        if !out.contains(&v) {
            out.push(v);
        }
    }
    if let Some(v) = extra {
        if v == 0 {
            return Err(anyhow!("ws-lane: chunk_bytes must be >= 1"));
        }
        if !out.contains(&v) {
            out.push(v);
        }
    }
    if out.is_empty() {
        return Err(anyhow!("ws-lane: chunk-sweep produced no sizes"));
    }
    out.sort_unstable();
    Ok(out)
}
fn ws_slot(v: u8) -> Result<usize> {
    match v {
        0..=2 => Ok(v as usize),
        _ => Err(anyhow!("ws-lane: invalid class symbol {}", v)),
    }
}
#[inline]
fn bucket_u8_local(b: u8, k: u8) -> u8 {
    ((b as u16 * k as u16) >> 8) as u8
}
