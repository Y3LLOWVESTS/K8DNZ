use anyhow::{anyhow, Context, Result};
use k8dnz_apextrace::{analyze_key_against_bytes, brute_force_best, render_subtree_stats, SearchCfg};

use crate::cmd::apextrace::{RenderFormat, StatsArgs, WindowScanArgs};

use super::common::{
    match_pct, match_pct_f64, percent_from_ppm, pick_hot_node, pick_hot_node_min,
    render_stats_csv, render_stats_txt, signed_percent_from_ppm, write_or_print,
};
use super::key_ops::resolve_key_for_target;

pub fn run_stats(args: StatsArgs) -> Result<()> {
    let target = std::fs::read(&args.r#in).with_context(|| format!("read {}", args.r#in))?;
    let key = resolve_key_for_target(
        args.atk.as_deref(),
        &target,
        args.seed_from,
        args.seed_count,
        args.seed_step,
        args.recipe_seed,
        args.out_key.as_deref(),
        "stats",
    )?;
    let body = match args.format {
        RenderFormat::Csv => render_stats_csv(&key, &target, args.max_quats, args.active_only)?,
        RenderFormat::Txt => render_stats_txt(&key, &target, args.max_quats, args.active_only)?,
    };
    write_or_print(args.out.as_deref(), &body)?;
    if let Some(path) = args.out.as_deref() {
        eprintln!("apextrace stats ok: out={} format={:?}", path, args.format);
    }
    Ok(())
}

pub fn run_window_scan(args: WindowScanArgs) -> Result<()> {
    if args.window_bytes == 0 {
        return Err(anyhow!("window-scan requires --window-bytes >= 1"));
    }
    if args.step_bytes == 0 {
        return Err(anyhow!("window-scan requires --step-bytes >= 1"));
    }
    let bytes = std::fs::read(&args.r#in).with_context(|| format!("read {}", args.r#in))?;
    if bytes.len() < args.window_bytes {
        return Err(anyhow!(
            "window-scan input is smaller than one window: bytes={} window_bytes={}",
            bytes.len(),
            args.window_bytes
        ));
    }
    let cfg = SearchCfg {
        seed_from: args.seed_from,
        seed_count: args.seed_count,
        seed_step: args.seed_step,
        recipe_seed: args.recipe_seed,
    };

    let mut rows = Vec::new();
    let mut start = 0usize;
    let mut window_idx = 0usize;
    while start + args.window_bytes <= bytes.len() {
        if let Some(max_windows) = args.max_windows {
            if rows.len() >= max_windows {
                break;
            }
        }
        let end = start + args.window_bytes;
        let window = &bytes[start..end];
        let best = brute_force_best(window, cfg)?;
        let diag = analyze_key_against_bytes(&best.key, window)?;
        let stats = render_subtree_stats(&best.key, window, None)?;
        let hot = pick_hot_node(&stats);
        let hot_sig8 = pick_hot_node_min(&stats, 8);
        let hot_sig16 = pick_hot_node_min(&stats, 16);
        rows.push(WindowScanRow {
            window_idx,
            byte_start: start,
            byte_end: end,
            byte_len: args.window_bytes,
            quat_len: best.key.quat_len,
            best_root_quadrant: best.key.root_quadrant,
            best_root_seed: best.key.root_seed,
            recipe_seed: best.key.recipe_seed,
            matches: diag.score.matches,
            prefix: diag.score.prefix,
            total: diag.score.total,
            byte_matches: diag.byte_matches,
            longest_run: diag.longest_run,
            longest_run_start: diag.longest_run_start,
            target_hist: diag.target_hist,
            pred_hist: diag.pred_hist,
            hot_row: hot.map(|s| s.row).unwrap_or(0),
            hot_k: hot.map(|s| s.k).unwrap_or(0),
            hot_subtree_size: hot.map(|s| s.subtree_size).unwrap_or(0),
            hot_matches: hot.map(|s| s.matches).unwrap_or(0),
            hot_match_rate_ppm: hot.map(|s| s.match_rate_ppm()).unwrap_or(0),
            hot_match_excess_ppm: hot.map(|s| s.match_excess_ppm()).unwrap_or(0),
            hot_sig8_row: hot_sig8.map(|s| s.row).unwrap_or(0),
            hot_sig8_k: hot_sig8.map(|s| s.k).unwrap_or(0),
            hot_sig8_subtree_size: hot_sig8.map(|s| s.subtree_size).unwrap_or(0),
            hot_sig8_matches: hot_sig8.map(|s| s.matches).unwrap_or(0),
            hot_sig8_match_rate_ppm: hot_sig8.map(|s| s.match_rate_ppm()).unwrap_or(0),
            hot_sig8_match_excess_ppm: hot_sig8.map(|s| s.match_excess_ppm()).unwrap_or(0),
            hot_sig16_row: hot_sig16.map(|s| s.row).unwrap_or(0),
            hot_sig16_k: hot_sig16.map(|s| s.k).unwrap_or(0),
            hot_sig16_subtree_size: hot_sig16.map(|s| s.subtree_size).unwrap_or(0),
            hot_sig16_matches: hot_sig16.map(|s| s.matches).unwrap_or(0),
            hot_sig16_match_rate_ppm: hot_sig16.map(|s| s.match_rate_ppm()).unwrap_or(0),
            hot_sig16_match_excess_ppm: hot_sig16.map(|s| s.match_excess_ppm()).unwrap_or(0),
        });
        start = start.saturating_add(args.step_bytes);
        window_idx = window_idx.saturating_add(1);
    }

    let body = match args.format {
        RenderFormat::Csv => window_scan_csv(&rows),
        RenderFormat::Txt => window_scan_txt(&rows),
    };
    write_or_print(args.out.as_deref(), &body)?;

    let summary = summarize_window_scan(&rows);
    if let Some(best) = rows.iter().max_by_key(|row| (row.matches, row.longest_run, row.byte_matches)) {
        eprintln!(
            "apextrace window-scan ok: windows={} best_idx={} byte_range={}..{} matches={} total={} match_pct={:.6} longest_run={} hot_node=({}, {}) hot_match_excess_vs_random={:.6} hot_sig8_node=({}, {}) hot_sig8_subtree_size={} hot_sig8_match_excess_vs_random={:.6} hot_sig16_node=({}, {}) hot_sig16_subtree_size={} hot_sig16_match_excess_vs_random={:.6}",
            summary.windows,
            best.window_idx,
            best.byte_start,
            best.byte_end,
            best.matches,
            best.total,
            match_pct(best.matches, best.total),
            best.longest_run,
            best.hot_row,
            best.hot_k,
            signed_percent_from_ppm(best.hot_match_excess_ppm),
            best.hot_sig8_row,
            best.hot_sig8_k,
            best.hot_sig8_subtree_size,
            signed_percent_from_ppm(best.hot_sig8_match_excess_ppm),
            best.hot_sig16_row,
            best.hot_sig16_k,
            best.hot_sig16_subtree_size,
            signed_percent_from_ppm(best.hot_sig16_match_excess_ppm),
        );
        eprintln!(
            "apextrace window-scan summary: match_pct min={:.6} mean={:.6} p50={:.6} p90={:.6} p99={:.6} max={:.6}",
            match_pct(summary.min_matches, best.total),
            match_pct_f64(summary.mean_matches, best.total),
            match_pct(summary.p50_matches, best.total),
            match_pct(summary.p90_matches, best.total),
            match_pct(summary.p99_matches, best.total),
            match_pct(summary.max_matches, best.total),
        );
        eprintln!(
            "apextrace window-scan summary: longest_run_max={} windows_ge_50pct={} windows_ge_52pct={} windows_ge_54pct={}",
            summary.longest_run_max,
            summary.windows_ge_50pct,
            summary.windows_ge_52pct,
            summary.windows_ge_54pct,
        );
        if let Some(best_sig8) = rows
            .iter()
            .filter(|row| row.hot_sig8_subtree_size >= 8)
            .max_by_key(|row| (row.matches, row.hot_sig8_match_excess_ppm, row.longest_run, row.byte_matches))
        {
            eprintln!(
                "apextrace window-scan summary: best_sig8_idx={} byte_range={}..{} matches={} match_pct={:.6} sig8_node=({}, {}) sig8_subtree_size={} sig8_match_rate={:.6} sig8_match_excess_vs_random={:.6}",
                best_sig8.window_idx,
                best_sig8.byte_start,
                best_sig8.byte_end,
                best_sig8.matches,
                match_pct(best_sig8.matches, best_sig8.total),
                best_sig8.hot_sig8_row,
                best_sig8.hot_sig8_k,
                best_sig8.hot_sig8_subtree_size,
                percent_from_ppm(best_sig8.hot_sig8_match_rate_ppm),
                signed_percent_from_ppm(best_sig8.hot_sig8_match_excess_ppm),
            );
        }
        if let Some(best_sig16) = rows
            .iter()
            .filter(|row| row.hot_sig16_subtree_size >= 16)
            .max_by_key(|row| (row.matches, row.hot_sig16_match_excess_ppm, row.longest_run, row.byte_matches))
        {
            eprintln!(
                "apextrace window-scan summary: best_sig16_idx={} byte_range={}..{} matches={} match_pct={:.6} sig16_node=({}, {}) sig16_subtree_size={} sig16_match_rate={:.6} sig16_match_excess_vs_random={:.6}",
                best_sig16.window_idx,
                best_sig16.byte_start,
                best_sig16.byte_end,
                best_sig16.matches,
                match_pct(best_sig16.matches, best_sig16.total),
                best_sig16.hot_sig16_row,
                best_sig16.hot_sig16_k,
                best_sig16.hot_sig16_subtree_size,
                percent_from_ppm(best_sig16.hot_sig16_match_rate_ppm),
                signed_percent_from_ppm(best_sig16.hot_sig16_match_excess_ppm),
            );
        }
    } else {
        eprintln!("apextrace window-scan ok: windows=0");
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct WindowScanRow {
    window_idx: usize,
    byte_start: usize,
    byte_end: usize,
    byte_len: usize,
    quat_len: u64,
    best_root_quadrant: u8,
    best_root_seed: u64,
    recipe_seed: u64,
    matches: u64,
    prefix: u64,
    total: u64,
    byte_matches: u64,
    longest_run: u64,
    longest_run_start: u64,
    target_hist: [u64; 4],
    pred_hist: [u64; 4],
    hot_row: u16,
    hot_k: u16,
    hot_subtree_size: u64,
    hot_matches: u64,
    hot_match_rate_ppm: u64,
    hot_match_excess_ppm: i64,
    hot_sig8_row: u16,
    hot_sig8_k: u16,
    hot_sig8_subtree_size: u64,
    hot_sig8_matches: u64,
    hot_sig8_match_rate_ppm: u64,
    hot_sig8_match_excess_ppm: i64,
    hot_sig16_row: u16,
    hot_sig16_k: u16,
    hot_sig16_subtree_size: u64,
    hot_sig16_matches: u64,
    hot_sig16_match_rate_ppm: u64,
    hot_sig16_match_excess_ppm: i64,
}

#[derive(Clone, Debug)]
struct WindowScanSummary {
    windows: usize,
    min_matches: u64,
    max_matches: u64,
    mean_matches: f64,
    p50_matches: u64,
    p90_matches: u64,
    p99_matches: u64,
    longest_run_max: u64,
    windows_ge_50pct: usize,
    windows_ge_52pct: usize,
    windows_ge_54pct: usize,
}

fn window_scan_csv(rows: &[WindowScanRow]) -> String {
    let mut out = String::from(
        "window_idx,byte_start,byte_end,byte_len,quat_len,best_root_quadrant,best_root_seed_hex,recipe_seed_hex,matches,prefix,total,match_pct,byte_matches,longest_run,longest_run_start,target_hist_1,target_hist_2,target_hist_3,target_hist_4,pred_hist_1,pred_hist_2,pred_hist_3,pred_hist_4,hot_row,hot_k,hot_subtree_size,hot_matches,hot_match_rate,hot_match_excess_vs_random,hot_sig8_row,hot_sig8_k,hot_sig8_subtree_size,hot_sig8_matches,hot_sig8_match_rate,hot_sig8_match_excess_vs_random,hot_sig16_row,hot_sig16_k,hot_sig16_subtree_size,hot_sig16_matches,hot_sig16_match_rate,hot_sig16_match_excess_vs_random\n",
    );
    for row in rows {
        out.push_str(&format!(
            "{},{},{},{},{},{},0x{:016X},0x{:016X},{},{},{},{:.6},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{:.6},{:.6},{},{},{},{},{:.6},{:.6},{},{},{},{},{:.6},{:.6}\n",
            row.window_idx,
            row.byte_start,
            row.byte_end,
            row.byte_len,
            row.quat_len,
            row.best_root_quadrant,
            row.best_root_seed,
            row.recipe_seed,
            row.matches,
            row.prefix,
            row.total,
            match_pct(row.matches, row.total),
            row.byte_matches,
            row.longest_run,
            row.longest_run_start,
            row.target_hist[0], row.target_hist[1], row.target_hist[2], row.target_hist[3],
            row.pred_hist[0], row.pred_hist[1], row.pred_hist[2], row.pred_hist[3],
            row.hot_row, row.hot_k, row.hot_subtree_size, row.hot_matches,
            percent_from_ppm(row.hot_match_rate_ppm),
            signed_percent_from_ppm(row.hot_match_excess_ppm),
            row.hot_sig8_row, row.hot_sig8_k, row.hot_sig8_subtree_size, row.hot_sig8_matches,
            percent_from_ppm(row.hot_sig8_match_rate_ppm),
            signed_percent_from_ppm(row.hot_sig8_match_excess_ppm),
            row.hot_sig16_row, row.hot_sig16_k, row.hot_sig16_subtree_size, row.hot_sig16_matches,
            percent_from_ppm(row.hot_sig16_match_rate_ppm),
            signed_percent_from_ppm(row.hot_sig16_match_excess_ppm),
        ));
    }
    out
}

fn window_scan_txt(rows: &[WindowScanRow]) -> String {
    let mut out = String::new();
    for row in rows {
        out.push_str(&format!(
            "window_idx={} byte_start={} byte_end={} byte_len={} quat_len={} best_root_quadrant={} best_root_seed=0x{:016X} recipe_seed=0x{:016X} matches={} prefix={} total={} match_pct={:.6} byte_matches={} longest_run={} longest_run_start={} target_hist=[{},{},{},{}] pred_hist=[{},{},{},{}] hot_row={} hot_k={} hot_subtree_size={} hot_matches={} hot_match_rate={:.6} hot_match_excess_vs_random={:.6} hot_sig8_row={} hot_sig8_k={} hot_sig8_subtree_size={} hot_sig8_matches={} hot_sig8_match_rate={:.6} hot_sig8_match_excess_vs_random={:.6} hot_sig16_row={} hot_sig16_k={} hot_sig16_subtree_size={} hot_sig16_matches={} hot_sig16_match_rate={:.6} hot_sig16_match_excess_vs_random={:.6}\n",
            row.window_idx,
            row.byte_start,
            row.byte_end,
            row.byte_len,
            row.quat_len,
            row.best_root_quadrant,
            row.best_root_seed,
            row.recipe_seed,
            row.matches,
            row.prefix,
            row.total,
            match_pct(row.matches, row.total),
            row.byte_matches,
            row.longest_run,
            row.longest_run_start,
            row.target_hist[0], row.target_hist[1], row.target_hist[2], row.target_hist[3],
            row.pred_hist[0], row.pred_hist[1], row.pred_hist[2], row.pred_hist[3],
            row.hot_row, row.hot_k, row.hot_subtree_size, row.hot_matches,
            percent_from_ppm(row.hot_match_rate_ppm),
            signed_percent_from_ppm(row.hot_match_excess_ppm),
            row.hot_sig8_row, row.hot_sig8_k, row.hot_sig8_subtree_size, row.hot_sig8_matches,
            percent_from_ppm(row.hot_sig8_match_rate_ppm),
            signed_percent_from_ppm(row.hot_sig8_match_excess_ppm),
            row.hot_sig16_row, row.hot_sig16_k, row.hot_sig16_subtree_size, row.hot_sig16_matches,
            percent_from_ppm(row.hot_sig16_match_rate_ppm),
            signed_percent_from_ppm(row.hot_sig16_match_excess_ppm),
        ));
    }
    out
}

fn summarize_window_scan(rows: &[WindowScanRow]) -> WindowScanSummary {
    let mut matches: Vec<u64> = rows.iter().map(|row| row.matches).collect();
    matches.sort_unstable();
    let mean_matches = if matches.is_empty() {
        0.0
    } else {
        let sum: u64 = matches.iter().copied().sum();
        (sum as f64) / (matches.len() as f64)
    };
    WindowScanSummary {
        windows: rows.len(),
        min_matches: matches.first().copied().unwrap_or(0),
        max_matches: matches.last().copied().unwrap_or(0),
        mean_matches,
        p50_matches: percentile_u64(&matches, 50),
        p90_matches: percentile_u64(&matches, 90),
        p99_matches: percentile_u64(&matches, 99),
        longest_run_max: rows.iter().map(|row| row.longest_run).max().unwrap_or(0),
        windows_ge_50pct: rows.iter().filter(|row| row.total > 0 && row.matches * 10000 >= row.total * 5000).count(),
        windows_ge_52pct: rows.iter().filter(|row| row.total > 0 && row.matches * 10000 >= row.total * 5200).count(),
        windows_ge_54pct: rows.iter().filter(|row| row.total > 0 && row.matches * 10000 >= row.total * 5400).count(),
    }
}

fn percentile_u64(sorted: &[u64], pct: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = ((sorted.len() - 1) * pct) / 100;
    sorted[rank]
}
