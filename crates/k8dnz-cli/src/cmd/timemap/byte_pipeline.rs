// crates/k8dnz-cli/src/cmd/timemap/byte_pipeline.rs

use super::args::*;
use super::mapping::map_byte;
use super::residual::{apply_residual_byte, make_residual_byte};
use super::tags::{apply_conditioning_if_enabled, read_cond_tags, CondTags};
use super::util::{
    parse_seed, parse_seed_hex_opt, tm_jump_cost, zstd_compress_len,
};

use k8dnz_core::signal::timing_map::TimingMap;
use k8dnz_core::Engine;

use crate::io::{recipe_file, timemap};

pub fn cmd_make(a: MakeArgs) -> anyhow::Result<()> {
    let tm = TimingMap::stride(a.len, a.start, a.step).map_err(|e| anyhow::anyhow!("{e}"))?;
    timemap::write_timemap_auto(&a.out, &tm)?;
    eprintln!(
        "timemap ok: out={} len={} start={} step={} last={:?}",
        a.out,
        tm.indices.len(),
        a.start,
        a.step,
        tm.last_index()
    );
    Ok(())
}

pub fn cmd_inspect(a: InspectArgs) -> anyhow::Result<()> {
    let tm = timemap::read_timemap(&a.r#in)?;
    eprintln!(
        "timemap: in={} len={} first={:?} last={:?}",
        a.r#in,
        tm.indices.len(),
        tm.indices.first(),
        tm.indices.last()
    );
    Ok(())
}

pub fn cmd_map_seed(a: MapSeedArgs) -> anyhow::Result<()> {
    let seed = parse_seed(&a)?;
    let seed_hex = format!("0x{seed:016x}");

    match a.map {
        MapMode::Text40Field => {
            let seed_lo = seed as u32;
            let rate = ((seed >> 32) & 0xFF) as u8;
            let tshift = ((seed >> 40) & 0xFF) as u8;
            let phase0 = ((seed >> 48) & 0xFF) as u8;
            let shift_amp = ((seed >> 56) & 0xFF) as u8;

            match a.fmt {
                SeedFmt::Text => {
                    println!("map=text40-field");
                    println!("seed_dec={seed}");
                    println!("seed_hex={seed_hex}");
                    println!("seed_lo_u32={seed_lo}");
                    println!("rate_u8={rate}");
                    println!("tshift_u8={tshift}");
                    println!("phase0_u8={phase0}");
                    println!("shift_amp_u8={shift_amp}");
                }
                SeedFmt::Json => {
                    println!(
                        "{{\"map\":\"text40-field\",\"seed_dec\":{seed},\"seed_hex\":\"{seed_hex}\",\"seed_lo_u32\":{seed_lo},\"rate_u8\":{rate},\"tshift_u8\":{tshift},\"phase0_u8\":{phase0},\"shift_amp_u8\":{shift_amp}}}"
                    );
                }
            }
        }
        _ => match a.fmt {
            SeedFmt::Text => {
                println!("map={:?}", a.map);
                println!("seed_dec={seed}");
                println!("seed_hex={seed_hex}");
                println!("note=decoder-ring is only defined for text40-field currently");
            }
            SeedFmt::Json => {
                println!(
                    "{{\"map\":\"{:?}\",\"seed_dec\":{seed},\"seed_hex\":\"{seed_hex}\",\"note\":\"decoder-ring is only defined for text40-field currently\"}}",
                    a.map
                );
            }
        },
    }

    Ok(())
}

pub fn cmd_apply(a: ApplyArgs) -> anyhow::Result<()> {
    let recipe = recipe_file::load_k8r(&a.recipe)?;
    let tm = timemap::read_timemap(&a.timemap)?;
    if tm.indices.is_empty() {
        anyhow::bail!("timemap empty");
    }

    let mut engine = Engine::new(recipe)?;

    match a.mode {
        ApplyMode::Pair => {
            let bytes = collect_pair_bytes(&mut engine, &tm, a.max_ticks)?;
            std::fs::write(&a.out, &bytes)?;
            eprintln!(
                "apply ok: out={} bytes={} ticks={} emissions={}",
                a.out,
                bytes.len(),
                engine.stats.ticks,
                engine.stats.emissions
            );
        }
        ApplyMode::Rgbpair => {
            let bytes = collect_rgbpair_bytes(&mut engine, &tm, a.max_ticks)?;
            std::fs::write(&a.out, &bytes)?;
            eprintln!(
                "apply ok: out={} bytes={} ticks={} emissions={}",
                a.out,
                bytes.len(),
                engine.stats.ticks,
                engine.stats.emissions
            );
        }
    }

    Ok(())
}

pub fn cmd_fit(a: FitArgs) -> anyhow::Result<()> {
    let recipe = recipe_file::load_k8r(&a.recipe)?;
    let target = std::fs::read(&a.target)?;
    if target.is_empty() {
        anyhow::bail!("target is empty");
    }

    let mut engine = Engine::new(recipe)?;
    let mut indices: Vec<u64> = Vec::with_capacity(target.len());

    let mut want: usize = 0;
    let want_len = target.len();
    let first_byte = target[0];

    while (engine.stats.emissions as u64) < a.start_emission && engine.stats.ticks < a.max_ticks {
        let _ = engine.step();
        if (engine.stats.emissions as u64) >= a.search_emissions {
            break;
        }
    }

    let start_ticks = engine.stats.ticks;
    let mut first_byte_seen: u64 = 0;

    while (engine.stats.emissions as u64) < a.search_emissions && engine.stats.ticks < a.max_ticks {
        if let Some(tok) = engine.step() {
            let idx = (engine.stats.emissions - 1) as u64;
            let b = tok.pack_byte();

            if b == first_byte {
                first_byte_seen += 1;
            }

            if b == target[want] {
                indices.push(idx);
                want += 1;
                if want == want_len {
                    break;
                }
            }
        }
    }

    if want != want_len {
        anyhow::bail!(
            "timemap fit failed: matched {}/{} bytes; first_target=0x{:02x} first_seen={} start_emission={} searched_emissions={} ticks={} (start_ticks={} delta_ticks={})",
            want,
            want_len,
            first_byte,
            first_byte_seen,
            a.start_emission,
            engine.stats.emissions as u64,
            engine.stats.ticks,
            start_ticks,
            engine.stats.ticks.saturating_sub(start_ticks),
        );
    }

    let tm = TimingMap { indices };
    timemap::write_timemap_auto(&a.out, &tm)?;

    eprintln!(
        "timemap fit ok: out={} target_bytes={} first_idx={:?} last_idx={:?} start_emission={} start_ticks={} end_emissions={} end_ticks={} delta_ticks={}",
        a.out,
        want_len,
        tm.indices.first(),
        tm.indices.last(),
        a.start_emission,
        start_ticks,
        engine.stats.emissions,
        engine.stats.ticks,
        engine.stats.ticks.saturating_sub(start_ticks),
    );

    Ok(())
}

pub fn cmd_fit_xor(a: FitXorArgs) -> anyhow::Result<()> {
    let recipe = recipe_file::load_k8r(&a.recipe)?;
    let recipe_raw_len = std::fs::read(&a.recipe).map(|b| b.len()).unwrap_or(0usize);

    let target = std::fs::read(&a.target)?;
    if target.is_empty() {
        anyhow::bail!("target is empty");
    }
    if a.scan_step == 0 {
        anyhow::bail!("--scan-step must be >= 1");
    }

    let seed = parse_seed_hex_opt(a.map_seed, &a.map_seed_hex)?;

    let cond_seed = parse_seed_hex_opt(a.cond_seed, &a.cond_seed_hex)?;
    let cond: Option<CondTags> = if let Some(p) = &a.cond_tags {
        Some(read_cond_tags(p, a.cond_tag_format, a.cond_block_bytes)?)
    } else {
        None
    };

    let n = target.len();

    let bytes_per_emission: u64 = match a.mode {
        ApplyMode::Pair => 1,
        ApplyMode::Rgbpair => 6,
    };

    let mut engine = Engine::new(recipe)?;

    while (engine.stats.emissions as u64) < a.start_emission && engine.stats.ticks < a.max_ticks {
        let _ = engine.step();
        if (engine.stats.emissions as u64) >= a.search_emissions {
            break;
        }
    }

    let start_ticks = engine.stats.ticks;
    let start_em = engine.stats.emissions as u64;

    let mut stream: Vec<u8> = Vec::new();
    stream.reserve(
        ((a.search_emissions.saturating_sub(start_em)).min(200_000) * bytes_per_emission) as usize,
    );

    while (engine.stats.emissions as u64) < a.search_emissions && engine.stats.ticks < a.max_ticks {
        if let Some(tok) = engine.step() {
            match a.mode {
                ApplyMode::Pair => stream.push(tok.pack_byte()),
                ApplyMode::Rgbpair => stream.extend_from_slice(&tok.to_rgb_pair().to_bytes()),
            }
        }
    }

    if stream.len() < n {
        anyhow::bail!(
            "timemap fit-xor short: need at least {} stream bytes after start_emission={}, got {} (mode={:?}, ticks={} delta_ticks={})",
            n,
            a.start_emission,
            stream.len(),
            a.mode,
            engine.stats.ticks,
            engine.stats.ticks.saturating_sub(start_ticks),
        );
    }

    let max_start = stream.len() - n;
    let abs_stream_base_pos: u64 = a.start_emission * bytes_per_emission;

    let mut scratch_resid: Vec<u8> = vec![0u8; n];

    let mut best_start: usize = 0;
    let mut best_matches: u64 = 0;

    let mut best_zstd_resid: usize = usize::MAX;
    let mut best_score_effective: usize = usize::MAX;

    let mut scanned: u64 = 0;

    let mut s: usize = 0;
    while s <= max_start {
        scanned += 1;

        let base_pos = abs_stream_base_pos + (s as u64);
        let mut m: u64 = 0;

        for i in 0..n {
            let pos = base_pos + (i as u64);
            let mapped0 = map_byte(a.map, seed, pos, stream[s + i]);
            let mapped = apply_conditioning_if_enabled(mapped0, &cond, cond_seed, i);
            let resid = make_residual_byte(a.residual, mapped, target[i]);
            scratch_resid[i] = resid;
            if resid == 0 {
                m += 1;
            }
        }

        let score_metric = match a.objective {
            FitObjective::Matches => (n as u64).saturating_sub(m) as usize,
            FitObjective::Zstd => zstd_compress_len(&scratch_resid, a.zstd_level),
        };

        // IMPORTANT FIX:
        // Previously we added tm1_len_contig(...) which overestimates program cost now that TM0 exists.
        // For contiguous indices, the on-disk timemap will be TM0 (tiny), so use tm0_len_contig(...) here.
        let tm_raw_len = tm0_len_contig(n as u64);
        let score_effective = score_metric.saturating_add(tm_raw_len);

        if score_effective < best_score_effective {
            best_score_effective = score_effective;
            best_zstd_resid = score_metric;
            best_start = s;
            best_matches = m;
            if best_score_effective == 0 {
                break;
            }
        }

        s = s.saturating_add(a.scan_step);
    }

    let abs_win_start_pos: u64 = abs_stream_base_pos + (best_start as u64);

    let tm = TimingMap::stride(n as u64, abs_win_start_pos, 1).map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut residual: Vec<u8> = Vec::with_capacity(n);
    for i in 0..n {
        let pos = abs_win_start_pos + (i as u64);
        let mapped0 = map_byte(a.map, seed, pos, stream[best_start + i]);
        let mapped = apply_conditioning_if_enabled(mapped0, &cond, cond_seed, i);
        residual.push(make_residual_byte(a.residual, mapped, target[i]));
    }

    let tm_bytes = tm.encode_auto();
    let tm_raw = tm_bytes.len();
    let tm_zstd = zstd_compress_len(&tm_bytes, a.zstd_level);

    let resid_raw = residual.len();
    let resid_zstd = zstd_compress_len(&residual, a.zstd_level);

    let plain_zstd = zstd_compress_len(&target, a.zstd_level);

    let effective_no_recipe = tm_zstd.saturating_add(resid_zstd);
    let effective_with_recipe = recipe_raw_len.saturating_add(effective_no_recipe);

    timemap::write_timemap_auto(&a.out_timemap, &tm)?;
    std::fs::write(&a.out_residual, &residual)?;

    eprintln!(
        "timemap fit-xor ok: mode={:?} map={:?} map_seed={} (0x{:016x}) residual={:?} objective={:?} scan_step={} scanned_windows={} zstd_level={} tm_out={} resid_out={} target_bytes={} matches={}/{} ({:.4}%) window_start_pos={} scanned_emissions={} stream_bytes={} ticks={} cond_tags={} cond_seed={} (0x{:016x}) cond_block_bytes={} cond_tag_format={:?}",
        a.mode,
        a.map,
        seed,
        seed,
        a.residual,
        a.objective,
        a.scan_step,
        scanned,
        a.zstd_level,
        a.out_timemap,
        a.out_residual,
        n,
        best_matches,
        n,
        (best_matches as f64) * 100.0 / (n as f64),
        abs_win_start_pos,
        (start_em + (stream.len() as u64 / bytes_per_emission)),
        stream.len(),
        engine.stats.ticks,
        a.cond_tags.as_deref().unwrap_or("<none>"),
        cond_seed,
        cond_seed,
        a.cond_block_bytes,
        a.cond_tag_format
    );

    eprintln!("--- scoreboard ---");
    eprintln!("recipe_raw_bytes           = {}", recipe_raw_len);
    eprintln!("plain_raw_bytes            = {}", target.len());
    eprintln!("plain_zstd_bytes           = {}", plain_zstd);
    eprintln!("tm_raw_bytes               = {}", tm_raw);
    eprintln!("tm_zstd_bytes              = {}", tm_zstd);
    eprintln!("resid_raw_bytes            = {}", resid_raw);
    eprintln!("resid_zstd_bytes           = {}", resid_zstd);
    eprintln!("effective_bytes_no_recipe  = {}", effective_no_recipe);
    eprintln!("effective_bytes_with_recipe= {}", effective_with_recipe);
    eprintln!(
        "delta_vs_plain_zstd_no_recipe  = {}",
        (effective_no_recipe as i64) - (plain_zstd as i64)
    );
    eprintln!(
        "delta_vs_plain_zstd_with_recipe= {}",
        (effective_with_recipe as i64) - (plain_zstd as i64)
    );
    eprintln!("note_best_scan_score_proxy_or_zstd = {}", best_zstd_resid);
    eprintln!(
        "note_best_scan_effective_prog_plus_score = {}",
        best_score_effective
    );

    Ok(())
}

pub fn cmd_fit_xor_chunked(a: FitXorChunkedArgs) -> anyhow::Result<()> {
    let recipe = recipe_file::load_k8r(&a.recipe)?;
    let recipe_raw_len = std::fs::read(&a.recipe).map(|b| b.len()).unwrap_or(0usize);

    let target = std::fs::read(&a.target)?;
    if target.is_empty() {
        anyhow::bail!("target is empty");
    }
    if a.chunk_size == 0 {
        anyhow::bail!("--chunk-size must be >= 1");
    }
    if a.scan_step == 0 {
        anyhow::bail!("--scan-step must be >= 1");
    }

    let seed = parse_seed_hex_opt(a.map_seed, &a.map_seed_hex)?;

    let cond_seed = parse_seed_hex_opt(a.cond_seed, &a.cond_seed_hex)?;
    let cond: Option<CondTags> = if let Some(p) = &a.cond_tags {
        Some(read_cond_tags(p, a.cond_tag_format, a.cond_block_bytes)?)
    } else {
        None
    };

    let bytes_per_emission: u64 = match a.mode {
        ApplyMode::Pair => 1,
        ApplyMode::Rgbpair => 6,
    };

    let mut engine = Engine::new(recipe)?;

    while (engine.stats.emissions as u64) < a.start_emission && engine.stats.ticks < a.max_ticks {
        let _ = engine.step();
        if (engine.stats.emissions as u64) >= a.search_emissions {
            break;
        }
    }

    let start_ticks = engine.stats.ticks;
    let start_em = engine.stats.emissions as u64;

    let mut stream: Vec<u8> = Vec::new();
    stream.reserve(
        ((a.search_emissions.saturating_sub(start_em)).min(500_000) * bytes_per_emission) as usize,
    );

    while (engine.stats.emissions as u64) < a.search_emissions && engine.stats.ticks < a.max_ticks {
        if let Some(tok) = engine.step() {
            match a.mode {
                ApplyMode::Pair => stream.push(tok.pack_byte()),
                ApplyMode::Rgbpair => stream.extend_from_slice(&tok.to_rgb_pair().to_bytes()),
            }
        }
    }

    let abs_stream_base_pos: u64 = a.start_emission * bytes_per_emission;
    let total_n = target.len();

    let mut tm_indices: Vec<u64> = Vec::with_capacity(total_n);
    let mut residual: Vec<u8> = Vec::with_capacity(total_n);

    eprintln!(
        "--- fit-xor-chunked --- mode={:?} map={:?} map_seed={} (0x{:016x}) residual={:?} objective={:?} refine_topk={} lookahead={} chunk_size={} scan_step={} zstd_level={} target_bytes={} stream_bytes={} base_pos={} start_emission={} end_emissions={} ticks={} delta_ticks={} cond_tags={} cond_seed={} (0x{:016x}) cond_block_bytes={} cond_tag_format={:?}",
        a.mode,
        a.map,
        seed,
        seed,
        a.residual,
        a.objective,
        a.refine_topk,
        a.lookahead,
        a.chunk_size,
        a.scan_step,
        a.zstd_level,
        total_n,
        stream.len(),
        abs_stream_base_pos,
        a.start_emission,
        (start_em + (stream.len() as u64 / bytes_per_emission)),
        engine.stats.ticks,
        engine.stats.ticks.saturating_sub(start_ticks),
        a.cond_tags.as_deref().unwrap_or("<none>"),
        cond_seed,
        cond_seed,
        a.cond_block_bytes,
        a.cond_tag_format
    );

    let mut prev_pos: Option<u64> = None;
    let mut chunk_idx: usize = 0;
    let mut off: usize = 0;

    while off < total_n {
        if a.max_chunks != 0 && chunk_idx >= a.max_chunks {
            break;
        }

        let remaining_total = total_n - off;
        let n = remaining_total.min(a.chunk_size);

        let min_pos: u64 = match prev_pos {
            None => abs_stream_base_pos,
            Some(p) => p.saturating_add(1),
        };

        let min_start: usize = (min_pos - abs_stream_base_pos) as usize;
        let max_start_cap = min_start.saturating_add(a.lookahead);

        let need_min = min_start.saturating_add(n);
        if need_min > stream.len()
            && !ensure_stream_len(
                &mut engine,
                &mut stream,
                need_min,
                a.mode,
                a.search_emissions,
                a.max_ticks,
            )
        {
            eprintln!("no room for chunk {} (writing partial)", chunk_idx);
            break;
        }

        let need_finish_from_min = min_start.saturating_add(remaining_total);
        if need_finish_from_min > stream.len()
            && !ensure_stream_len(
                &mut engine,
                &mut stream,
                need_finish_from_min,
                a.mode,
                a.search_emissions,
                a.max_ticks,
            )
        {
            eprintln!(
                "no room to finish from min_start for chunk {} (writing partial)",
                chunk_idx
            );
            break;
        }

        let max_start_possible = if stream.len() >= n {
            stream.len() - n
        } else {
            0
        };
        let max_start_finish = stream.len().saturating_sub(remaining_total);
        let max_start: usize = max_start_possible.min(max_start_cap).min(max_start_finish);

        if min_start > max_start {
            eprintln!("no legal window for chunk {} (writing partial)", chunk_idx);
            break;
        }

        let mut scratch_resid: Vec<u8> = vec![0u8; n];
        let mut best_start_proxy: usize = min_start;
        let mut best_matches_proxy: u64 = 0;
        let mut best_proxy_score: usize = usize::MAX;

        let mut refine: Vec<(usize, usize, u64)> = Vec::new();
        let mut scanned: u64 = 0;

        let mut s: usize = min_start;
        while s <= max_start {
            scanned += 1;

            let base_pos = abs_stream_base_pos + (s as u64);
            let mut matches: u64 = 0;

            for i in 0..n {
                let pos = base_pos + (i as u64);
                let mapped0 = map_byte(a.map, seed, pos, stream[s + i]);
                let mapped = apply_conditioning_if_enabled(mapped0, &cond, cond_seed, off + i);
                let resid_b = make_residual_byte(a.residual, mapped, target[off + i]);
                scratch_resid[i] = resid_b;
                if resid_b == 0 {
                    matches += 1;
                }
            }

            let jump_cost = tm_jump_cost(prev_pos, base_pos);

            if a.objective == FitObjective::Zstd {
                let zlen = zstd_compress_len(&scratch_resid, a.zstd_level);
                let score = zlen.saturating_add(jump_cost);
                if score < best_proxy_score || (score == best_proxy_score && s < best_start_proxy) {
                    best_proxy_score = score;
                    best_start_proxy = s;
                    best_matches_proxy = matches;
                }
            } else {
                let proxy_cost = (n as u64).saturating_sub(matches) as usize;
                let proxy_score = proxy_cost.saturating_add(jump_cost);
                if proxy_score < best_proxy_score
                    || (proxy_score == best_proxy_score && s < best_start_proxy)
                {
                    best_proxy_score = proxy_score;
                    best_start_proxy = s;
                    best_matches_proxy = matches;
                }
                if a.refine_topk != 0 {
                    refine.push((proxy_score, s, matches));
                }
            }

            s = s.saturating_add(a.scan_step);
        }

        let mut best_start: usize = best_start_proxy;
        let mut best_matches: u64 = best_matches_proxy;
        let mut best_score: usize = best_proxy_score;
        let mut best_resid_zstd: usize = usize::MAX;

        if a.objective == FitObjective::Matches && a.refine_topk != 0 && !refine.is_empty() {
            refine.sort_by(|a1, b1| a1.0.cmp(&b1.0).then_with(|| a1.1.cmp(&b1.1)));
            if refine.len() > a.refine_topk {
                refine.truncate(a.refine_topk);
            }

            for &(_proxy_score, cand_s, cand_matches) in refine.iter() {
                let base_pos = abs_stream_base_pos + (cand_s as u64);
                let jump_cost = tm_jump_cost(prev_pos, base_pos);

                for i in 0..n {
                    let pos = base_pos + (i as u64);
                    let mapped0 = map_byte(a.map, seed, pos, stream[cand_s + i]);
                    let mapped = apply_conditioning_if_enabled(mapped0, &cond, cond_seed, off + i);
                    scratch_resid[i] = make_residual_byte(a.residual, mapped, target[off + i]);
                }

                let zlen = zstd_compress_len(&scratch_resid, a.zstd_level);
                let score = zlen.saturating_add(jump_cost);

                if score < best_score || (score == best_score && cand_s < best_start) {
                    best_score = score;
                    best_start = cand_s;
                    best_matches = cand_matches;
                    best_resid_zstd = zlen;
                }
            }
        }

        let base_pos = abs_stream_base_pos + (best_start as u64);
        let jump_cost = tm_jump_cost(prev_pos, base_pos);

        for i in 0..n {
            let pos = base_pos + (i as u64);
            let mapped0 = map_byte(a.map, seed, pos, stream[best_start + i]);
            let mapped = apply_conditioning_if_enabled(mapped0, &cond, cond_seed, off + i);
            tm_indices.push(pos);
            residual.push(make_residual_byte(a.residual, mapped, target[off + i]));
        }

        prev_pos = Some(base_pos + (n as u64) - 1);

        let printed_resid_metric = if a.objective == FitObjective::Zstd {
            let mut scratch: Vec<u8> = vec![0u8; n];
            for i in 0..n {
                let pos = base_pos + (i as u64);
                let mapped0 = map_byte(a.map, seed, pos, stream[best_start + i]);
                let mapped = apply_conditioning_if_enabled(mapped0, &cond, cond_seed, off + i);
                scratch[i] = make_residual_byte(a.residual, mapped, target[off + i]);
            }
            zstd_compress_len(&scratch, a.zstd_level)
        } else if best_resid_zstd != usize::MAX {
            best_resid_zstd
        } else {
            (n as u64).saturating_sub(best_matches) as usize
        };

        eprintln!(
            "chunk {:04} off={} len={} start_pos={} scanned_windows={} matches={}/{} ({:.2}%) jump_cost={} chunk_score={} chunk_resid_metric={}",
            chunk_idx,
            off,
            n,
            base_pos,
            scanned,
            best_matches,
            n,
            (best_matches as f64) * 100.0 / (n as f64),
            jump_cost,
            best_score,
            printed_resid_metric
        );

        off += n;
        chunk_idx += 1;
    }

    if tm_indices.len() != residual.len() {
        anyhow::bail!(
            "internal: tm_indices/residual len mismatch: tm={} resid={}",
            tm_indices.len(),
            residual.len()
        );
    }
    if tm_indices.is_empty() {
        anyhow::bail!("no output produced");
    }

    let produced = residual.len();
    if produced != target.len() {
        eprintln!(
            "note: partial output produced_bytes={} target_bytes={}",
            produced,
            target.len()
        );
    }

    let tm = TimingMap {
        indices: tm_indices,
    };
    let tm_bytes = tm.encode_auto();
    let tm_raw = tm_bytes.len();
    let tm_zstd = zstd_compress_len(&tm_bytes, a.zstd_level);

    let resid_raw = residual.len();
    let resid_zstd = zstd_compress_len(&residual, a.zstd_level);

    let target_slice = &target[..produced];
    let plain_zstd = zstd_compress_len(target_slice, a.zstd_level);

    let effective_no_recipe = tm_zstd.saturating_add(resid_zstd);
    let effective_with_recipe = recipe_raw_len.saturating_add(effective_no_recipe);

    timemap::write_timemap_auto(&a.out_timemap, &tm)?;
    std::fs::write(&a.out_residual, &residual)?;

    eprintln!("--- scoreboard ---");
    eprintln!("recipe_raw_bytes           = {}", recipe_raw_len);
    eprintln!("plain_raw_bytes            = {}", produced);
    eprintln!("plain_zstd_bytes           = {}", plain_zstd);
    eprintln!("tm_raw_bytes               = {}", tm_raw);
    eprintln!("tm_zstd_bytes              = {}", tm_zstd);
    eprintln!("resid_raw_bytes            = {}", resid_raw);
    eprintln!("resid_zstd_bytes           = {}", resid_zstd);
    eprintln!("effective_bytes_no_recipe  = {}", effective_no_recipe);
    eprintln!("effective_bytes_with_recipe= {}", effective_with_recipe);
    eprintln!(
        "delta_vs_plain_zstd_no_recipe  = {}",
        (effective_no_recipe as i64) - (plain_zstd as i64)
    );
    eprintln!(
        "delta_vs_plain_zstd_with_recipe= {}",
        (effective_with_recipe as i64) - (plain_zstd as i64)
    );

    Ok(())
}

pub fn cmd_reconstruct(a: ReconstructArgs) -> anyhow::Result<()> {
    let recipe = recipe_file::load_k8r(&a.recipe)?;
    let tm = timemap::read_timemap(&a.timemap)?;
    if tm.indices.is_empty() {
        anyhow::bail!("timemap empty");
    }

    let seed = parse_seed_hex_opt(a.map_seed, &a.map_seed_hex)?;

    let cond_seed = parse_seed_hex_opt(a.cond_seed, &a.cond_seed_hex)?;
    let cond: Option<CondTags> = if let Some(p) = &a.cond_tags {
        Some(read_cond_tags(p, a.cond_tag_format, a.cond_block_bytes)?)
    } else {
        None
    };

    let resid = std::fs::read(&a.residual)?;
    if tm.indices.len() != resid.len() {
        anyhow::bail!(
            "timemap/residual len mismatch: tm={} resid={}",
            tm.indices.len(),
            resid.len()
        );
    }

    let mut engine = Engine::new(recipe)?;
    let mut out: Vec<u8> = Vec::with_capacity(resid.len());
    let mut i: usize = 0;

    let mut max_idx: u64 = 0;
    for &idx in tm.indices.iter() {
        if idx > max_idx {
            max_idx = idx;
        }
    }

    match a.mode {
        ApplyMode::Pair => {
            while engine.stats.ticks < a.max_ticks && (engine.stats.emissions as u64) <= max_idx {
                if let Some(tok) = engine.step() {
                    let idx = (engine.stats.emissions - 1) as u64;

                    while i < tm.indices.len() && tm.indices[i] == idx {
                        let mapped0 = map_byte(a.map, seed, idx, tok.pack_byte());
                        let mapped = apply_conditioning_if_enabled(mapped0, &cond, cond_seed, i);
                        out.push(apply_residual_byte(a.residual_mode, mapped, resid[i]));
                        i += 1;
                    }
                }
            }
        }
        ApplyMode::Rgbpair => {
            while engine.stats.ticks < a.max_ticks
                && ((engine.stats.emissions as u64) * 6) <= max_idx
            {
                if let Some(tok) = engine.step() {
                    let em = (engine.stats.emissions - 1) as u64;
                    let base = em * 6;
                    let rgb6 = tok.to_rgb_pair().to_bytes();

                    for lane in 0..6u64 {
                        let pos = base + lane;
                        if pos > max_idx {
                            break;
                        }
                        while i < tm.indices.len() && tm.indices[i] == pos {
                            let mapped0 = map_byte(a.map, seed, pos, rgb6[lane as usize]);
                            let mapped =
                                apply_conditioning_if_enabled(mapped0, &cond, cond_seed, i);
                            out.push(apply_residual_byte(a.residual_mode, mapped, resid[i]));
                            i += 1;
                        }
                    }
                }
            }
        }
    }

    if i != tm.indices.len() {
        anyhow::bail!(
            "reconstruct short: wrote {} of {} bytes (max_idx={} ticks={} emissions={})",
            i,
            tm.indices.len(),
            max_idx,
            engine.stats.ticks,
            engine.stats.emissions
        );
    }

    std::fs::write(&a.out, &out)?;
    eprintln!(
        "reconstruct ok: out={} bytes={} ticks={} emissions={} map_seed={} (0x{:016x}) cond_tags={} cond_seed={} (0x{:016x})",
        a.out,
        out.len(),
        engine.stats.ticks,
        engine.stats.emissions,
        seed,
        seed,
        a.cond_tags.as_deref().unwrap_or("<none>"),
        cond_seed,
        cond_seed
    );
    Ok(())
}

// ---- helpers ----

fn ensure_stream_len(
    engine: &mut Engine,
    stream: &mut Vec<u8>,
    need_len: usize,
    mode: ApplyMode,
    search_emissions: u64,
    max_ticks: u64,
) -> bool {
    if stream.len() >= need_len {
        return true;
    }

    while stream.len() < need_len
        && (engine.stats.emissions as u64) < search_emissions
        && engine.stats.ticks < max_ticks
    {
        if let Some(tok) = engine.step() {
            match mode {
                ApplyMode::Pair => stream.push(tok.pack_byte()),
                ApplyMode::Rgbpair => stream.extend_from_slice(&tok.to_rgb_pair().to_bytes()),
            }
        }
    }

    stream.len() >= need_len
}

fn collect_pair_bytes(
    engine: &mut Engine,
    tm: &TimingMap,
    max_ticks: u64,
) -> anyhow::Result<Vec<u8>> {
    let mut out: Vec<u8> = Vec::with_capacity(tm.indices.len());
    let mut i: usize = 0;
    let max_idx = *tm.indices.last().unwrap_or(&0);

    while engine.stats.ticks < max_ticks && (engine.stats.emissions as u64) <= max_idx {
        if let Some(tok) = engine.step() {
            let idx = (engine.stats.emissions - 1) as u64;
            while i < tm.indices.len() && tm.indices[i] == idx {
                out.push(tok.pack_byte());
                i += 1;
            }
        }
    }

    if i != tm.indices.len() {
        anyhow::bail!(
            "apply short: wrote {} of {} bytes (max_idx={} ticks={} emissions={})",
            i,
            tm.indices.len(),
            max_idx,
            engine.stats.ticks,
            engine.stats.emissions
        );
    }

    Ok(out)
}

fn collect_rgbpair_bytes(
    engine: &mut Engine,
    tm: &TimingMap,
    max_ticks: u64,
) -> anyhow::Result<Vec<u8>> {
    let mut out: Vec<u8> = Vec::with_capacity(tm.indices.len());
    let mut i: usize = 0;
    let max_idx = *tm.indices.last().unwrap_or(&0);

    while engine.stats.ticks < max_ticks && ((engine.stats.emissions as u64) * 6) <= max_idx {
        if let Some(tok) = engine.step() {
            let em = (engine.stats.emissions - 1) as u64;
            let base = em * 6;
            let rgb6 = tok.to_rgb_pair().to_bytes();

            for lane in 0..6u64 {
                let pos = base + lane;
                if pos > max_idx {
                    break;
                }
                while i < tm.indices.len() && tm.indices[i] == pos {
                    out.push(rgb6[lane as usize]);
                    i += 1;
                }
            }
        }
    }

    if i != tm.indices.len() {
        anyhow::bail!(
            "apply short: wrote {} of {} bytes (max_idx={} ticks={} emissions={})",
            i,
            tm.indices.len(),
            max_idx,
            engine.stats.ticks,
            engine.stats.emissions
        );
    }

    Ok(out)
}

// TM0 raw size estimate for contiguous stride (step=1).
// Format is:
// MAGIC(4) + varint(len) + varint(start) + varint(step)
// This is used only for scan scoring in cmd_fit_xor.
fn tm0_len_contig(len: u64) -> usize {
    4 + varint_len_u64(len) + varint_len_u64(0) + varint_len_u64(1)
}

fn varint_len_u64(mut x: u64) -> usize {
    let mut n = 1usize;
    while x >= 0x80 {
        x >>= 7;
        n += 1;
    }
    n
}
