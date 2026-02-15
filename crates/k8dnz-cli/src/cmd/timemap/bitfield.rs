// crates/k8dnz-cli/src/cmd/timemap/bitfield.rs

use super::args::*;
use super::residual::{apply_residual_symbol, make_residual_symbol, sym_mask};
use super::util::{parse_seed_hex_opt, tm_jump_cost, zstd_compress_len, zstd_decompress};

use anyhow::Context;

use k8dnz_core::signal::bitpack;
use k8dnz_core::signal::timing_map::TimingMap;
use k8dnz_core::Engine;

use crate::io::{recipe_file, timemap};

const BF1_MAGIC: &[u8; 4] = b"BF1\0";
const BF2_MAGIC: &[u8; 4] = b"BF2\0";

/// Local helper: compress bytes with zstd at a given level.
fn zstd_compress(bytes: &[u8], level: i32) -> anyhow::Result<Vec<u8>> {
    zstd::encode_all(bytes, level).map_err(|e| anyhow::anyhow!("zstd compress: {e}"))
}

/// Local helper: decompress bytes with zstd.
fn zstd_decompress_bytes(bytes: &[u8]) -> anyhow::Result<Vec<u8>> {
    zstd::decode_all(bytes).map_err(|e| anyhow::anyhow!("zstd decompress: {e}"))
}

#[derive(Clone, Debug)]
pub enum BitfieldResidual {
    /// BF1: packed residual symbols (payload = packed symbols)
    Bf1 {
        bits_per_emission: u8,
        mapping: BitMapping,
        orig_len_bytes: usize,
        symbol_count: usize,
        packed_symbols: Vec<u8>,
    },
    /// BF2: time-split lanes (payload = lane bitsets, each zstd-compressed)
    Bf2 {
        bits_per_emission: u8,
        mapping: BitMapping,
        orig_len_bytes: usize,
        symbol_count: usize,
        lane_count: usize,
        lanes_raw_bitsets: Vec<Vec<u8>>, // decompressed bitsets, each len = (symbol_count+7)/8
    },
}

fn mapping_tag(m: BitMapping) -> u8 {
    match m {
        BitMapping::Geom => 0,
        BitMapping::Hash => 1,
    }
}

fn mapping_from_tag(v: u8) -> anyhow::Result<BitMapping> {
    match v {
        0 => Ok(BitMapping::Geom),
        1 => Ok(BitMapping::Hash),
        _ => anyhow::bail!("bitfield residual unknown mapping tag: {}", v),
    }
}

fn read_bitfield_residual(path: &str) -> anyhow::Result<BitfieldResidual> {
    let bytes = std::fs::read(path).with_context(|| format!("read bf: {}", path))?;

    if bytes.len() < 24 {
        anyhow::bail!("bitfield residual too small: {} bytes", bytes.len());
    }

    let magic = &bytes[0..4];

    // ---------------- BF1 ----------------
    if magic == BF1_MAGIC {
        let bits = bytes[4];
        let mapping_u8 = bytes[5];
        let mapping = mapping_from_tag(mapping_u8)?;

        let orig_len_bytes = u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as usize;
        let symbol_count = u64::from_le_bytes(bytes[16..24].try_into().unwrap()) as usize;
        let payload = bytes[24..].to_vec();

        return Ok(BitfieldResidual::Bf1 {
            bits_per_emission: bits,
            mapping,
            orig_len_bytes,
            symbol_count,
            packed_symbols: payload,
        });
    }

    // ---------------- BF2 ----------------
    if magic == BF2_MAGIC {
        if bytes.len() < 32 {
            anyhow::bail!("BF2 residual too small: {} bytes", bytes.len());
        }

        let bits = bytes[4];
        let mapping_u8 = bytes[5];
        let mapping = mapping_from_tag(mapping_u8)?;

        let orig_len_bytes = u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as usize;
        let symbol_count = u64::from_le_bytes(bytes[16..24].try_into().unwrap()) as usize;
        let lane_count = u32::from_le_bytes(bytes[24..28].try_into().unwrap()) as usize;

        let bitset_len = (symbol_count + 7) / 8;

        let mut cursor = 32usize;
        let mut lanes_raw: Vec<Vec<u8>> = Vec::with_capacity(lane_count);

        for lane_i in 0..lane_count {
            if cursor + 4 > bytes.len() {
                anyhow::bail!("BF2 truncated reading lane length (lane {})", lane_i);
            }
            let clen = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
            cursor += 4;

            if cursor + clen > bytes.len() {
                anyhow::bail!("BF2 truncated reading lane payload (lane {})", lane_i);
            }
            let comp = &bytes[cursor..cursor + clen];
            cursor += clen;

            let raw = zstd_decompress_bytes(comp)?;
            if raw.len() != bitset_len {
                anyhow::bail!(
                    "BF2 lane bitset len mismatch (lane {}): got {} want {}",
                    lane_i,
                    raw.len(),
                    bitset_len
                );
            }
            lanes_raw.push(raw);
        }

        return Ok(BitfieldResidual::Bf2 {
            bits_per_emission: bits,
            mapping,
            orig_len_bytes,
            symbol_count,
            lane_count,
            lanes_raw_bitsets: lanes_raw,
        });
    }

    anyhow::bail!("bitfield residual bad magic (expected BF1\\0 or BF2\\0)");
}

fn write_bitfield_residual_bf1(
    path: &str,
    bits_per_emission: u8,
    mapping: BitMapping,
    orig_len_bytes: usize,
    residual_symbols: &[u8],
) -> anyhow::Result<Vec<u8>> {
    let packed = bitpack::pack_symbols(bits_per_emission, residual_symbols)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut out: Vec<u8> = Vec::with_capacity(24 + packed.len());
    out.extend_from_slice(BF1_MAGIC);
    out.push(bits_per_emission);
    out.push(mapping_tag(mapping));
    out.extend_from_slice(&[0u8, 0u8]); // padding
    out.extend_from_slice(&(orig_len_bytes as u64).to_le_bytes());
    out.extend_from_slice(&(residual_symbols.len() as u64).to_le_bytes());
    out.extend_from_slice(&packed);

    std::fs::write(path, &out).with_context(|| format!("write BF1 residual: {}", path))?;
    Ok(packed)
}

fn write_bitfield_residual_bf2(
    path: &str,
    bits_per_emission: u8,
    mapping: BitMapping,
    orig_len_bytes: usize,
    residual_symbols: &[u8],
    zstd_level: i32,
) -> anyhow::Result<()> {
    if bits_per_emission == 0 || bits_per_emission > 8 {
        anyhow::bail!("BF2: bits_per_emission must be 1..=8");
    }
    let lane_count: usize = 1usize << (bits_per_emission as usize);
    let symbol_count = residual_symbols.len();
    let bitset_len = (symbol_count + 7) / 8;

    // Build lane bitsets
    let mut lane_bitsets: Vec<Vec<u8>> = (0..lane_count).map(|_| vec![0u8; bitset_len]).collect();
    let mask = sym_mask(bits_per_emission);

    for (i, &s) in residual_symbols.iter().enumerate() {
        let lane = (s & mask) as usize;
        let byte_i = i >> 3;
        let bit_i = (i & 7) as u8;
        lane_bitsets[lane][byte_i] |= 1u8 << bit_i;
    }

    // Compress each lane independently
    let mut lane_comp: Vec<Vec<u8>> = Vec::with_capacity(lane_count);
    for lane in lane_bitsets.iter() {
        lane_comp.push(zstd_compress(lane, zstd_level)?);
    }

    // Header: 32 bytes
    let mut out: Vec<u8> = Vec::new();
    out.extend_from_slice(BF2_MAGIC);
    out.push(bits_per_emission);
    out.push(mapping_tag(mapping));
    out.extend_from_slice(&[0u8, 0u8]); // padding
    out.extend_from_slice(&(orig_len_bytes as u64).to_le_bytes());
    out.extend_from_slice(&(symbol_count as u64).to_le_bytes());
    out.extend_from_slice(&(lane_count as u32).to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes()); // reserved

    // Lanes: [u32 len][bytes...]
    for c in lane_comp.iter() {
        out.extend_from_slice(&(c.len() as u32).to_le_bytes());
        out.extend_from_slice(c);
    }

    std::fs::write(path, &out).with_context(|| format!("write BF2 residual: {}", path))?;
    Ok(())
}

fn map_symbol_bitfield(
    mapping: BitMapping,
    map_seed: u64,
    emission: u64,
    rgb6: &[u8; 6],
    bits_per_emission: u8,
) -> u8 {
    let mask = sym_mask(bits_per_emission);

    match mapping {
        BitMapping::Geom => {
            let r = ((rgb6[0] as u16) + (rgb6[3] as u16)) / 2;
            let g = ((rgb6[1] as u16) + (rgb6[4] as u16)) / 2;
            let b = ((rgb6[2] as u16) + (rgb6[5] as u16)) / 2;

            let mut sym: u8 = 0;

            if bits_per_emission >= 1 {
                sym |= ((r > g) as u8) << 0;
            }
            if bits_per_emission >= 2 {
                sym |= ((b > g) as u8) << 1;
            }
            if bits_per_emission >= 3 {
                sym |= ((r > b) as u8) << 2;
            }
            if bits_per_emission >= 4 {
                sym |= ((g > r) as u8) << 3;
            }
            if bits_per_emission >= 5 {
                sym |= ((g > b) as u8) << 4;
            }
            if bits_per_emission >= 6 {
                let y = r + g + b;
                sym |= ((y > (3 * 128)) as u8) << 5;
            }
            if bits_per_emission >= 7 {
                sym |= (((r as u8) & 0x40 != 0) as u8) << 6;
            }
            if bits_per_emission >= 8 {
                sym |= (((b as u8) & 0x40 != 0) as u8) << 7;
            }

            sym & mask
        }
        BitMapping::Hash => {
            let mut x = map_seed ^ emission.rotate_left(17);
            for &b in rgb6.iter() {
                x ^= b as u64;
                x = x.wrapping_mul(0x9e3779b97f4a7c15);
                x ^= x >> 32;
            }
            (x as u8) & mask
        }
    }
}

fn ensure_symbol_stream_len(
    engine: &mut Engine,
    stream_syms: &mut Vec<u8>,
    need_len: usize,
    mapping: BitMapping,
    map_seed: u64,
    bits_per_emission: u8,
    search_emissions: u64,
    max_ticks: u64,
) -> bool {
    if stream_syms.len() >= need_len {
        return true;
    }

    while stream_syms.len() < need_len
        && (engine.stats.emissions as u64) < search_emissions
        && engine.stats.ticks < max_ticks
    {
        if let Some(tok) = engine.step() {
            let em = (engine.stats.emissions - 1) as u64;
            let rgb6 = tok.to_rgb_pair().to_bytes();
            let sym = map_symbol_bitfield(mapping, map_seed, em, &rgb6, bits_per_emission);
            stream_syms.push(sym);
        }
    }

    stream_syms.len() >= need_len
}

pub fn cmd_fit_xor_chunked_bitfield(a: FitXorChunkedArgs) -> anyhow::Result<()> {
    if a.mode != ApplyMode::Rgbpair {
        anyhow::bail!("--map bitfield requires --mode rgbpair");
    }
    if a.bits_per_emission == 0 || a.bits_per_emission > 8 {
        anyhow::bail!("--bits-per-emission must be in 1..=8");
    }
    if a.chunk_size == 0 {
        anyhow::bail!("--chunk-size must be >= 1");
    }
    if a.scan_step == 0 {
        anyhow::bail!("--scan-step must be >= 1");
    }

    let recipe = recipe_file::load_k8r(&a.recipe)?;
    let recipe_raw_len = std::fs::read(&a.recipe).map(|b| b.len()).unwrap_or(0usize);

    let target_bytes =
        std::fs::read(&a.target).with_context(|| format!("read target: {}", a.target))?;
    if target_bytes.is_empty() {
        anyhow::bail!("target is empty");
    }

    let seed = parse_seed_hex_opt(a.map_seed, &a.map_seed_hex)?;

    let bit_len: usize = target_bytes.len() * 8;
    let sym_count: usize =
        (bit_len + (a.bits_per_emission as usize) - 1) / (a.bits_per_emission as usize);

    let target_syms = bitpack::unpack_symbols(a.bits_per_emission, &target_bytes, sym_count)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let mask = sym_mask(a.bits_per_emission);

    let mut engine = Engine::new(recipe)?;

    while (engine.stats.emissions as u64) < a.start_emission && engine.stats.ticks < a.max_ticks {
        let _ = engine.step();
        if (engine.stats.emissions as u64) >= a.search_emissions {
            break;
        }
    }

    let start_ticks = engine.stats.ticks;
    let start_em = engine.stats.emissions as u64;

    let mut stream_syms: Vec<u8> = Vec::new();
    stream_syms.reserve((a.search_emissions.saturating_sub(start_em)).min(500_000) as usize);

    while (engine.stats.emissions as u64) < a.search_emissions && engine.stats.ticks < a.max_ticks {
        if let Some(tok) = engine.step() {
            let em = (engine.stats.emissions - 1) as u64;
            let rgb6 = tok.to_rgb_pair().to_bytes();
            let sym = map_symbol_bitfield(a.bit_mapping, seed, em, &rgb6, a.bits_per_emission);
            stream_syms.push(sym & mask);
        }
    }

    let abs_stream_base_pos: u64 = a.start_emission;
    let total_n = target_syms.len();

    let mut tm_indices: Vec<u64> = Vec::with_capacity(total_n.min(stream_syms.len()));
    let mut residual_syms: Vec<u8> = Vec::with_capacity(total_n.min(stream_syms.len()));

    eprintln!(
        "--- fit-xor-chunked (bitfield) --- map=bitfield bits_per_emission={} bit_mapping={:?} map_seed={} (0x{:016x}) residual={:?} objective={:?} refine_topk={} lookahead={} trans_penalty={} chunk_size={} scan_step={} zstd_level={} target_bytes={} target_symbols={} stream_symbols={} base_pos={} start_emission={} end_emissions={} ticks={} delta_ticks={}",
        a.bits_per_emission,
        a.bit_mapping,
        seed,
        seed,
        a.residual,
        a.objective,
        a.refine_topk,
        a.lookahead,
        a.trans_penalty,
        a.chunk_size,
        a.scan_step,
        a.zstd_level,
        target_bytes.len(),
        total_n,
        stream_syms.len(),
        abs_stream_base_pos,
        a.start_emission,
        (start_em + (stream_syms.len() as u64)),
        engine.stats.ticks,
        engine.stats.ticks.saturating_sub(start_ticks),
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

        // Only require enough stream to score THIS CHUNK (n), not the entire remaining_total.
        let need_min = min_start.saturating_add(n);
        if need_min > stream_syms.len()
            && !ensure_symbol_stream_len(
                &mut engine,
                &mut stream_syms,
                need_min,
                a.bit_mapping,
                seed,
                a.bits_per_emission,
                a.search_emissions,
                a.max_ticks,
            )
        {
            eprintln!(
                "no room for chunk {} (need {} syms, have {}); stopping (partial output)",
                chunk_idx,
                need_min,
                stream_syms.len()
            );
            break;
        }

        let max_start_possible = if stream_syms.len() >= n {
            stream_syms.len() - n
        } else {
            0
        };
        let max_start: usize = max_start_possible.min(max_start_cap);

        if min_start > max_start {
            eprintln!("no legal window for chunk {} (partial output)", chunk_idx);
            break;
        }

        let mut scratch_resid: Vec<u8> = vec![0u8; n];

        let mut best_start_proxy: usize = min_start;
        let mut best_matches_proxy: u64 = 0;
        let mut best_proxy_score: usize = usize::MAX;

        let mut refine: Vec<(usize, usize, u64)> = Vec::new();
        let mut scanned: u64 = 0;

        let mut s0: usize = min_start;
        while s0 <= max_start {
            scanned += 1;

            let base_pos = abs_stream_base_pos + (s0 as u64);
            let mut matches: u64 = 0;

            for i in 0..n {
                let pred = stream_syms[s0 + i] & mask;
                let resid_b =
                    make_residual_symbol(a.residual, pred, target_syms[off + i] & mask, mask);
                scratch_resid[i] = resid_b;
                if resid_b == 0 {
                    matches += 1;
                }
            }

            let jump_cost_raw = tm_jump_cost(prev_pos, base_pos) as u64;
            let jump_cost_u64 = jump_cost_raw.saturating_mul(a.trans_penalty);
            let jump_cost = jump_cost_u64 as usize;

            if a.objective == FitObjective::Zstd {
                let zlen = zstd_compress_len(&scratch_resid, a.zstd_level);
                let score = zlen.saturating_add(jump_cost);
                if score < best_proxy_score || (score == best_proxy_score && s0 < best_start_proxy)
                {
                    best_proxy_score = score;
                    best_start_proxy = s0;
                    best_matches_proxy = matches;
                }
            } else {
                let proxy_cost = (n as u64).saturating_sub(matches) as usize;
                let proxy_score = proxy_cost.saturating_add(jump_cost);
                if proxy_score < best_proxy_score
                    || (proxy_score == best_proxy_score && s0 < best_start_proxy)
                {
                    best_proxy_score = proxy_score;
                    best_start_proxy = s0;
                    best_matches_proxy = matches;
                }
                if a.refine_topk != 0 {
                    refine.push((proxy_score, s0, matches));
                }
            }

            s0 = s0.saturating_add(a.scan_step);
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

                let jump_cost_raw = tm_jump_cost(prev_pos, base_pos) as u64;
                let jump_cost_u64 = jump_cost_raw.saturating_mul(a.trans_penalty);
                let jump_cost = jump_cost_u64 as usize;

                for i in 0..n {
                    scratch_resid[i] = make_residual_symbol(
                        a.residual,
                        stream_syms[cand_s + i] & mask,
                        target_syms[off + i] & mask,
                        mask,
                    );
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

        let jump_cost_raw = tm_jump_cost(prev_pos, base_pos) as u64;
        let jump_cost_u64 = jump_cost_raw.saturating_mul(a.trans_penalty);
        let jump_cost = jump_cost_u64 as usize;

        for i in 0..n {
            let pos = base_pos + (i as u64);
            tm_indices.push(pos);
            residual_syms.push(make_residual_symbol(
                a.residual,
                stream_syms[best_start + i] & mask,
                target_syms[off + i] & mask,
                mask,
            ));
        }

        prev_pos = Some(base_pos + (n as u64) - 1);

        let printed_resid_metric = if a.objective == FitObjective::Zstd {
            let mut scratch: Vec<u8> = vec![0u8; n];
            for i in 0..n {
                scratch[i] = make_residual_symbol(
                    a.residual,
                    stream_syms[best_start + i] & mask,
                    target_syms[off + i] & mask,
                    mask,
                );
            }
            zstd_compress_len(&scratch, a.zstd_level)
        } else if best_resid_zstd != usize::MAX {
            best_resid_zstd
        } else {
            (n as u64).saturating_sub(best_matches) as usize
        };

        eprintln!(
            "chunk {:04} off_sym={} len_sym={} start_emission={} scanned_windows={} matches={}/{} ({:.2}%) jump_cost={} chunk_score={} chunk_resid_metric={}",
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

    if tm_indices.len() != residual_syms.len() {
        anyhow::bail!(
            "internal: tm_indices/residual len mismatch: tm={} resid={}",
            tm_indices.len(),
            residual_syms.len()
        );
    }
    if tm_indices.is_empty() {
        anyhow::bail!("no output produced");
    }

    let produced_syms = residual_syms.len();
    if produced_syms != target_syms.len() {
        eprintln!(
            "note: partial output produced_symbols={} target_symbols={}",
            produced_syms,
            target_syms.len()
        );
    }

    let tm = TimingMap {
        indices: tm_indices,
    };

    let tm_bytes = tm.encode_tm1();
    let tm_raw = tm_bytes.len();
    let tm_zstd = zstd_compress_len(&tm_bytes, a.zstd_level);

    // Decide BF encoding
    let want_lanes = a.time_split || a.bitfield_residual == BitfieldResidualEncoding::Lanes;

    // Write residual
    let (resid_raw, resid_zstd) = if !want_lanes {
        let packed_resid = write_bitfield_residual_bf1(
            &a.out_residual,
            a.bits_per_emission,
            a.bit_mapping,
            target_bytes.len(),
            &residual_syms,
        )?;
        let resid_raw = packed_resid.len();
        let resid_zstd = zstd_compress_len(&packed_resid, a.zstd_level);
        (resid_raw, resid_zstd)
    } else {
        write_bitfield_residual_bf2(
            &a.out_residual,
            a.bits_per_emission,
            a.bit_mapping,
            target_bytes.len(),
            &residual_syms,
            a.zstd_level,
        )?;
        let file_bytes = std::fs::read(&a.out_residual)
            .with_context(|| format!("read back residual for sizing: {}", a.out_residual))?;
        let resid_raw = file_bytes.len();
        let resid_zstd = zstd_compress_len(&file_bytes, a.zstd_level);
        (resid_raw, resid_zstd)
    };

    let target_packed = bitpack::pack_symbols(a.bits_per_emission, &target_syms[..produced_syms])
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let plain_zstd = zstd_compress_len(&target_packed, a.zstd_level);

    let effective_no_recipe = tm_zstd.saturating_add(resid_zstd);
    let effective_with_recipe = recipe_raw_len.saturating_add(effective_no_recipe);

    timemap::write_tm1(&a.out_timemap, &tm)?;

    eprintln!("--- scoreboard (bitfield) ---");
    eprintln!("recipe_raw_bytes           = {}", recipe_raw_len);
    eprintln!("plain_raw_bytes            = {}", target_bytes.len());
    eprintln!("plain_zstd_bytes           = {}", plain_zstd);
    eprintln!("tm1_raw_bytes              = {}", tm_raw);
    eprintln!("tm1_zstd_bytes             = {}", tm_zstd);
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

pub fn cmd_reconstruct_bitfield(a: ReconstructArgs) -> anyhow::Result<()> {
    if a.mode != ApplyMode::Rgbpair {
        anyhow::bail!("--map bitfield requires --mode rgbpair");
    }
    if a.bits_per_emission == 0 || a.bits_per_emission > 8 {
        anyhow::bail!("--bits-per-emission must be in 1..=8");
    }

    let recipe = recipe_file::load_k8r(&a.recipe)?;
    let tm = timemap::read_tm1(&a.timemap)?;
    if tm.indices.is_empty() {
        anyhow::bail!("timemap empty");
    }

    let seed = parse_seed_hex_opt(a.map_seed, &a.map_seed_hex)?;

    let bf = read_bitfield_residual(&a.residual)?;

    // Resolve residual symbols from BF container
    let (bf_bits, bf_mapping, bf_orig_len_bytes, bf_symbol_count, resid_syms): (
        u8,
        BitMapping,
        usize,
        usize,
        Vec<u8>,
    ) = match bf {
        BitfieldResidual::Bf1 {
            bits_per_emission,
            mapping,
            orig_len_bytes,
            symbol_count,
            packed_symbols,
        } => {
            let syms = bitpack::unpack_symbols(bits_per_emission, &packed_symbols, symbol_count)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            (
                bits_per_emission,
                mapping,
                orig_len_bytes,
                symbol_count,
                syms,
            )
        }
        BitfieldResidual::Bf2 {
            bits_per_emission,
            mapping,
            orig_len_bytes,
            symbol_count,
            lane_count,
            lanes_raw_bitsets,
        } => {
            let mut out = vec![0u8; symbol_count];
            let mut seen = vec![false; symbol_count];

            for lane in 0..lane_count {
                let bs = &lanes_raw_bitsets[lane];
                for i in 0..symbol_count {
                    let byte = bs[i >> 3];
                    let bit = (byte >> (i & 7)) & 1;
                    if bit == 1 {
                        if seen[i] {
                            anyhow::bail!(
                                "BF2 invalid: symbol position {} set in multiple lanes",
                                i
                            );
                        }
                        out[i] = lane as u8;
                        seen[i] = true;
                    }
                }
            }

            if seen.iter().any(|&v| !v) {
                anyhow::bail!("BF2 invalid: some symbol positions not assigned to any lane");
            }

            (
                bits_per_emission,
                mapping,
                orig_len_bytes,
                symbol_count,
                out,
            )
        }
    };

    if bf_bits != a.bits_per_emission {
        anyhow::bail!(
            "bitfield residual bits_per_emission mismatch: file={} cli={}",
            bf_bits,
            a.bits_per_emission
        );
    }
    if bf_mapping != a.bit_mapping {
        anyhow::bail!(
            "bitfield residual mapping mismatch: file={:?} cli={:?}",
            bf_mapping,
            a.bit_mapping
        );
    }
    if tm.indices.len() != bf_symbol_count {
        anyhow::bail!(
            "timemap/residual symbol_count mismatch: tm={} resid_symbols={}",
            tm.indices.len(),
            bf_symbol_count
        );
    }

    let mut engine = Engine::new(recipe)?;

    let mut max_idx: u64 = 0;
    for &idx in tm.indices.iter() {
        if idx > max_idx {
            max_idx = idx;
        }
    }

    let mut out_syms: Vec<u8> = Vec::with_capacity(bf_symbol_count);
    let mut i: usize = 0;

    let mask = sym_mask(a.bits_per_emission);

    while engine.stats.ticks < a.max_ticks && (engine.stats.emissions as u64) <= max_idx {
        if let Some(tok) = engine.step() {
            let em = (engine.stats.emissions - 1) as u64;

            while i < tm.indices.len() && tm.indices[i] == em {
                let rgb6 = tok.to_rgb_pair().to_bytes();
                let pred =
                    map_symbol_bitfield(a.bit_mapping, seed, em, &rgb6, a.bits_per_emission) & mask;
                let sym = apply_residual_symbol(a.residual_mode, pred, resid_syms[i] & mask, mask);
                out_syms.push(sym);
                i += 1;
            }
        }
    }

    if i != tm.indices.len() {
        anyhow::bail!(
            "reconstruct short (bitfield): wrote {} of {} symbols (max_idx={} ticks={} emissions={})",
            i,
            tm.indices.len(),
            max_idx,
            engine.stats.ticks,
            engine.stats.emissions
        );
    }

    let mut out_bytes = bitpack::pack_symbols(a.bits_per_emission, &out_syms)
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    out_bytes.truncate(bf_orig_len_bytes);

    std::fs::write(&a.out, &out_bytes)
        .with_context(|| format!("write reconstruct out: {}", a.out))?;
    eprintln!(
        "reconstruct ok (bitfield): out={} bytes={} symbols={} bits_per_emission={} bit_mapping={:?} ticks={} emissions={} map_seed={} (0x{:016x})",
        a.out,
        out_bytes.len(),
        out_syms.len(),
        a.bits_per_emission,
        a.bit_mapping,
        engine.stats.ticks,
        engine.stats.emissions,
        seed,
        seed
    );

    Ok(())
}
