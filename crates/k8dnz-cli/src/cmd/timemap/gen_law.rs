use super::args::*;
use super::bitfield::{map_symbol_bitfield, write_bitfield_residual, LowpassState};
use super::residual::{make_residual_symbol, sym_mask};
use super::util::{parse_seed_hex_opt, splitmix64, zstd_compress_len};

use anyhow::Context;

use k8dnz_core::signal::bitpack;
use k8dnz_core::signal::timing_map::TimingMap;
use k8dnz_core::Engine;

use crate::io::{recipe_file, timemap};

fn circ_dist_turn32(a: u32, b: u32) -> u32 {
    let d1 = a.wrapping_sub(b);
    let d2 = b.wrapping_sub(a);
    if d1 < d2 { d1 } else { d2 }
}

fn law_jump_u64(
    law_seed: u64,
    i: u64,
    phase_a: u32,
    phase_c: u32,
    phase_l: u32,
    dist: u32,
    locked: bool,
    pitch: u32,
    max_jump: u32,
    lock_div: u32,
) -> u64 {
    let mj = if max_jump == 0 { 1u32 } else { max_jump };
    let div = if lock_div == 0 { 1u32 } else { lock_div };
    let mj_eff = if locked { (mj / div).max(1) } else { mj };

    let x0 = law_seed
        ^ i.wrapping_mul(0x9E3779B97F4A7C15)
        ^ ((phase_a as u64) << 32)
        ^ (phase_c as u64)
        ^ ((phase_l as u64) << 1)
        ^ ((dist as u64) << 17)
        ^ ((pitch as u64) << 47)
        ^ (locked as u64);

    let x = splitmix64(x0);
    1u64 + (x % (mj_eff as u64))
}

fn chunk_k_from_law(law_seed: u64, chunk_index: u64, mask: u8) -> u8 {
    (splitmix64(law_seed ^ (chunk_index.wrapping_mul(0xD6E8FEB86659FD93))) as u8) & mask
}

fn apply_chunk_addk(pred: u8, k: u8, mask: u8) -> u8 {
    pred.wrapping_add(k) & mask
}

fn tri_wave_i64(k: u64, period: u64, phi: u64) -> i64 {
    if period == 0 {
        return 0;
    }
    let u = (k.wrapping_add(phi)) % period;
    let half = period / 2;
    if half == 0 {
        return 0;
    }
    let r = if u < half { u } else { period - u };
    let v = (r as i64) - (half as i64 / 2);
    v
}

fn closed_form_start_offset(
    k: u64,
    window_len: u64,
    b: i64,
    a: i64,
    c: i64,
    p1: u64,
    g1: i64,
    phi1: u64,
    p2: u64,
    g2: i64,
    phi2: u64,
) -> usize {
    if window_len == 0 {
        return 0;
    }

    let kk = k as i64;

    let tri1 = if p1 != 0 && g1 != 0 {
        g1.saturating_mul(tri_wave_i64(k, p1, phi1))
    } else {
        0
    };

    let tri2 = if p2 != 0 && g2 != 0 {
        g2.saturating_mul(tri_wave_i64(k, p2, phi2))
    } else {
        0
    };

    let quad = c
        .saturating_mul(kk.saturating_mul(kk.saturating_sub(1)) / 2);

    let raw = b
        .saturating_add(a.saturating_mul(kk))
        .saturating_add(quad)
        .saturating_add(tri1)
        .saturating_add(tri2);

    let wl = window_len as i64;
    let mut m = raw % wl;
    if m < 0 {
        m += wl;
    }
    (m as u64 % window_len) as usize
}

pub fn cmd_gen_law(a: GenLawArgs) -> anyhow::Result<()> {
    if a.map != MapMode::Bitfield {
        anyhow::bail!("timemap gen-law currently implemented only for --map bitfield");
    }
    if a.mode != ApplyMode::Rgbpair {
        anyhow::bail!("timemap gen-law (bitfield) requires --mode rgbpair");
    }
    if a.bits_per_emission == 0 || a.bits_per_emission > 8 {
        anyhow::bail!("--bits-per-emission must be in 1..=8");
    }
    if a.bit_mapping == BitMapping::LowpassThresh && a.bits_per_emission != 1 {
        anyhow::bail!("bit-mapping lowpass-thresh requires --bits-per-emission 1");
    }
    if a.chunk_size == 0 {
        anyhow::bail!("--chunk-size must be >= 1");
    }
    if a.search_emissions == 0 {
        anyhow::bail!("--search-emissions must be >= 1");
    }
    if a.law_type == LawType::JumpWalk && a.law_max_jump == 0 {
        anyhow::bail!("--law-max-jump must be >= 1 (jump-walk)");
    }

    let recipe = recipe_file::load_k8r(&a.recipe)?;
    let recipe_raw_len = std::fs::read(&a.recipe).map(|b| b.len()).unwrap_or(0usize);

    let target_bytes =
        std::fs::read(&a.target).with_context(|| format!("read target: {}", a.target))?;
    if target_bytes.is_empty() {
        anyhow::bail!("target is empty");
    }

    let map_seed = parse_seed_hex_opt(a.map_seed, &a.map_seed_hex)?;
    let law_seed = parse_seed_hex_opt(a.law_seed, &a.law_seed_hex)?;

    let bit_len: usize = target_bytes.len() * 8;
    let bits: usize = a.bits_per_emission as usize;
    let sym_count: usize = (bit_len + bits - 1) / bits;

    let total_bits: usize = sym_count.saturating_mul(bits);
    let need_bytes: usize = (total_bits + 7) / 8;
    let mut padded_target: Vec<u8> = vec![0u8; need_bytes];
    padded_target[..target_bytes.len()].copy_from_slice(&target_bytes);

    let target_syms = bitpack::unpack_symbols(a.bits_per_emission, &padded_target, sym_count)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let mask = sym_mask(a.bits_per_emission);

    if a.start_emission >= a.search_emissions {
        anyhow::bail!(
            "--start-emission {} must be < --search-emissions {}",
            a.start_emission,
            a.search_emissions
        );
    }

    let mut engine = Engine::new(recipe)?;

    while (engine.stats.emissions as u64) < a.start_emission && engine.stats.ticks < a.max_ticks {
        let _ = engine.step();
    }

    let base_emission: u64 = engine.stats.emissions as u64;

    if base_emission != a.start_emission {
        anyhow::bail!(
            "gen-law: could not reach --start-emission within --max-ticks: want={} got={} (ticks={} emissions={})",
            a.start_emission,
            base_emission,
            engine.stats.ticks,
            engine.stats.emissions
        );
    }

    let mut stream_syms: Vec<u8> = Vec::new();
    stream_syms.reserve(300_000);

    let mut lp_state = LowpassState::new();

    while (engine.stats.emissions as u64) < a.search_emissions && engine.stats.ticks < a.max_ticks {
        if let Some(tok) = engine.step() {
            let em = (engine.stats.emissions - 1) as u64;
            let rgb6 = tok.to_rgb_pair().to_bytes();
            let sym = map_symbol_bitfield(
                a.bit_mapping,
                map_seed,
                em,
                &rgb6,
                a.bits_per_emission,
                a.bit_tau,
                a.bit_smooth_shift,
                &mut lp_state,
            );
            stream_syms.push(sym & mask);
        }
    }

    let produced_emissions_end_excl: u64 = base_emission + (stream_syms.len() as u64);
    if stream_syms.len() < sym_count {
        anyhow::bail!(
            "gen-law: not enough emissions produced under --max-ticks to cover target symbols: have={} need={} (ticks={} emissions_total={} cap_emissions={} tip: increase --max-ticks or reduce target or use a recipe that emits more densely)",
            stream_syms.len(),
            sym_count,
            engine.stats.ticks,
            engine.stats.emissions,
            a.search_emissions
        );
    }

    let window_len: u64 = (stream_syms.len() - sym_count + 1) as u64;

    let use_addk = a.chunk_xform == ChunkXform::Addk;
    let chunks = (sym_count + a.chunk_size - 1) / a.chunk_size;

    let mut chunk_addk: Vec<u8> = Vec::new();
    if use_addk {
        chunk_addk.reserve(chunks);
        for c in 0..(chunks as u64) {
            chunk_addk.push(chunk_k_from_law(law_seed, c, mask));
        }
    }

    let mut tm_indices: Vec<u64> = Vec::with_capacity(sym_count);
    let mut residual_syms: Vec<u8> = Vec::with_capacity(sym_count);
    let mut matches: usize = 0;

    match a.law_type {
        LawType::JumpWalk => {
            let mut phase_a: u32 = (splitmix64(law_seed ^ 0xA1A2A3A4A5A6A7A8) >> 32) as u32;
            let mut phase_c: u32 = (splitmix64(law_seed ^ 0xC1C2C3C4C5C6C7C8) >> 32) as u32;
            let mut phase_l: u32 = (splitmix64(law_seed ^ 0x9E3779B97F4A7C15) >> 32) as u32;

            let mut offset_total: u64 = 0;
            let sym_u64 = sym_count as u64;

            for i in 0..sym_u64 {
                phase_a = phase_a.wrapping_add(a.law_v_a);
                phase_c = phase_c.wrapping_sub(a.law_v_c);

                let dist = circ_dist_turn32(phase_a, phase_c);
                let locked = dist <= a.law_epsilon;

                if locked {
                    phase_l = phase_a.wrapping_add(a.law_delta);
                    phase_l = phase_l.wrapping_add(a.law_v_l);
                } else {
                    phase_l = phase_l.wrapping_add(a.law_v_l ^ a.law_pitch);
                }

                let jump = law_jump_u64(
                    law_seed,
                    i,
                    phase_a,
                    phase_c,
                    phase_l,
                    dist,
                    locked,
                    a.law_pitch,
                    a.law_max_jump,
                    a.law_lock_div,
                );
                offset_total = offset_total.wrapping_add(jump);
            }

            let start_offset: usize = (offset_total % window_len) as usize;
            let start_pos: u64 = base_emission + (start_offset as u64);

            for i in 0..sym_count {
                let em = start_pos + (i as u64);
                tm_indices.push(em);

                let mut pred = stream_syms[start_offset + i] & mask;
                if use_addk {
                    let ci = i / a.chunk_size;
                    pred = apply_chunk_addk(pred, chunk_addk[ci] & mask, mask);
                }

                let plain = target_syms[i] & mask;
                let resid = make_residual_symbol(a.residual, pred, plain, mask);
                if resid == 0 { matches += 1; }
                residual_syms.push(resid & mask);
            }

            let tm = TimingMap { indices: tm_indices };
            timemap::write_tm1(&a.out_timemap, &tm)?;

            let enc = if a.time_split { BitfieldResidualEncoding::Lanes } else { a.bitfield_residual };

            let resid_container_bytes = write_bitfield_residual(
                &a.out_residual,
                a.bits_per_emission,
                a.bit_mapping,
                target_bytes.len(),
                &residual_syms,
                a.zstd_level,
                enc,
                if use_addk { Some(a.chunk_size) } else { None },
                if use_addk { Some(chunk_addk.as_slice()) } else { None },
            )?;

            let tm_file_bytes = std::fs::read(&a.out_timemap).unwrap_or_default();
            let tm_zstd = zstd_compress_len(&tm_file_bytes, a.zstd_level);

            let packed_resid = bitpack::pack_symbols(a.bits_per_emission, &residual_syms)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            let resid_zstd = zstd_compress_len(&packed_resid, a.zstd_level);

            let plain_packed = bitpack::pack_symbols(a.bits_per_emission, &target_syms)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            let plain_zstd = zstd_compress_len(&plain_packed, a.zstd_level);

            let effective_no_recipe = tm_zstd.saturating_add(resid_zstd);
            let effective_with_recipe = recipe_raw_len.saturating_add(effective_no_recipe);

            eprintln!("--- gen-law (bitfield) ---");
            eprintln!("law_type                   = {:?}", a.law_type);
            eprintln!("target_bytes               = {}", target_bytes.len());
            eprintln!("symbols                    = {}", sym_count);
            eprintln!("chunk_size                 = {}", a.chunk_size);
            eprintln!("chunks                     = {}", chunks);
            eprintln!("bits_per_emission          = {}", a.bits_per_emission);
            eprintln!("bit_mapping                = {:?}", a.bit_mapping);
            eprintln!("bit_tau                    = {}", a.bit_tau);
            eprintln!("bit_smooth_shift           = {}", a.bit_smooth_shift);
            eprintln!("map_seed                   = {} (0x{:016x})", map_seed, map_seed);
            eprintln!("law_seed                   = {} (0x{:016x})", law_seed, law_seed);
            eprintln!("start_emission(base)       = {}", base_emission);
            eprintln!("search_emissions_cap       = {}", a.search_emissions);
            eprintln!("produced_emissions_end     = {}", produced_emissions_end_excl);
            eprintln!("max_ticks                  = {}", a.max_ticks);
            eprintln!("window_len(starts)         = {}", window_len);
            eprintln!("offset_total               = {}", offset_total);
            eprintln!("start_offset               = {}", start_offset);
            eprintln!("start_pos(emission)        = {}", start_pos);
            eprintln!(
                "matches                    = {}/{} ({:.2}%)",
                matches,
                sym_count,
                (matches as f64) * 100.0 / (sym_count as f64)
            );
            eprintln!("recipe_raw_bytes           = {}", recipe_raw_len);
            eprintln!("plain_zstd_bytes           = {}", plain_zstd);
            eprintln!("tm1_zstd_bytes             = {}", tm_zstd);
            eprintln!("residual_container_bytes   = {}", resid_container_bytes);
            eprintln!("residual_packed_zstd_bytes = {}", resid_zstd);
            eprintln!("effective_no_recipe        = {}", effective_no_recipe);
            eprintln!("effective_with_recipe      = {}", effective_with_recipe);
            eprintln!(
                "delta_vs_plain_zstd_no_recipe   = {}",
                (effective_no_recipe as i64) - (plain_zstd as i64)
            );
            eprintln!(
                "delta_vs_plain_zstd_with_recipe = {}",
                (effective_with_recipe as i64) - (plain_zstd as i64)
            );

            Ok(())
        }

        LawType::ClosedForm => {
            for k in 0..(chunks as u64) {
                let start_offset = closed_form_start_offset(
                    k,
                    window_len,
                    a.law_cf_b,
                    a.law_cf_a,
                    a.law_cf_c,
                    a.law_cf_p1,
                    a.law_cf_g1,
                    a.law_cf_phi1,
                    a.law_cf_p2,
                    a.law_cf_g2,
                    a.law_cf_phi2,
                );

                let chunk_off = (k as usize) * a.chunk_size;
                let remaining = sym_count.saturating_sub(chunk_off);
                let n = remaining.min(a.chunk_size);

                let start_pos = base_emission + (start_offset as u64);

                for i in 0..n {
                    let gi = chunk_off + i;
                    let em = start_pos + (i as u64);
                    tm_indices.push(em);

                    let mut pred = stream_syms[start_offset + i] & mask;
                    if use_addk {
                        pred = apply_chunk_addk(pred, chunk_addk[k as usize] & mask, mask);
                    }

                    let plain = target_syms[gi] & mask;
                    let resid = make_residual_symbol(a.residual, pred, plain, mask);
                    if resid == 0 { matches += 1; }
                    residual_syms.push(resid & mask);
                }
            }

            let tm = TimingMap { indices: tm_indices };
            timemap::write_tm1(&a.out_timemap, &tm)?;

            let enc = if a.time_split { BitfieldResidualEncoding::Lanes } else { a.bitfield_residual };

            let resid_container_bytes = write_bitfield_residual(
                &a.out_residual,
                a.bits_per_emission,
                a.bit_mapping,
                target_bytes.len(),
                &residual_syms,
                a.zstd_level,
                enc,
                if use_addk { Some(a.chunk_size) } else { None },
                if use_addk { Some(chunk_addk.as_slice()) } else { None },
            )?;

            let tm_file_bytes = std::fs::read(&a.out_timemap).unwrap_or_default();
            let tm_zstd = zstd_compress_len(&tm_file_bytes, a.zstd_level);

            let packed_resid = bitpack::pack_symbols(a.bits_per_emission, &residual_syms)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            let resid_zstd = zstd_compress_len(&packed_resid, a.zstd_level);

            let plain_packed = bitpack::pack_symbols(a.bits_per_emission, &target_syms)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            let plain_zstd = zstd_compress_len(&plain_packed, a.zstd_level);

            let effective_no_recipe = tm_zstd.saturating_add(resid_zstd);
            let effective_with_recipe = recipe_raw_len.saturating_add(effective_no_recipe);

            eprintln!("--- gen-law (bitfield) ---");
            eprintln!("law_type                   = {:?}", a.law_type);
            eprintln!("target_bytes               = {}", target_bytes.len());
            eprintln!("symbols                    = {}", sym_count);
            eprintln!("chunk_size                 = {}", a.chunk_size);
            eprintln!("chunks                     = {}", chunks);
            eprintln!("bits_per_emission          = {}", a.bits_per_emission);
            eprintln!("bit_mapping                = {:?}", a.bit_mapping);
            eprintln!("bit_tau                    = {}", a.bit_tau);
            eprintln!("bit_smooth_shift           = {}", a.bit_smooth_shift);
            eprintln!("map_seed                   = {} (0x{:016x})", map_seed, map_seed);
            eprintln!("law_seed                   = {} (0x{:016x})", law_seed, law_seed);
            eprintln!("start_emission(base)       = {}", base_emission);
            eprintln!("search_emissions_cap       = {}", a.search_emissions);
            eprintln!("produced_emissions_end     = {}", produced_emissions_end_excl);
            eprintln!("max_ticks                  = {}", a.max_ticks);
            eprintln!("window_len(typical)        = {}", window_len);
            eprintln!("cf_b                       = {}", a.law_cf_b);
            eprintln!("cf_a                       = {}", a.law_cf_a);
            eprintln!("cf_c                       = {}", a.law_cf_c);
            eprintln!(
                "cf_p1/cf_g1/cf_phi1         = {}/{}/{}",
                a.law_cf_p1, a.law_cf_g1, a.law_cf_phi1
            );
            eprintln!(
                "cf_p2/cf_g2/cf_phi2         = {}/{}/{}",
                a.law_cf_p2, a.law_cf_g2, a.law_cf_phi2
            );
            eprintln!(
                "matches                    = {}/{} ({:.2}%)",
                matches,
                sym_count,
                (matches as f64) * 100.0 / (sym_count as f64)
            );
            eprintln!("recipe_raw_bytes           = {}", recipe_raw_len);
            eprintln!("plain_zstd_bytes           = {}", plain_zstd);
            eprintln!("tm1_zstd_bytes             = {}", tm_zstd);
            eprintln!("residual_container_bytes   = {}", resid_container_bytes);
            eprintln!("residual_packed_zstd_bytes = {}", resid_zstd);
            eprintln!("effective_no_recipe        = {}", effective_no_recipe);
            eprintln!("effective_with_recipe      = {}", effective_with_recipe);
            eprintln!(
                "delta_vs_plain_zstd_no_recipe   = {}",
                (effective_no_recipe as i64) - (plain_zstd as i64)
            );
            eprintln!(
                "delta_vs_plain_zstd_with_recipe = {}",
                (effective_with_recipe as i64) - (plain_zstd as i64)
            );

            Ok(())
        }
    }
}
