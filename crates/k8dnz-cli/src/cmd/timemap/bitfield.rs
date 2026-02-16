use super::args::*;
use super::residual::{apply_residual_symbol, make_residual_symbol, sym_mask};
use super::util::{parse_seed_hex_opt, tm_jump_cost, zstd_compress_len};

use anyhow::Context;

use k8dnz_core::signal::bitpack;
use k8dnz_core::signal::timing_map::TimingMap;
use k8dnz_core::Engine;

use crate::io::{recipe_file, timemap};

const BF1_MAGIC: &[u8; 4] = b"BF1\0";
const BF2_MAGIC: &[u8; 4] = b"BF2\0";

const BF1_FLAG_CHUNK_ADDK: u8 = 1u8 << 0;

fn zstd_compress(bytes: &[u8], level: i32) -> anyhow::Result<Vec<u8>> {
    zstd::encode_all(bytes, level).map_err(|e| anyhow::anyhow!("zstd compress: {e}"))
}

fn zstd_decompress_bytes(bytes: &[u8]) -> anyhow::Result<Vec<u8>> {
    zstd::decode_all(bytes).map_err(|e| anyhow::anyhow!("zstd decompress: {e}"))
}

#[derive(Clone, Debug)]
pub enum BitfieldResidual {
    Bf1 {
        bits_per_emission: u8,
        mapping: BitMapping,
        orig_len_bytes: usize,
        symbol_count: usize,
        chunk_size: Option<usize>,
        chunk_addk: Option<Vec<u8>>,
        packed_symbols: Vec<u8>,
    },
    Bf2 {
        bits_per_emission: u8,
        mapping: BitMapping,
        orig_len_bytes: usize,
        symbol_count: usize,
        lane_count: usize,
        lanes_raw_bitsets: Vec<Vec<u8>>,
    },
}

fn mapping_tag(m: BitMapping) -> u8 {
    match m {
        BitMapping::Geom => 0,
        BitMapping::Hash => 1,
        BitMapping::LowpassThresh => 2,
    }
}

fn mapping_from_tag(v: u8) -> anyhow::Result<BitMapping> {
    match v {
        0 => Ok(BitMapping::Geom),
        1 => Ok(BitMapping::Hash),
        2 => Ok(BitMapping::LowpassThresh),
        _ => anyhow::bail!("bitfield residual unknown mapping tag: {}", v),
    }
}

fn read_bitfield_residual(path: &str) -> anyhow::Result<BitfieldResidual> {
    let bytes = std::fs::read(path).with_context(|| format!("read bf: {}", path))?;

    if bytes.len() < 24 {
        anyhow::bail!("bitfield residual too small: {} bytes", bytes.len());
    }

    let magic = &bytes[0..4];

    if magic == BF1_MAGIC {
        let bits = bytes[4];
        let mapping_u8 = bytes[5];
        let mapping = mapping_from_tag(mapping_u8)?;

        let flags = bytes[6];
        let _reserved = bytes[7];

        let orig_len_bytes = u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as usize;
        let symbol_count = u64::from_le_bytes(bytes[16..24].try_into().unwrap()) as usize;

        let mut cursor = 24usize;

        let mut chunk_size: Option<usize> = None;
        let mut chunk_addk: Option<Vec<u8>> = None;

        if (flags & BF1_FLAG_CHUNK_ADDK) != 0 {
            if bytes.len() < cursor + 8 {
                anyhow::bail!("BF1 truncated reading chunk header");
            }
            let cs = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
            let cc =
                u32::from_le_bytes(bytes[cursor + 4..cursor + 8].try_into().unwrap()) as usize;
            cursor += 8;

            if cs == 0 {
                anyhow::bail!("BF1 invalid: chunk_size=0");
            }
            if bytes.len() < cursor + cc {
                anyhow::bail!("BF1 truncated reading chunk_addk");
            }
            let ks = bytes[cursor..cursor + cc].to_vec();
            cursor += cc;

            chunk_size = Some(cs);
            chunk_addk = Some(ks);
        }

        let payload = bytes[cursor..].to_vec();

        return Ok(BitfieldResidual::Bf1 {
            bits_per_emission: bits,
            mapping,
            orig_len_bytes,
            symbol_count,
            chunk_size,
            chunk_addk,
            packed_symbols: payload,
        });
    }

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
    chunk_size: Option<usize>,
    chunk_addk: Option<&[u8]>,
) -> anyhow::Result<Vec<u8>> {
    let packed = bitpack::pack_symbols(bits_per_emission, residual_symbols)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut flags: u8 = 0;
    let mut extra: Vec<u8> = Vec::new();

    if let (Some(cs), Some(ks)) = (chunk_size, chunk_addk) {
        if cs == 0 {
            anyhow::bail!("BF1: chunk_size must be > 0");
        }
        flags |= BF1_FLAG_CHUNK_ADDK;

        let cc = ks.len();
        extra.extend_from_slice(&(cs as u32).to_le_bytes());
        extra.extend_from_slice(&(cc as u32).to_le_bytes());
        extra.extend_from_slice(ks);
    }

    let mut out: Vec<u8> = Vec::with_capacity(24 + extra.len() + packed.len());
    out.extend_from_slice(BF1_MAGIC);
    out.push(bits_per_emission);
    out.push(mapping_tag(mapping));
    out.push(flags);
    out.push(0u8);
    out.extend_from_slice(&(orig_len_bytes as u64).to_le_bytes());
    out.extend_from_slice(&(residual_symbols.len() as u64).to_le_bytes());
    out.extend_from_slice(&extra);
    out.extend_from_slice(&packed);

    std::fs::write(path, &out).with_context(|| format!("write BF1 residual: {}", path))?;
    Ok(out)
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

    let mut lane_bitsets: Vec<Vec<u8>> = (0..lane_count).map(|_| vec![0u8; bitset_len]).collect();
    let mask = sym_mask(bits_per_emission);

    for (i, &s) in residual_symbols.iter().enumerate() {
        let lane = (s & mask) as usize;
        let byte_i = i >> 3;
        let bit_i = (i & 7) as u8;
        lane_bitsets[lane][byte_i] |= 1u8 << bit_i;
    }

    let mut lane_comp: Vec<Vec<u8>> = Vec::with_capacity(lane_count);
    for lane in lane_bitsets.iter() {
        lane_comp.push(zstd_compress(lane, zstd_level)?);
    }

    let mut out: Vec<u8> = Vec::new();
    out.extend_from_slice(BF2_MAGIC);
    out.push(bits_per_emission);
    out.push(mapping_tag(mapping));
    out.extend_from_slice(&[0u8, 0u8]);
    out.extend_from_slice(&(orig_len_bytes as u64).to_le_bytes());
    out.extend_from_slice(&(symbol_count as u64).to_le_bytes());
    out.extend_from_slice(&(lane_count as u32).to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());

    for c in lane_comp.iter() {
        out.extend_from_slice(&(c.len() as u32).to_le_bytes());
        out.extend_from_slice(c);
    }

    std::fs::write(path, &out).with_context(|| format!("write BF2 residual: {}", path))?;
    Ok(())
}

/// Wrapper used by gen-law to choose BF1 vs BF2 without duplicating container code.
pub(crate) fn write_bitfield_residual(
    path: &str,
    bits_per_emission: u8,
    mapping: BitMapping,
    orig_len_bytes: usize,
    residual_symbols: &[u8],
    zstd_level: i32,
    encoding: BitfieldResidualEncoding,
    chunk_size: Option<usize>,
    chunk_addk: Option<&[u8]>,
) -> anyhow::Result<usize> {
    match encoding {
        BitfieldResidualEncoding::Packed => {
            let bytes = write_bitfield_residual_bf1(
                path,
                bits_per_emission,
                mapping,
                orig_len_bytes,
                residual_symbols,
                chunk_size,
                chunk_addk,
            )?;
            Ok(bytes.len())
        }
        BitfieldResidualEncoding::Lanes => {
            write_bitfield_residual_bf2(
                path,
                bits_per_emission,
                mapping,
                orig_len_bytes,
                residual_symbols,
                zstd_level,
            )?;
            let n = std::fs::read(path).map(|b| b.len()).unwrap_or(0usize);
            Ok(n)
        }
    }
}

fn geom_symbol_from_rgb_msb_interleave(rgb6: &[u8; 6], bits_per_emission: u8) -> u8 {
    let bits = bits_per_emission as usize;
    let mask = sym_mask(bits_per_emission);

    let r = (((rgb6[0] as u16) + (rgb6[3] as u16)) >> 1) as u8;
    let g = (((rgb6[1] as u16) + (rgb6[4] as u16)) >> 1) as u8;
    let b = (((rgb6[2] as u16) + (rgb6[5] as u16)) >> 1) as u8;

    let chans = [r, g, b];

    let mut out: u8 = 0;
    for i in 0..bits {
        let chan = i % 3;
        let k = i / 3;
        let shift = 7usize.saturating_sub(k);
        let bit = (chans[chan] >> shift) & 1;
        out |= bit << (i as u8);
    }

    out & mask
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct LowpassState {
    y: u16,
}

impl LowpassState {
    pub(crate) fn new() -> Self {
        Self { y: 0 }
    }
}

fn intensity_u8_from_rgb6(rgb6: &[u8; 6]) -> u8 {
    let r = (((rgb6[0] as u16) + (rgb6[3] as u16)) >> 1) as u16;
    let g = (((rgb6[1] as u16) + (rgb6[4] as u16)) >> 1) as u16;
    let b = (((rgb6[2] as u16) + (rgb6[5] as u16)) >> 1) as u16;
    let y = (r + g + b) / 3;
    y as u8
}

fn lowpass_iir_update(state: &mut LowpassState, x: u8, smooth_shift: u8) -> u8 {
    if smooth_shift == 0 {
        state.y = x as u16;
        return x;
    }
    let sh = smooth_shift.min(15);
    let y = state.y as i32;
    let xi = x as i32;
    let diff = xi - y;
    let step = diff >> (sh as i32);
    let y2 = (y + step).clamp(0, 255);
    state.y = y2 as u16;
    state.y as u8
}

/// Exported for gen-law so we can map predicted symbols identically.
pub(crate) fn map_symbol_bitfield(
    mapping: BitMapping,
    map_seed: u64,
    emission: u64,
    rgb6: &[u8; 6],
    bits_per_emission: u8,
    bit_tau: u8,
    bit_smooth_shift: u8,
    lp_state: &mut LowpassState,
) -> u8 {
    let mask = sym_mask(bits_per_emission);

    match mapping {
        BitMapping::Geom => geom_symbol_from_rgb_msb_interleave(rgb6, bits_per_emission),
        BitMapping::Hash => {
            let mut x = map_seed ^ emission.rotate_left(17);
            for &b in rgb6.iter() {
                x ^= b as u64;
                x = x.wrapping_mul(0x9e3779b97f4a7c15);
                x ^= x >> 32;
            }
            (x as u8) & mask
        }
        BitMapping::LowpassThresh => {
            if bits_per_emission != 1 {
                return 0;
            }
            let x = intensity_u8_from_rgb6(rgb6);
            let y = lowpass_iir_update(lp_state, x, bit_smooth_shift);
            let bit = if y >= bit_tau { 1u8 } else { 0u8 };
            bit & mask
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
    bit_tau: u8,
    bit_smooth_shift: u8,
    lp_state: &mut LowpassState,
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
            let sym = map_symbol_bitfield(
                mapping,
                map_seed,
                em,
                &rgb6,
                bits_per_emission,
                bit_tau,
                bit_smooth_shift,
                lp_state,
            );
            stream_syms.push(sym);
        }
    }

    stream_syms.len() >= need_len
}

fn apply_chunk_addk(pred: u8, k: u8, mask: u8) -> u8 {
    pred.wrapping_add(k) & mask
}

fn proxy_cost_for_residual(resid_mode: ResidualMode, resid_sym: u8) -> usize {
    if resid_mode == ResidualMode::Xor {
        resid_sym.count_ones() as usize
    } else {
        if resid_sym == 0 { 0 } else { 1 }
    }
}

fn pack_bits01_to_u64(bits01: &[u8]) -> Vec<u64> {
    let n = bits01.len();
    let words = (n + 63) / 64;
    let mut out = vec![0u64; words + 1];
    for (i, &b) in bits01.iter().enumerate() {
        if (b & 1) != 0 {
            out[i >> 6] |= 1u64 << (i & 63);
        }
    }
    out
}

fn hamming01_aligned(target_words: &[u64], stream_words: &[u64], start_bit: usize, n_bits: usize) -> u32 {
    let word_shift = start_bit >> 6;
    let bit_shift = start_bit & 63;

    let full_words = n_bits >> 6;
    let tail_bits = n_bits & 63;

    let mut acc: u32 = 0;

    for i in 0..full_words {
        let sw = if bit_shift == 0 {
            stream_words[word_shift + i]
        } else {
            let lo = stream_words[word_shift + i] >> bit_shift;
            let hi = stream_words[word_shift + i + 1] << (64 - bit_shift);
            lo | hi
        };
        acc = acc.saturating_add((target_words[i] ^ sw).count_ones());
    }

    if tail_bits != 0 {
        let i = full_words;
        let sw = if bit_shift == 0 {
            stream_words[word_shift + i]
        } else {
            let lo = stream_words[word_shift + i] >> bit_shift;
            let hi = stream_words[word_shift + i + 1] << (64 - bit_shift);
            lo | hi
        };
        let mask = if tail_bits == 64 {
            !0u64
        } else {
            (1u64 << tail_bits) - 1
        };
        acc = acc.saturating_add(((target_words[i] ^ sw) & mask).count_ones());
    }

    acc
}

pub fn cmd_fit_xor_chunked_bitfield(a: FitXorChunkedArgs) -> anyhow::Result<()> {
    if a.mode != ApplyMode::Rgbpair {
        anyhow::bail!("--map bitfield requires --mode rgbpair");
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
    let bits: usize = a.bits_per_emission as usize;
    let sym_count: usize = (bit_len + bits - 1) / bits;

    let total_bits: usize = sym_count.saturating_mul(bits);
    let need_bytes: usize = (total_bits + 7) / 8;
    let mut padded_target: Vec<u8> = vec![0u8; need_bytes];
    padded_target[..target_bytes.len()].copy_from_slice(&target_bytes);

    let target_syms = bitpack::unpack_symbols(a.bits_per_emission, &padded_target, sym_count)
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

    let mut lp_state = LowpassState::new();

    while (engine.stats.emissions as u64) < a.search_emissions && engine.stats.ticks < a.max_ticks {
        if let Some(tok) = engine.step() {
            let em = (engine.stats.emissions - 1) as u64;
            let rgb6 = tok.to_rgb_pair().to_bytes();
            let sym = map_symbol_bitfield(
                a.bit_mapping,
                seed,
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

    let abs_stream_base_pos: u64 = a.start_emission;
    let total_n = target_syms.len();

    let mut tm_indices: Vec<u64> = Vec::with_capacity(total_n.min(stream_syms.len()));
    let mut residual_syms: Vec<u8> = Vec::with_capacity(total_n.min(stream_syms.len()));

    let mut chunk_addk: Vec<u8> = Vec::new();

    eprintln!(
        "--- fit-xor-chunked (bitfield) --- map=bitfield bits_per_emission={} bit_mapping={:?} map_seed={} (0x{:016x}) bit_tau={} bit_smooth_shift={} residual={:?} objective={:?} refine_topk={} lookahead={} trans_penalty={} chunk_size={} scan_step={} zstd_level={} chunk_xform={:?} target_bytes={} target_symbols={} stream_symbols={} base_pos={} start_emission={} end_emissions={} ticks={} delta_ticks={}",
        a.bits_per_emission,
        a.bit_mapping,
        seed,
        seed,
        a.bit_tau,
        a.bit_smooth_shift,
        a.residual,
        a.objective,
        a.refine_topk,
        a.lookahead,
        a.trans_penalty,
        a.chunk_size,
        a.scan_step,
        a.zstd_level,
        a.chunk_xform,
        target_bytes.len(),
        total_n,
        stream_syms.len(),
        abs_stream_base_pos,
        a.start_emission,
        (start_em + (stream_syms.len() as u64)),
        engine.stats.ticks,
        engine.stats.ticks.saturating_sub(start_ticks),
    );

    let want_addk = a.chunk_xform == ChunkXform::Addk;

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
                a.bit_tau,
                a.bit_smooth_shift,
                &mut lp_state,
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

        let mut best_start: usize = min_start;
        let mut best_matches: u64 = 0;
        let mut best_score: usize = usize::MAX;
        let mut best_resid_metric: usize = usize::MAX;
        let mut best_k: u8 = 0;
        let mut scanned: u64 = 0;

        let fast01 = a.bits_per_emission == 1
            && a.residual == ResidualMode::Xor
            && a.objective == FitObjective::Zstd
            && a.bit_mapping != BitMapping::LowpassThresh;

        if fast01 {
            let target_slice = &target_syms[off..off + n];
            let stream_slice = &stream_syms;

            let target_words = pack_bits01_to_u64(target_slice);
            let stream_words = pack_bits01_to_u64(stream_slice);

            let mut s0: usize = min_start;
            while s0 <= max_start {
                scanned += 1;

                let base_pos = abs_stream_base_pos + (s0 as u64);
                let jump_cost_raw = tm_jump_cost(prev_pos, base_pos) as u64;
                let jump_cost_u64 = jump_cost_raw.saturating_mul(a.trans_penalty);
                let jump_cost = jump_cost_u64 as usize;

                let d0 = hamming01_aligned(&target_words, &stream_words, s0, n) as usize;

                if want_addk {
                    let d1 = n.saturating_sub(d0);
                    let (d, k) = if d0 <= d1 { (d0, 0u8) } else { (d1, 1u8) };
                    let score = d.saturating_add(jump_cost);
                    if score < best_score || (score == best_score && s0 < best_start) {
                        best_score = score;
                        best_start = s0;
                        best_k = k;
                        best_matches = (n - d) as u64;
                        best_resid_metric = d;
                    }
                } else {
                    let score = d0.saturating_add(jump_cost);
                    if score < best_score || (score == best_score && s0 < best_start) {
                        best_score = score;
                        best_start = s0;
                        best_k = 0;
                        best_matches = (n - d0) as u64;
                        best_resid_metric = d0;
                    }
                }

                s0 = s0.saturating_add(a.scan_step);
            }
        } else {
            let mut scratch_resid: Vec<u8> = vec![0u8; n];

            let mut refine: Vec<(usize, usize, u64)> = Vec::new();

            let mut s0: usize = min_start;
            while s0 <= max_start {
                scanned += 1;

                let base_pos = abs_stream_base_pos + (s0 as u64);

                let mut matches: u64 = 0;
                let mut proxy_cost: usize = 0;

                for i in 0..n {
                    let pred0 = stream_syms[s0 + i] & mask;
                    let resid_b =
                        make_residual_symbol(a.residual, pred0, target_syms[off + i] & mask, mask);
                    scratch_resid[i] = resid_b;
                    if resid_b == 0 {
                        matches += 1;
                    }
                    proxy_cost = proxy_cost
                        .saturating_add(proxy_cost_for_residual(a.residual, resid_b));
                }

                let jump_cost_raw = tm_jump_cost(prev_pos, base_pos) as u64;
                let jump_cost_u64 = jump_cost_raw.saturating_mul(a.trans_penalty);
                let jump_cost = jump_cost_u64 as usize;

                if a.objective == FitObjective::Zstd {
                    refine.push((proxy_cost.saturating_add(jump_cost), s0, matches));
                } else {
                    let score = proxy_cost.saturating_add(jump_cost);
                    if score < best_score || (score == best_score && s0 < best_start) {
                        best_score = score;
                        best_start = s0;
                        best_matches = matches;
                        best_resid_metric = proxy_cost;
                    }
                    if a.refine_topk != 0 {
                        refine.push((score, s0, matches));
                    }
                }

                s0 = s0.saturating_add(a.scan_step);
            }

            if a.objective == FitObjective::Zstd {
                refine.sort_by(|a1, b1| a1.0.cmp(&b1.0).then_with(|| a1.1.cmp(&b1.1)));

                let mut topk = a.refine_topk;
                if topk == 0 {
                    topk = 256;
                }
                if refine.len() > topk {
                    refine.truncate(topk);
                }

                for &(_proxy_score, cand_s, _cand_matches) in refine.iter() {
                    let base_pos = abs_stream_base_pos + (cand_s as u64);

                    let jump_cost_raw = tm_jump_cost(prev_pos, base_pos) as u64;
                    let jump_cost_u64 = jump_cost_raw.saturating_mul(a.trans_penalty);
                    let jump_cost = jump_cost_u64 as usize;

                    if want_addk {
                        let alpha = 1usize << (a.bits_per_emission as usize);

                        let mut counts: Vec<u32> = vec![0u32; alpha];
                        for i in 0..n {
                            let pred0 = stream_syms[cand_s + i] & mask;
                            let targ = target_syms[off + i] & mask;
                            let ksym = targ.wrapping_sub(pred0) & mask;
                            counts[ksym as usize] = counts[ksym as usize].saturating_add(1);
                        }

                        let mut ks: Vec<(u32, u8)> =
                            (0..alpha).map(|k| (counts[k], k as u8)).collect();
                        ks.sort_by(|a1, b1| b1.0.cmp(&a1.0).then_with(|| a1.1.cmp(&b1.1)));
                        if ks.len() > 4 {
                            ks.truncate(4);
                        }

                        for &(_cnt, kk) in ks.iter() {
                            let mut matches: u64 = 0;
                            for i in 0..n {
                                let pred0 = stream_syms[cand_s + i] & mask;
                                let pred = apply_chunk_addk(pred0, kk, mask);
                                let resid = make_residual_symbol(
                                    a.residual,
                                    pred,
                                    target_syms[off + i] & mask,
                                    mask,
                                );
                                scratch_resid[i] = resid;
                                if resid == 0 {
                                    matches += 1;
                                }
                            }

                            let zlen = zstd_compress_len(&scratch_resid, a.zstd_level);
                            let score = zlen.saturating_add(jump_cost);

                            if score < best_score || (score == best_score && cand_s < best_start) {
                                best_score = score;
                                best_start = cand_s;
                                best_matches = matches;
                                best_resid_metric = zlen;
                                best_k = kk;
                            }
                        }
                    } else {
                        let mut matches: u64 = 0;
                        for i in 0..n {
                            let pred0 = stream_syms[cand_s + i] & mask;
                            let resid = make_residual_symbol(
                                a.residual,
                                pred0,
                                target_syms[off + i] & mask,
                                mask,
                            );
                            scratch_resid[i] = resid;
                            if resid == 0 {
                                matches += 1;
                            }
                        }

                        let zlen = zstd_compress_len(&scratch_resid, a.zstd_level);
                        let score = zlen.saturating_add(jump_cost);

                        if score < best_score || (score == best_score && cand_s < best_start) {
                            best_score = score;
                            best_start = cand_s;
                            best_matches = matches;
                            best_resid_metric = zlen;
                            best_k = 0;
                        }
                    }
                }
            }
        }

        let base_pos = abs_stream_base_pos + (best_start as u64);

        for i in 0..n {
            let pos = base_pos + (i as u64);
            tm_indices.push(pos);

            let pred0 = stream_syms[best_start + i] & mask;
            let pred = if want_addk {
                apply_chunk_addk(pred0, best_k, mask)
            } else {
                pred0
            };

            residual_syms.push(make_residual_symbol(
                a.residual,
                pred,
                target_syms[off + i] & mask,
                mask,
            ));
        }

        if want_addk {
            chunk_addk.push(best_k);
        }

        prev_pos = Some(base_pos + (n as u64) - 1);

        if want_addk {
            eprintln!(
                "chunk {:04} off_sym={} len_sym={} start_emission={} scanned_windows={} matches={}/{} ({:.2}%) chunk_score={} chunk_resid_metric={} addk={}",
                chunk_idx,
                off,
                n,
                base_pos,
                scanned,
                best_matches,
                n,
                (best_matches as f64) * 100.0 / (n as f64),
                best_score,
                best_resid_metric,
                best_k
            );
        } else {
            eprintln!(
                "chunk {:04} off_sym={} len_sym={} start_emission={} scanned_windows={} matches={}/{} ({:.2}%) chunk_score={} chunk_resid_metric={}",
                chunk_idx,
                off,
                n,
                base_pos,
                scanned,
                best_matches,
                n,
                (best_matches as f64) * 100.0 / (n as f64),
                best_score,
                best_resid_metric
            );
        }

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

    let tm = TimingMap { indices: tm_indices };

    let tm_bytes = tm.encode_tm1();
    let tm_raw = tm_bytes.len();
    let tm_zstd = zstd_compress_len(&tm_bytes, a.zstd_level);

    let want_lanes = a.time_split || a.bitfield_residual == BitfieldResidualEncoding::Lanes;

    let (resid_raw, resid_zstd) = if !want_lanes {
        let file_bytes = write_bitfield_residual_bf1(
            &a.out_residual,
            a.bits_per_emission,
            a.bit_mapping,
            target_bytes.len(),
            &residual_syms,
            if want_addk { Some(a.chunk_size) } else { None },
            if want_addk {
                Some(chunk_addk.as_slice())
            } else {
                None
            },
        )?;
        let resid_raw = file_bytes.len();
        let resid_zstd = zstd_compress_len(&file_bytes, a.zstd_level);
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
    if a.bit_mapping == BitMapping::LowpassThresh && a.bits_per_emission != 1 {
        anyhow::bail!("bit-mapping lowpass-thresh requires --bits-per-emission 1");
    }

    let recipe = recipe_file::load_k8r(&a.recipe)?;
    let tm = timemap::read_tm1(&a.timemap)?;
    if tm.indices.is_empty() {
        anyhow::bail!("timemap empty");
    }

    let seed = parse_seed_hex_opt(a.map_seed, &a.map_seed_hex)?;

    let bf = read_bitfield_residual(&a.residual)?;

    let (bf_bits, bf_mapping, bf_orig_len_bytes, bf_symbol_count, bf_chunk_size, bf_chunk_addk, resid_syms): (
        u8,
        BitMapping,
        usize,
        usize,
        Option<usize>,
        Option<Vec<u8>>,
        Vec<u8>,
    ) = match bf {
        BitfieldResidual::Bf1 {
            bits_per_emission,
            mapping,
            orig_len_bytes,
            symbol_count,
            chunk_size,
            chunk_addk,
            packed_symbols,
        } => {
            let syms = bitpack::unpack_symbols(bits_per_emission, &packed_symbols, symbol_count)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            (
                bits_per_emission,
                mapping,
                orig_len_bytes,
                symbol_count,
                chunk_size,
                chunk_addk,
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
                None,
                None,
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

    if let (Some(cs), Some(ref ks)) = (bf_chunk_size, bf_chunk_addk.as_ref()) {
        let need_chunks = (bf_symbol_count + cs - 1) / cs;
        if ks.len() != need_chunks {
            anyhow::bail!(
                "BF1 chunk_addk len mismatch: got {} want {} (symbols={} chunk_size={})",
                ks.len(),
                need_chunks,
                bf_symbol_count,
                cs
            );
        }
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

    let mut lp_state = LowpassState::new();

    while engine.stats.ticks < a.max_ticks && (engine.stats.emissions as u64) <= max_idx {
        if let Some(tok) = engine.step() {
            let em = (engine.stats.emissions - 1) as u64;

            while i < tm.indices.len() && tm.indices[i] == em {
                let rgb6 = tok.to_rgb_pair().to_bytes();
                let pred0 = map_symbol_bitfield(
                    a.bit_mapping,
                    seed,
                    em,
                    &rgb6,
                    a.bits_per_emission,
                    a.bit_tau,
                    a.bit_smooth_shift,
                    &mut lp_state,
                ) & mask;

                let pred = if let (Some(cs), Some(ref ks)) = (bf_chunk_size, bf_chunk_addk.as_ref()) {
                    let ci = i / cs;
                    apply_chunk_addk(pred0, ks[ci], mask)
                } else {
                    pred0
                };

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
        "reconstruct ok (bitfield): out={} bytes={} symbols={} bits_per_emission={} bit_mapping={:?} bit_tau={} bit_smooth_shift={} ticks={} emissions={} map_seed={} (0x{:016x})",
        a.out,
        out_bytes.len(),
        out_syms.len(),
        a.bits_per_emission,
        a.bit_mapping,
        a.bit_tau,
        a.bit_smooth_shift,
        engine.stats.ticks,
        engine.stats.emissions,
        seed,
        seed
    );

    Ok(())
}
