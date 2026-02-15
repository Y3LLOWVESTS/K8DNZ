// crates/k8dnz-cli/src/cmd/timemap/bf_lanes.rs
//
// BF1/BF2 lane analysis (time-split):
// - Parses BF1 or BF2 residual files
// - BF1: unpacks symbols -> builds lane bitsets -> reports raw + zstd sizes
// - BF2: reads lane bitsets directly (already time-split) -> reports raw + zstd sizes
// - Baseline: packed-symbol payload zstd (BF1) or packed-symbol reconstructed zstd (BF2)
//
// Used by `timemap bf-lanes`.

use anyhow::Context;
use k8dnz_core::signal::bitpack;

use super::args::{BfLanesArgs, BitMapping};
use super::util::{zstd_compress_len, zstd_decompress};

const BF1_MAGIC: &[u8; 4] = b"BF1\0";
const BF2_MAGIC: &[u8; 4] = b"BF2\0";

fn sym_mask(bits_per_emission: u8) -> u8 {
    if bits_per_emission == 0 {
        0
    } else if bits_per_emission >= 8 {
        0xFF
    } else {
        ((1u16 << bits_per_emission) - 1) as u8
    }
}

fn mapping_from_tag(v: u8) -> anyhow::Result<BitMapping> {
    match v {
        0 => Ok(BitMapping::Geom),
        1 => Ok(BitMapping::Hash),
        _ => anyhow::bail!("unknown mapping tag: {}", v),
    }
}

fn popcount_bytes(bs: &[u8]) -> usize {
    bs.iter().map(|b| b.count_ones() as usize).sum()
}

pub fn cmd_bf_lanes(a: BfLanesArgs) -> anyhow::Result<()> {
    let in_path = a.r#in.as_str();
    let zstd_level = a.zstd_level;

    let bytes = std::fs::read(in_path).with_context(|| format!("read bf: {}", in_path))?;
    if bytes.len() < 24 {
        anyhow::bail!("bf-lanes: file too small: {} bytes", bytes.len());
    }

    let magic = &bytes[0..4];

    // ---------------- BF1 ----------------
    if magic == BF1_MAGIC {
        let bits = bytes[4];
        if bits == 0 || bits > 8 {
            anyhow::bail!(
                "bf-lanes: unsupported bits_per_emission={} (expected 1..=8)",
                bits
            );
        }
        let mapping = mapping_from_tag(bytes[5])?;
        let orig_len_bytes = u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as usize;
        let symbol_count = u64::from_le_bytes(bytes[16..24].try_into().unwrap()) as usize;
        let payload = bytes[24..].to_vec();

        let lanes: usize = 1usize << (bits as usize);
        let mask: u8 = sym_mask(bits);

        let syms = bitpack::unpack_symbols(bits, &payload, symbol_count)
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        // Baseline = zstd over packed payload (no header), matching fit scoreboard convention.
        let baseline_payload_raw = payload.len();
        let baseline_payload_zstd = zstd_compress_len(&payload, zstd_level);

        let bitset_bytes = (symbol_count + 7) / 8;
        let mut lane_bitsets: Vec<Vec<u8>> = (0..lanes).map(|_| vec![0u8; bitset_bytes]).collect();
        let mut lane_counts: Vec<usize> = vec![0usize; lanes];

        for (i, &s) in syms.iter().enumerate() {
            let lane = (s & mask) as usize;
            lane_counts[lane] += 1;
            let byte_i = i >> 3;
            let bit_i = (i & 7) as u8;
            lane_bitsets[lane][byte_i] |= 1u8 << bit_i;
        }

        eprintln!("--- bf-lanes (BF1) ---");
        eprintln!("in                     = {}", in_path);
        eprintln!("bits_per_emission       = {}", bits);
        eprintln!("mapping                 = {:?}", mapping);
        eprintln!("orig_len_bytes          = {}", orig_len_bytes);
        eprintln!("symbol_count            = {}", symbol_count);
        eprintln!("lanes (2^k)             = {}", lanes);
        eprintln!("bitset_raw_bytes_each   = {}", bitset_bytes);
        eprintln!("zstd_level              = {}", zstd_level);
        eprintln!("baseline_payload_raw    = {}", baseline_payload_raw);
        eprintln!("baseline_payload_zstd   = {}", baseline_payload_zstd);
        eprintln!("note                    = baseline_payload_* matches fit scoreboard (resid_raw_bytes/resid_zstd_bytes)");
        eprintln!("");

        let mut total_bitset_raw: usize = 0;
        let mut total_bitset_zstd: usize = 0;

        for lane in 0..lanes {
            let raw = lane_bitsets[lane].len();
            let z = zstd_compress_len(&lane_bitsets[lane], zstd_level);
            total_bitset_raw += raw;
            total_bitset_zstd = total_bitset_zstd.saturating_add(z);

            let c = lane_counts[lane];
            let pct = if symbol_count == 0 {
                0.0
            } else {
                (c as f64) * 100.0 / (symbol_count as f64)
            };

            eprintln!(
                "lane {:>3} ({:0width$b})  count={:>8}  pct={:>6.2}%  bitset_raw={:>8}  bitset_zstd={:>8}",
                lane,
                lane,
                c,
                pct,
                raw,
                z,
                width = (bits as usize)
            );
        }

        eprintln!("");
        eprintln!("--- totals ---");
        eprintln!("total_bitset_raw        = {}", total_bitset_raw);
        eprintln!("total_bitset_zstd       = {}", total_bitset_zstd);
        eprintln!(
            "delta_total_zstd_vs_baseline_payload = {}",
            (total_bitset_zstd as i64) - (baseline_payload_zstd as i64)
        );

        return Ok(());
    }

    // ---------------- BF2 ----------------
    if magic == BF2_MAGIC {
        if bytes.len() < 32 {
            anyhow::bail!("bf-lanes: BF2 header too small: {} bytes", bytes.len());
        }
        let bits = bytes[4];
        if bits == 0 || bits > 8 {
            anyhow::bail!(
                "bf-lanes: unsupported bits_per_emission={} (expected 1..=8)",
                bits
            );
        }
        let mapping = mapping_from_tag(bytes[5])?;
        let orig_len_bytes = u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as usize;
        let symbol_count = u64::from_le_bytes(bytes[16..24].try_into().unwrap()) as usize;
        let lane_count = u32::from_le_bytes(bytes[24..28].try_into().unwrap()) as usize;

        let expected_lanes = 1usize << (bits as usize);
        if lane_count != expected_lanes {
            anyhow::bail!(
                "bf-lanes: BF2 lane_count mismatch: file={} expected={}",
                lane_count,
                expected_lanes
            );
        }

        let bitset_bytes = (symbol_count + 7) / 8;

        let mut cursor = 32usize;
        let mut lane_bitsets: Vec<Vec<u8>> = Vec::with_capacity(lane_count);
        let mut lane_comp_lens: Vec<usize> = Vec::with_capacity(lane_count);

        for _ in 0..lane_count {
            if cursor + 4 > bytes.len() {
                anyhow::bail!("bf-lanes: BF2 truncated reading lane length");
            }
            let clen = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
            cursor += 4;

            if cursor + clen > bytes.len() {
                anyhow::bail!("bf-lanes: BF2 truncated reading lane payload");
            }
            let comp = &bytes[cursor..cursor + clen];
            cursor += clen;

            lane_comp_lens.push(clen);
            let raw = zstd_decompress(comp)?;
            if raw.len() != bitset_bytes {
                anyhow::bail!(
                    "bf-lanes: BF2 lane bitset len mismatch: got {} want {}",
                    raw.len(),
                    bitset_bytes
                );
            }
            lane_bitsets.push(raw);
        }

        // Baseline: reconstruct residual symbols, then pack + zstd (no header),
        // to compare apples-to-apples with BF1 packed baseline.
        let mask = sym_mask(bits);
        let mut resid_syms = vec![0u8; symbol_count];
        let mut seen = vec![false; symbol_count];
        for lane in 0..lane_count {
            let bs = &lane_bitsets[lane];
            for i in 0..symbol_count {
                let byte = bs[i >> 3];
                let bit = (byte >> (i & 7)) & 1;
                if bit == 1 {
                    resid_syms[i] = (lane as u8) & mask;
                    seen[i] = true;
                }
            }
        }
        if seen.iter().any(|&v| !v) {
            anyhow::bail!("bf-lanes: BF2 invalid: some symbol positions not assigned to any lane");
        }
        let packed =
            bitpack::pack_symbols(bits, &resid_syms).map_err(|e| anyhow::anyhow!("{e}"))?;
        let baseline_payload_raw = packed.len();
        let baseline_payload_zstd = zstd_compress_len(&packed, zstd_level);

        eprintln!("--- bf-lanes (BF2) ---");
        eprintln!("in                     = {}", in_path);
        eprintln!("bits_per_emission       = {}", bits);
        eprintln!("mapping                 = {:?}", mapping);
        eprintln!("orig_len_bytes          = {}", orig_len_bytes);
        eprintln!("symbol_count            = {}", symbol_count);
        eprintln!("lanes (2^k)             = {}", lane_count);
        eprintln!("bitset_raw_bytes_each   = {}", bitset_bytes);
        eprintln!("zstd_level              = {}", zstd_level);
        eprintln!("baseline_packed_raw     = {}", baseline_payload_raw);
        eprintln!("baseline_packed_zstd    = {}", baseline_payload_zstd);
        eprintln!("note                    = baseline_packed_* is packed-symbol baseline reconstructed from lanes (comparable to BF1)");
        eprintln!("");

        let mut total_bitset_raw: usize = 0;
        let mut total_bitset_zstd: usize = 0;

        for lane in 0..lane_count {
            let raw = lane_bitsets[lane].len();
            let z = zstd_compress_len(&lane_bitsets[lane], zstd_level);
            total_bitset_raw += raw;
            total_bitset_zstd = total_bitset_zstd.saturating_add(z);

            let c = popcount_bytes(&lane_bitsets[lane]);
            let pct = if symbol_count == 0 {
                0.0
            } else {
                (c as f64) * 100.0 / (symbol_count as f64)
            };

            eprintln!(
                "lane {:>3} ({:0width$b})  count={:>8}  pct={:>6.2}%  bitset_raw={:>8}  bitset_zstd={:>8}  lane_comp_raw={:>8}",
                lane,
                lane,
                c,
                pct,
                raw,
                z,
                lane_comp_lens[lane],
                width = (bits as usize)
            );
        }

        eprintln!("");
        eprintln!("--- totals ---");
        eprintln!("total_bitset_raw        = {}", total_bitset_raw);
        eprintln!("total_bitset_zstd       = {}", total_bitset_zstd);
        eprintln!(
            "delta_total_zstd_vs_baseline_packed = {}",
            (total_bitset_zstd as i64) - (baseline_payload_zstd as i64)
        );

        return Ok(());
    }

    anyhow::bail!("bf-lanes: unknown magic (expected BF1\\0 or BF2\\0)");
}
