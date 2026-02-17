// crates/k8dnz-cli/src/cmd/timemap/util.rs

use super::args::MapSeedArgs;

pub fn parse_seed(a: &MapSeedArgs) -> anyhow::Result<u64> {
    parse_seed_hex_opt(a.map_seed, &a.map_seed_hex)
}

/// Parse a seed given a decimal default and an optional hex override.
/// Accepts "0x..." or raw hex.
pub fn parse_seed_hex_opt(map_seed: u64, map_seed_hex: &Option<String>) -> anyhow::Result<u64> {
    if let Some(s) = map_seed_hex {
        let t = s.trim();
        let t = t.strip_prefix("0x").unwrap_or(t);
        let v = u64::from_str_radix(t, 16)
            .map_err(|e| anyhow::anyhow!("invalid hex seed ({s}): {e}"))?;
        Ok(v)
    } else {
        Ok(map_seed)
    }
}

pub fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

/// Returns compressed length in bytes, or usize::MAX on error (keeps callers simple).
pub fn zstd_compress_len(bytes: &[u8], level: i32) -> usize {
    zstd::encode_all(bytes, level)
        .map(|v| v.len())
        .unwrap_or(usize::MAX)
}

pub fn zstd_decompress(bytes: &[u8]) -> anyhow::Result<Vec<u8>> {
    Ok(zstd::decode_all(bytes)?)
}

pub fn var_u64_len(mut x: u64) -> usize {
    let mut n: usize = 1;
    while x >= 0x80 {
        n += 1;
        x >>= 7;
    }
    n
}
#[allow(dead_code)]
pub fn tm1_len_contig(start_pos: u64, n: usize) -> usize {
    let magic = 4usize;
    let count = var_u64_len(n as u64);
    let delta0 = var_u64_len(start_pos);
    let deltas_rest = if n <= 1 { 0usize } else { n - 1 };
    magic + count + delta0 + deltas_rest
}

pub fn tm_jump_cost(prev_pos: Option<u64>, next_start_pos: u64) -> usize {
    match prev_pos {
        None => var_u64_len(next_start_pos),
        Some(p) => {
            let delta = next_start_pos.saturating_sub(p);
            var_u64_len(delta)
        }
    }
}
