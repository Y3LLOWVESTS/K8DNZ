// crates/k8dnz-cli/src/merkle/runner.rs

use anyhow::{Context, Result};
use tempfile::TempDir;

use crate::cmd::timemap::args::{
    ApplyMode, BitMapping, BitfieldResidualEncoding, ChunkXform, FitObjective, FitXorChunkedArgs, MapMode,
    ReconstructArgs, ResidualMode, TagFormat, TimemapArgs, TimemapCmd,
};
use crate::cmd::timemap::run as timemap_run;

use super::format::{K8b1Blob, ReconParams};

pub struct FitProfile {
    pub bits_per_emission: u8,
    pub bit_mapping: BitMapping,
    pub residual_mode: ResidualMode,
    pub objective: FitObjective,
    pub zstd_level: i32,

    pub lookahead: u64,
    pub refine_topk: usize,
    pub search_emissions: u64,
    pub scan_step: u64,
    pub start_emission: u64,

    pub trans_penalty: u64,
    pub max_chunks: usize,

    pub bit_tau: u16,
    pub bit_smooth_shift: u8,
    pub bitfield_residual: BitfieldResidualEncoding,
    pub chunk_xform: ChunkXform,
    pub time_split: bool,

    pub max_ticks_start: u64,
    pub max_ticks_cap: u64,

    pub verify_reconstruct: bool,

    pub lookahead_cap: u64,
    pub search_emissions_cap: u64,

    /// If 0 => unlimited attempts (keep expanding budgets until success or integer saturation).
    pub max_attempts: usize,
}

impl Default for FitProfile {
    fn default() -> Self {
        Self {
            bits_per_emission: 1,
            bit_mapping: BitMapping::LowpassThresh,
            residual_mode: ResidualMode::Xor,
            objective: FitObjective::Zstd,
            zstd_level: 3,

            lookahead: 400_000,
            refine_topk: 2048,
            search_emissions: 2_000_000,
            scan_step: 1,
            start_emission: 0,

            trans_penalty: 1,
            max_chunks: 0,

            bit_tau: 128,
            bit_smooth_shift: 3,
            bitfield_residual: BitfieldResidualEncoding::Packed,
            chunk_xform: ChunkXform::None,
            time_split: false,

            max_ticks_start: 200_000_000,

            // IMPORTANT: remove hard caps by default so arkc can run on any sized file.
            max_ticks_cap: u64::MAX,
            lookahead_cap: u64::MAX,
            search_emissions_cap: u64::MAX,

            verify_reconstruct: true,

            // IMPORTANT: unlimited by default (0 => unlimited).
            max_attempts: 0,
        }
    }
}

/// IMPORTANT:
/// `timemap reconstruct` does not accept `--bitfield-residual`, so it must infer the residual
/// container/encoding from the residual file itself (commonly by extension like .bf1/.bf2).
/// If we write residual to a generic ".bf", reconstruct may parse it incorrectly and produce a
/// deterministic mismatch forever.
fn residual_ext_for_bits(bits_per_emission: u8) -> &'static str {
    match bits_per_emission {
        1 => "bf1",
        2 => "bf2",
        4 => "bf4",
        8 => "bf8",
        _ => "bf", // fallback
    }
}

fn is_capacity_fit_error(e: &anyhow::Error) -> bool {
    let s = format!("{:#}", e);
    s.contains("no room for chunk")
        || s.contains("no output produced")
        || s.contains("stopping")
        || s.contains("need ") && s.contains(" syms") && s.contains(" have ")
}

fn bump_budgets(profile: &FitProfile, max_ticks: &mut u64, lookahead: &mut u64, search_emissions: &mut u64) -> Result<()> {
    if *lookahead < profile.lookahead_cap {
        let next = lookahead.saturating_mul(2);
        *lookahead = next.min(profile.lookahead_cap);
    }
    if *search_emissions < profile.search_emissions_cap {
        let next = search_emissions.saturating_mul(2);
        *search_emissions = next.min(profile.search_emissions_cap);
    }

    let next_ticks = max_ticks.saturating_mul(2);
    if next_ticks == *max_ticks {
        anyhow::bail!("max_ticks saturated at {} (cannot grow further)", max_ticks);
    }

    let capped = next_ticks.min(profile.max_ticks_cap);
    if capped == *max_ticks {
        anyhow::bail!(
            "max_ticks cap reached (cap={}) without success (lookahead={} search_emissions={})",
            profile.max_ticks_cap,
            lookahead,
            search_emissions
        );
    }
    *max_ticks = capped;

    Ok(())
}

pub fn compress_payload_to_blob(
    recipe_path: &str,
    payload: &[u8],
    profile: &FitProfile,
    map_seed: u64,
) -> Result<K8b1Blob> {
    if payload.is_empty() {
        return Ok(K8b1Blob {
            payload_len: 0,
            recon: ReconParams {
                max_ticks: profile.max_ticks_start,
                map_seed,
                bits_per_emission: profile.bits_per_emission,
                bit_mapping: bit_mapping_to_u8(profile.bit_mapping),
                bit_tau: profile.bit_tau as u32,
                bit_smooth_shift: profile.bit_smooth_shift,
                residual_mode: residual_mode_to_u8(profile.residual_mode),
            },
            recipe: std::fs::read(recipe_path).context("read recipe")?,
            timemap: Vec::new(),
            residual: Vec::new(),
        });
    }

    let tmp = TempDir::new().context("tempdir")?;

    let target_path = tmp.path().join("payload.bin");
    std::fs::write(&target_path, payload).context("write payload")?;

    let out_tm = tmp.path().join("out.tm1");

    // Key fix: choose residual extension based on bits_per_emission so reconstruct parses correctly.
    let res_ext = residual_ext_for_bits(profile.bits_per_emission);
    let out_res = tmp.path().join(format!("out.{res_ext}"));

    let recipe_bytes = std::fs::read(recipe_path).context("read recipe")?;

    let mut max_ticks = profile.max_ticks_start.max(1);
    let mut lookahead = profile.lookahead.max(1);
    let mut search_emissions = profile.search_emissions.max(1);

    // IMPORTANT: chunk_size is in SYMBOLS for bitfield mode; respect bits_per_emission.
    // bits_total = bytes*8; symbols_needed = ceil(bits_total / bpe).
    let bits_total: u64 = (payload.len() as u64).saturating_mul(8);
    let bpe: u64 = (profile.bits_per_emission as u64).max(1);
    let chunk_size: u64 = ((bits_total + (bpe - 1)) / bpe).max(1);

    let max_chunks = if profile.max_chunks == 0 { 1 } else { profile.max_chunks.max(1) };

    let mut attempt: usize = 1;

    loop {
        if profile.max_attempts != 0 && attempt > profile.max_attempts {
            anyhow::bail!(
                "runner gave up after {} attempts without matching reconstruction (payload={} bytes)",
                profile.max_attempts,
                payload.len()
            );
        }

        eprintln!(
            "[runner] attempt {}{} payload={} max_ticks={} lookahead={} search_emissions={} chunk_size_syms={} max_chunks={} bpe={}",
            attempt,
            if profile.max_attempts == 0 {
                " (unbounded)"
            } else {
                ""
            },
            payload.len(),
            max_ticks,
            lookahead,
            search_emissions,
            chunk_size,
            max_chunks,
            profile.bits_per_emission
        );

        let a = FitXorChunkedArgs {
            recipe: recipe_path.to_string(),
            target: target_path.to_string_lossy().to_string(),
            out_timemap: out_tm.to_string_lossy().to_string(),
            out_residual: out_res.to_string_lossy().to_string(),

            mode: ApplyMode::Rgbpair,
            map: MapMode::Bitfield,

            map_seed,
            map_seed_hex: None,

            residual: profile.residual_mode,

            search_emissions,
            max_ticks,
            start_emission: profile.start_emission,
            scan_step: profile.scan_step as usize,

            zstd_level: profile.zstd_level,

            chunk_size: chunk_size as usize,
            max_chunks,

            objective: profile.objective,

            refine_topk: profile.refine_topk,
            lookahead: lookahead as usize,

            trans_penalty: profile.trans_penalty,

            bits_per_emission: profile.bits_per_emission,
            bit_mapping: profile.bit_mapping,
            bit_tau: profile.bit_tau,
            bit_smooth_shift: profile.bit_smooth_shift,

            bitfield_residual: profile.bitfield_residual,
            time_split: profile.time_split,
            chunk_xform: profile.chunk_xform,

            cond_tags: None,
            cond_tag_format: TagFormat::Byte,
            cond_block_bytes: 16,
            cond_seed: 0,
            cond_seed_hex: None,
        };

        let args = TimemapArgs {
            cmd: TimemapCmd::FitXorChunked(a),
        };

        // FIX: capacity failures must be retryable, not fatal.
        if let Err(e) = timemap_run(args) {
            if is_capacity_fit_error(&e) {
                eprintln!(
                    "[runner] fit capacity failure -> retry (attempt={} max_ticks={}) : {}",
                    attempt, max_ticks, e
                );
                bump_budgets(profile, &mut max_ticks, &mut lookahead, &mut search_emissions)?;
                attempt = attempt.saturating_add(1);
                continue;
            }
            return Err(e).context("timemap fit-xor-chunked(bitfield)");
        }

        let tm = std::fs::read(&out_tm).context("read timemap")?;
        let resid = std::fs::read(&out_res).context("read residual")?;

        let blob = K8b1Blob {
            payload_len: payload.len() as u32,
            recon: ReconParams {
                max_ticks,
                map_seed,
                bits_per_emission: profile.bits_per_emission,
                bit_mapping: bit_mapping_to_u8(profile.bit_mapping),
                bit_tau: profile.bit_tau as u32,
                bit_smooth_shift: profile.bit_smooth_shift,
                residual_mode: residual_mode_to_u8(profile.residual_mode),
            },
            recipe: recipe_bytes.clone(),
            timemap: tm,
            residual: resid,
        };

        if profile.verify_reconstruct {
            let out = reconstruct_blob(&blob).context("verify reconstruct")?;
            if out == payload {
                eprintln!(
                    "[runner] success: reconstruction matches payload (attempt={} max_ticks={})",
                    attempt, max_ticks
                );
                return Ok(blob);
            } else {
                eprintln!(
                    "[runner] reconstruct mismatch (attempt={} max_ticks={}) -> retry",
                    attempt, max_ticks
                );
                dump_mismatch(payload, &out);
            }
        } else {
            eprintln!("[runner] verify_reconstruct disabled: accepting blob");
            return Ok(blob);
        }

        bump_budgets(profile, &mut max_ticks, &mut lookahead, &mut search_emissions)?;
        attempt = attempt.saturating_add(1);
    }
}

fn dump_mismatch(expected: &[u8], got: &[u8]) {
    let _ = std::fs::write("/tmp/k8dnz_expected.bin", expected);
    let _ = std::fs::write("/tmp/k8dnz_got.bin", got);

    eprintln!(
        "[runner] wrote /tmp/k8dnz_expected.bin and /tmp/k8dnz_got.bin (for xxd/cmp)"
    );

    eprintln!(
        "[runner] mismatch lens: expected={} got={}",
        expected.len(),
        got.len()
    );
    eprintln!(
        "[runner] mismatch hash64: expected=0x{:016x} got=0x{:016x}",
        fnv1a64(expected),
        fnv1a64(got)
    );

    let n = expected.len().min(got.len());
    let mut diff = None;
    for i in 0..n {
        if expected[i] != got[i] {
            diff = Some(i);
            break;
        }
    }

    match diff {
        None => {
            if expected.len() != got.len() {
                eprintln!("[runner] mismatch: contents equal for min_len={}, only length differs", n);
            } else {
                eprintln!("[runner] mismatch: no differing byte found, but buffers not equal (unexpected)");
            }
        }
        Some(i) => {
            let start = i.saturating_sub(16);
            let end = (i + 16).min(n);
            eprintln!("[runner] first_diff_at={} (0x{:x})", i, i);
            eprintln!(
                "[runner] expected[{}..{}] = {}",
                start,
                end,
                hex_slice(&expected[start..end])
            );
            eprintln!(
                "[runner] got     [{}..{}] = {}",
                start,
                end,
                hex_slice(&got[start..end])
            );
        }
    }
}

fn fnv1a64(buf: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in buf {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

fn hex_slice(buf: &[u8]) -> String {
    let mut s = String::with_capacity(buf.len() * 2);
    for &b in buf {
        use std::fmt::Write;
        let _ = write!(s, "{:02x}", b);
    }
    s
}

pub fn reconstruct_blob(blob: &K8b1Blob) -> Result<Vec<u8>> {
    if blob.payload_len == 0 {
        return Ok(Vec::new());
    }

    let tmp = TempDir::new().context("tempdir")?;

    let recipe_path = tmp.path().join("recipe.k8r");
    let tm_path = tmp.path().join("tm.tm1");

    // Key fix mirrored here: residual filename must carry the right extension for reconstruct to parse.
    let res_ext = residual_ext_for_bits(blob.recon.bits_per_emission);
    let res_path = tmp.path().join(format!("res.{res_ext}"));

    let out_path = tmp.path().join("out.bin");

    std::fs::write(&recipe_path, &blob.recipe).context("write recipe")?;
    std::fs::write(&tm_path, &blob.timemap).context("write timemap")?;
    std::fs::write(&res_path, &blob.residual).context("write residual")?;

    let bit_mapping = bit_mapping_from_u8(blob.recon.bit_mapping);
    let residual_mode = residual_mode_from_u8(blob.recon.residual_mode);
    let bit_tau_u16 = u16::try_from(blob.recon.bit_tau).unwrap_or(u16::MAX);

    let r = ReconstructArgs {
        recipe: recipe_path.to_string_lossy().to_string(),
        timemap: tm_path.to_string_lossy().to_string(),
        residual: res_path.to_string_lossy().to_string(),
        out: out_path.to_string_lossy().to_string(),

        mode: ApplyMode::Rgbpair,
        map: MapMode::Bitfield,

        map_seed: blob.recon.map_seed,
        map_seed_hex: None,

        residual_mode,
        max_ticks: blob.recon.max_ticks,

        bits_per_emission: blob.recon.bits_per_emission,
        bit_mapping,
        bit_tau: bit_tau_u16,
        bit_smooth_shift: blob.recon.bit_smooth_shift,

        cond_tags: None,
        cond_tag_format: TagFormat::Byte,
        cond_block_bytes: 16,
        cond_seed: 0,
        cond_seed_hex: None,
    };

    let args = TimemapArgs {
        cmd: TimemapCmd::Reconstruct(r),
    };
    timemap_run(args).context("timemap reconstruct(bitfield)")?;

    let out = std::fs::read(&out_path).context("read reconstructed output")?;
    anyhow::ensure!(out.len() == blob.payload_len as usize, "reconstruct length mismatch");
    Ok(out)
}

fn bit_mapping_to_u8(v: BitMapping) -> u8 {
    match v {
        BitMapping::Geom => 0,
        BitMapping::Hash => 1,
        BitMapping::LowpassThresh => 2,
    }
}

fn bit_mapping_from_u8(v: u8) -> BitMapping {
    match v {
        0 => BitMapping::Geom,
        1 => BitMapping::Hash,
        2 => BitMapping::LowpassThresh,
        _ => BitMapping::Geom,
    }
}

fn residual_mode_to_u8(v: ResidualMode) -> u8 {
    match v {
        ResidualMode::Xor => 0,
        ResidualMode::Sub => 1,
    }
}

fn residual_mode_from_u8(v: u8) -> ResidualMode {
    match v {
        0 => ResidualMode::Xor,
        1 => ResidualMode::Sub,
        _ => ResidualMode::Xor,
    }
}
