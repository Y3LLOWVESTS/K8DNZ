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

    /// Attempts bound. If 0, treat as "legacy unbounded" (but Default no longer uses 0).
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

            trans_penalty: 0,
            max_chunks: 1,

            bit_tau: 256,
            bit_smooth_shift: 0,
            bitfield_residual: BitfieldResidualEncoding::Packed,
            chunk_xform: ChunkXform::None,
            time_split: false,

            max_ticks_start: 20_000_000,
            max_ticks_cap: 20_000_000,

            verify_reconstruct: true,

            lookahead_cap: 1_200_000,
            search_emissions_cap: 6_000_000,

            // KEY FIX: bounded by default so we don't "infinite loop" on persistent mismatch.
            max_attempts: 32,
        }
    }
}

pub fn compress_payload_to_blob(
    _recipe_path: &str,
    recipe_bytes: &[u8],
    payload: &[u8],
    map_seed: u64,
    profile: &FitProfile,
) -> Result<K8b1Blob> {
    let tmp = TempDir::new().context("TempDir")?;
    let target_path = tmp.path().join("target.bin");
    let recipe_tmp = tmp.path().join("recipe.k8r");
    let out_tm = tmp.path().join("out.tm1");
    let out_res = tmp.path().join("out.bf");

    std::fs::write(&target_path, payload).context("write target")?;
    std::fs::write(&recipe_tmp, recipe_bytes).context("write recipe")?;

    let mut max_ticks = profile.max_ticks_start;
    let mut lookahead = profile.lookahead;
    let mut search_emissions = profile.search_emissions;

    // chunk_size in SYMBOLS (not bytes): timemap chunking is in emission symbols.
    let chunk_size = {
        let bpe = profile.bits_per_emission as u64;
        let total_bits = (payload.len() as u64) * 8;
        if bpe == 0 {
            total_bits // degenerate, shouldn't happen
        } else {
            (total_bits + (bpe - 1)) / bpe
        }
    };

    let max_chunks = profile.max_chunks;

    let mut attempt: usize = 1;
    loop {
        if profile.max_attempts != 0 && attempt > profile.max_attempts {
            anyhow::bail!(
                "runner gave up after {} attempts without matching reconstruction (payload={} bytes). \
                 Consider inspecting /tmp/k8dnz_expected.bin and /tmp/k8dnz_got.bin, \
                 or increasing max_attempts explicitly.",
                profile.max_attempts,
                payload.len()
            );
        }

        eprintln!(
            "[runner] attempt {}{} payload={} max_ticks={} lookahead={} search_emissions={} chunk_size_syms={} max_chunks={} bpe={}",
            attempt,
            if profile.max_attempts == 0 { " (unbounded)" } else { "" },
            payload.len(),
            max_ticks,
            lookahead,
            search_emissions,
            chunk_size,
            max_chunks,
            profile.bits_per_emission
        );

        let a = FitXorChunkedArgs {
            recipe: recipe_tmp.to_string_lossy().to_string(),
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

        // Capacity failures should be retryable, not fatal.
        if let Err(e) = timemap_run(args) {
            if is_capacity_fit_error(&e) {
                eprintln!(
                    "[runner] fit capacity failure -> retry (attempt={} max_ticks={}) : {}",
                    attempt, max_ticks, e
                );
                if !advance_knobs(profile, &mut max_ticks, &mut lookahead, &mut search_emissions)? {
                    anyhow::bail!(
                        "runner cannot advance any knobs further (capacity failures persist). \
                         payload={} bytes bpe={} max_ticks={} lookahead={} search_emissions={}",
                        payload.len(),
                        profile.bits_per_emission,
                        max_ticks,
                        lookahead,
                        search_emissions
                    );
                }
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
            recipe: recipe_bytes.to_vec(),
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

        if !advance_knobs(profile, &mut max_ticks, &mut lookahead, &mut search_emissions)? {
            anyhow::bail!(
                "runner cannot advance any knobs further (reconstruct mismatch persists). \
                 payload={} bytes bpe={} max_ticks={} lookahead={} search_emissions={}",
                payload.len(),
                profile.bits_per_emission,
                max_ticks,
                lookahead,
                search_emissions
            );
        }

        attempt = attempt.saturating_add(1);
    }
}

fn advance_knobs(profile: &FitProfile, max_ticks: &mut u64, lookahead: &mut u64, search_emissions: &mut u64) -> Result<bool> {
    // Primary: increase budgets up to caps.
    let mut changed = false;

    let prev_ticks = *max_ticks;
    let prev_look = *lookahead;
    let prev_emit = *search_emissions;

    bump_budgets(profile, max_ticks, lookahead, search_emissions)?;

    if *max_ticks != prev_ticks || *lookahead != prev_look || *search_emissions != prev_emit {
        changed = true;
    }

    // If we hit caps and nothing changed, we cannot move further in this runner instance.
    Ok(changed)
}

fn bump_budgets(profile: &FitProfile, max_ticks: &mut u64, lookahead: &mut u64, search_emissions: &mut u64) -> Result<()> {
    // Expand max_ticks but respect cap.
    if *max_ticks < profile.max_ticks_cap {
        let next = (*max_ticks).saturating_add((*max_ticks) / 2).max(*max_ticks + 1);
        *max_ticks = next.min(profile.max_ticks_cap);
    }

    // Expand lookahead but respect cap.
    if *lookahead < profile.lookahead_cap {
        let next = (*lookahead).saturating_add((*lookahead) / 2).max(*lookahead + 1);
        *lookahead = next.min(profile.lookahead_cap);
    }

    // Expand search_emissions but respect cap.
    if *search_emissions < profile.search_emissions_cap {
        let next = (*search_emissions)
            .saturating_add((*search_emissions) / 2)
            .max(*search_emissions + 1);
        *search_emissions = next.min(profile.search_emissions_cap);
    }

    Ok(())
}

fn dump_mismatch(expected: &[u8], got: &[u8]) {
    let _ = std::fs::write("/tmp/k8dnz_expected.bin", expected);
    let _ = std::fs::write("/tmp/k8dnz_got.bin", got);

    eprintln!("[runner] wrote /tmp/k8dnz_expected.bin and /tmp/k8dnz_got.bin (for xxd/cmp)");
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
                eprintln!("[runner] mismatch: all shared bytes match; length differs.");
            } else {
                eprintln!("[runner] mismatch: no differing index found but buffers not equal (unexpected).");
            }
        }
        Some(i) => {
            eprintln!(
                "[runner] first diff @ {}: expected=0x{:02x} got=0x{:02x}",
                i, expected[i], got[i]
            );
        }
    }
}

fn fnv1a64(b: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &x in b {
        h ^= x as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

fn is_capacity_fit_error(e: &anyhow::Error) -> bool {
    let s = format!("{:#}", e);
    s.contains("no room for chunk") || s.contains("capacity")
}

fn bit_mapping_to_u8(m: BitMapping) -> u8 {
    match m {
        BitMapping::LowpassThresh => 1,
        BitMapping::Geom => 2,
        BitMapping::Hash => 3,
    }
}

fn residual_mode_to_u8(m: ResidualMode) -> u8 {
    match m {
        ResidualMode::Xor => 1,
        ResidualMode::Sub => 2,
    }
}

// FIX: make visible to merkle::unzip
pub(super) fn reconstruct_blob(blob: &K8b1Blob) -> Result<Vec<u8>> {
    let tmp = TempDir::new().context("TempDir")?;
    let recipe_path = tmp.path().join("recipe.k8r");
    let tm_path = tmp.path().join("tm.tm1");
    let resid_path = tmp.path().join("resid.bf");
    let out_path = tmp.path().join("out.bin");

    std::fs::write(&recipe_path, &blob.recipe).context("write recipe")?;
    std::fs::write(&tm_path, &blob.timemap).context("write timemap")?;
    std::fs::write(&resid_path, &blob.residual).context("write residual")?;

    // NOTE: ReconstructArgs no longer accepts payload_len/bitfield_residual/time_split/chunk_xform.
    // Keep only the currently supported fields + cond_* fields.
    let a = ReconstructArgs {
        recipe: recipe_path.to_string_lossy().to_string(),
        timemap: tm_path.to_string_lossy().to_string(),
        residual: resid_path.to_string_lossy().to_string(),
        out: out_path.to_string_lossy().to_string(),

        mode: ApplyMode::Rgbpair,
        map: MapMode::Bitfield,

        max_ticks: blob.recon.max_ticks,
        map_seed: blob.recon.map_seed,
        map_seed_hex: None,

        bits_per_emission: blob.recon.bits_per_emission,
        bit_mapping: u8_to_bit_mapping(blob.recon.bit_mapping),
        bit_tau: blob.recon.bit_tau as u16,
        bit_smooth_shift: blob.recon.bit_smooth_shift,

        residual_mode: u8_to_residual_mode(blob.recon.residual_mode),

        cond_tags: None,
        cond_tag_format: TagFormat::Byte,
        cond_block_bytes: 16,
        cond_seed: 0,
        cond_seed_hex: None,
    };

    let args = TimemapArgs {
        cmd: TimemapCmd::Reconstruct(a),
    };

    timemap_run(args).context("timemap reconstruct")?;
    let out = std::fs::read(&out_path).context("read reconstructed out")?;
    Ok(out)
}

fn u8_to_bit_mapping(v: u8) -> BitMapping {
    match v {
        2 => BitMapping::Geom,
        3 => BitMapping::Hash,
        _ => BitMapping::LowpassThresh,
    }
}

fn u8_to_residual_mode(v: u8) -> ResidualMode {
    match v {
        2 => ResidualMode::Sub,
        _ => ResidualMode::Xor,
    }
}
