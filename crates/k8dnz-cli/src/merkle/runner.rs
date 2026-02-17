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

    // NEW: caps so we don't blow up forever
    pub lookahead_cap: u64,
    pub search_emissions_cap: u64,
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
            max_ticks_cap: 20_000_000_000,

            verify_reconstruct: true,

            lookahead_cap: 6_400_000,        // 400k * 16
            search_emissions_cap: 32_000_000, // 2M * 16
        }
    }
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
    let out_res = tmp.path().join("out.bf");

    let recipe_bytes = std::fs::read(recipe_path).context("read recipe")?;

    let mut max_ticks = profile.max_ticks_start;
    let mut lookahead = profile.lookahead;
    let mut search_emissions = profile.search_emissions;

    loop {
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

            chunk_size: payload.len().max(1),
            max_chunks: profile.max_chunks,

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
        timemap_run(args).context("timemap fit-xor-chunked(bitfield)")?;

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
                return Ok(blob);
            }
        } else {
            return Ok(blob);
        }

        // Retry policy: if we're already maxing scanned_windows (lookahead-bound),
        // increasing max_ticks alone won't help. Increase lookahead/search_emissions deterministically.
        if lookahead < profile.lookahead_cap {
            lookahead = (lookahead.saturating_mul(2)).min(profile.lookahead_cap);
        }
        if search_emissions < profile.search_emissions_cap {
            search_emissions = (search_emissions.saturating_mul(2)).min(profile.search_emissions_cap);
        }

        // Still also increase max_ticks (can matter when lookahead grows)
        max_ticks = max_ticks.saturating_mul(2);

        anyhow::ensure!(
            max_ticks <= profile.max_ticks_cap,
            "max_ticks cap reached without matching reconstruction (lookahead={} search_emissions={})",
            lookahead,
            search_emissions
        );
        anyhow::ensure!(
            lookahead <= profile.lookahead_cap && search_emissions <= profile.search_emissions_cap,
            "search caps reached without matching reconstruction"
        );
    }
}

pub fn reconstruct_blob(blob: &K8b1Blob) -> Result<Vec<u8>> {
    if blob.payload_len == 0 {
        return Ok(Vec::new());
    }

    let tmp = TempDir::new().context("tempdir")?;

    let recipe_path = tmp.path().join("recipe.k8r");
    let tm_path = tmp.path().join("tm.tm1");
    let res_path = tmp.path().join("res.bf");
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
