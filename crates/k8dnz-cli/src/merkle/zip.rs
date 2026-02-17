use anyhow::{Context, Result};

use super::format::{Arkm1Root, K8b1Blob, K8p2Pair};
use super::planner::chunk_and_pad_pow2;
use super::runner::{compress_payload_to_blob, FitProfile};

pub struct ZipReport {
    pub input_bytes: u64,
    pub chunk_bytes: u32,
    pub leaf_count: u32,
    pub rounds: u32,
    pub root_bytes: u64,
}

pub fn merkle_zip_bytes(
    recipe_path: &str,
    input: &[u8],
    chunk_bytes: usize,
    profile: &FitProfile,
    map_seed: u64,
) -> Result<(Vec<u8>, ZipReport)> {
    let (chunks, original_len, leaf_count) = chunk_and_pad_pow2(input, chunk_bytes);

    eprintln!(
        "[arkc] input_bytes={} chunk_bytes={} real_leaves={} padded_leaves={} (pow2) ",
        original_len,
        chunk_bytes,
        ((original_len + chunk_bytes as u64 - 1) / chunk_bytes as u64),
        leaf_count
    );

    let p = FitProfile {
        bits_per_emission: profile.bits_per_emission,
        bit_mapping: profile.bit_mapping,
        residual_mode: profile.residual_mode,
        objective: profile.objective,
        zstd_level: profile.zstd_level,

        lookahead: profile.lookahead,
        refine_topk: profile.refine_topk,
        search_emissions: profile.search_emissions,
        scan_step: profile.scan_step,
        start_emission: profile.start_emission,

        trans_penalty: profile.trans_penalty,
        max_chunks: 1,

        bit_tau: profile.bit_tau,
        bit_smooth_shift: profile.bit_smooth_shift,
        bitfield_residual: profile.bitfield_residual,
        chunk_xform: profile.chunk_xform,
        time_split: profile.time_split,

        max_ticks_start: profile.max_ticks_start,
        max_ticks_cap: profile.max_ticks_cap,

        verify_reconstruct: profile.verify_reconstruct,

        lookahead_cap: profile.lookahead_cap,
        search_emissions_cap: profile.search_emissions_cap,

        max_attempts: profile.max_attempts,
    };

    let mut level: Vec<Vec<u8>> = Vec::with_capacity(chunks.len());
    for (i, ch) in chunks.iter().enumerate() {
        let seed_i = derive_leaf_seed(map_seed, i as u64);
        eprintln!(
            "[arkc] LEAF {}/{} bytes={} seed=0x{:016x}",
            i + 1,
            chunks.len(),
            ch.len(),
            seed_i
        );

        let blob: K8b1Blob = compress_payload_to_blob(recipe_path, ch, &p, seed_i)
            .with_context(|| format!("compress leaf payload i={}", i))?;

        let enc = blob.encode();
        eprintln!("[arkc]  -> leaf_blob_bytes={}", enc.len());
        level.push(enc);
    }

    let mut rounds = 0u32;
    while level.len() > 1 {
        rounds += 1;
        eprintln!(
            "[arkc] ROUND {} nodes_in={} nodes_out={}",
            rounds,
            level.len(),
            level.len() / 2
        );

        let mut next: Vec<Vec<u8>> = Vec::with_capacity((level.len() + 1) / 2);
        let mut i = 0usize;
        let mut node_idx = 0usize;

        while i < level.len() {
            let a = level[i].clone();
            let b = level[i + 1].clone();

            let pair_payload = K8p2Pair { a, b }.encode();
            let seed_n = derive_node_seed(map_seed, rounds as u64, node_idx as u64);

            eprintln!(
                "[arkc] NODE r={} {}/{} payload_bytes={} seed=0x{:016x}",
                rounds,
                node_idx + 1,
                level.len() / 2,
                pair_payload.len(),
                seed_n
            );

            let node_blob = compress_payload_to_blob(recipe_path, &pair_payload, &p, seed_n)
                .with_context(|| format!("compress internal node payload r={} node={}", rounds, node_idx))?;

            let enc = node_blob.encode();
            eprintln!("[arkc]  -> node_blob_bytes={}", enc.len());
            next.push(enc);

            i += 2;
            node_idx += 1;
        }

        level = next;
    }

    let root_blob = level.pop().expect("non-empty");

    let root = Arkm1Root {
        original_len,
        chunk_bytes: chunk_bytes as u32,
        leaf_count,
        root_blob,
    };

    let bytes = root.encode();

    eprintln!(
        "[arkc] DONE rounds={} root_bytes={} leaf_count={} chunk_bytes={}",
        rounds,
        bytes.len(),
        leaf_count,
        chunk_bytes
    );

    let rep = ZipReport {
        input_bytes: original_len,
        chunk_bytes: chunk_bytes as u32,
        leaf_count,
        rounds,
        root_bytes: bytes.len() as u64,
    };
    Ok((bytes, rep))
}

fn derive_leaf_seed(base: u64, idx: u64) -> u64 {
    base ^ idx.wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

fn derive_node_seed(base: u64, round: u64, node: u64) -> u64 {
    let key = (round << 32) ^ node;
    base ^ key.wrapping_mul(0xD1B5_4A32_D192_ED03)
}
