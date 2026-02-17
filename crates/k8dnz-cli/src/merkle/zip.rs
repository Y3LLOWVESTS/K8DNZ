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

pub fn merkle_zip_bytes(recipe_path: &str, input: &[u8], chunk_bytes: usize, profile: &FitProfile, map_seed: u64) -> Result<(Vec<u8>, ZipReport)> {
    let (chunks, original_len, leaf_count) = chunk_and_pad_pow2(input, chunk_bytes);

    // Leaves
    let mut level: Vec<Vec<u8>> = Vec::with_capacity(chunks.len());
    for ch in chunks.iter() {
        let blob: K8b1Blob = compress_payload_to_blob(recipe_path, ch, profile, map_seed)
            .with_context(|| "compress leaf payload")?;
        level.push(blob.encode());
    }

    // Internal levels: pairwise K8P2 payloads, each compressed as a node blob.
    let mut rounds = 0u32;
    while level.len() > 1 {
        rounds += 1;

        let mut next: Vec<Vec<u8>> = Vec::with_capacity((level.len() + 1) / 2);
        let mut i = 0usize;
        while i < level.len() {
            let a = level[i].clone();
            let b = level[i + 1].clone();

            let pair_payload = K8p2Pair { a, b }.encode();

            let node_blob = compress_payload_to_blob(recipe_path, &pair_payload, profile, map_seed)
                .with_context(|| "compress internal node payload")?;
            next.push(node_blob.encode());

            i += 2;
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
    let rep = ZipReport {
        input_bytes: original_len,
        chunk_bytes: chunk_bytes as u32,
        leaf_count,
        rounds,
        root_bytes: bytes.len() as u64,
    };
    Ok((bytes, rep))
}
