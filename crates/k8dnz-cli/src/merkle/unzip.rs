use anyhow::{Context, Result};

use super::format::{Arkm1Root, K8b1Blob, K8p2Pair, MAGIC_K8P2};
use super::runner::reconstruct_blob;

pub struct UnzipReport {
    pub root_bytes: u64,
    pub leaf_count: u32,
    pub rounds: u32,
    pub out_bytes: u64,
}

pub fn merkle_unzip_to_bytes(root_bytes: &[u8]) -> Result<(Vec<u8>, UnzipReport)> {
    let root = Arkm1Root::decode(root_bytes).context("decode ARKM1")?;

    let mut leaves: Vec<Vec<u8>> = Vec::new();
    let mut rounds = 0u32;

    expand_blob_to_leaves(&root.root_blob, &mut leaves, &mut rounds)
        .context("expand root")?;

    // concatenate leaf payloads and truncate to original_len
    let mut out = Vec::new();
    for leaf in leaves {
        out.extend_from_slice(&leaf);
    }
    out.truncate(root.original_len as usize);

    let rep = UnzipReport {
        root_bytes: root_bytes.len() as u64,
        leaf_count: root.leaf_count,
        rounds,
        out_bytes: out.len() as u64,
    };
    Ok((out, rep))
}

fn expand_blob_to_leaves(blob_bytes: &[u8], leaves_out: &mut Vec<Vec<u8>>, rounds: &mut u32) -> Result<()> {
    let blob = K8b1Blob::decode(blob_bytes).context("decode K8B1")?;
    let payload = reconstruct_blob(&blob).context("reconstruct K8B1")?;

    if payload.len() >= 4 && payload[0..4] == MAGIC_K8P2 {
        *rounds = rounds.saturating_add(1);

        let pair = K8p2Pair::decode(&payload).context("decode K8P2")?;
        expand_blob_to_leaves(&pair.a, leaves_out, rounds)?;
        expand_blob_to_leaves(&pair.b, leaves_out, rounds)?;
        Ok(())
    } else {
        leaves_out.push(payload);
        Ok(())
    }
}
