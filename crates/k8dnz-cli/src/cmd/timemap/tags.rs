// crates/k8dnz-cli/src/cmd/timemap/tags.rs

use super::args::TagFormat;
use super::util::splitmix64;

const TAG_MAGIC: &[u8; 4] = b"TG1\0";

#[derive(Clone, Debug)]
pub struct CondTags {
    pub tags: Vec<u8>,      // tag per block
    pub block_bytes: usize, // bytes per output block
}

fn unpack_tags(bits_per_tag: u8, tag_count: usize, payload: &[u8]) -> anyhow::Result<Vec<u8>> {
    if bits_per_tag == 0 || bits_per_tag > 8 {
        anyhow::bail!("bits_per_tag must be in 1..=8");
    }

    let total_bits = tag_count * (bits_per_tag as usize);
    let need_bytes = (total_bits + 7) / 8;
    if payload.len() < need_bytes {
        anyhow::bail!(
            "packed tags payload too small: have={} need={}",
            payload.len(),
            need_bytes
        );
    }

    let mask: u16 = (1u16 << bits_per_tag) - 1;
    let mut out: Vec<u8> = Vec::with_capacity(tag_count);

    let mut bitpos: usize = 0;
    for _ in 0..tag_count {
        let mut v: u16 = 0;
        for k in 0..(bits_per_tag as usize) {
            let byte_i = bitpos >> 3;
            let bit_i = bitpos & 7;
            let b = (payload[byte_i] >> bit_i) & 1;
            v |= (b as u16) << k;
            bitpos += 1;
        }
        out.push((v & mask) as u8);
    }

    Ok(out)
}

pub fn read_cond_tags(path: &str, fmt: TagFormat, block_bytes: usize) -> anyhow::Result<CondTags> {
    if block_bytes == 0 {
        anyhow::bail!("--cond-block-bytes must be >= 1");
    }

    let bytes = std::fs::read(path)?;

    match fmt {
        TagFormat::Byte => Ok(CondTags {
            tags: bytes,
            block_bytes,
        }),
        TagFormat::Packed => {
            if bytes.len() < (4 + 1 + 2 + 1 + 8) {
                anyhow::bail!("packed tags too small");
            }
            if &bytes[0..4] != TAG_MAGIC {
                anyhow::bail!("packed tags missing TG1 magic (expected TG1\\0 header)");
            }

            let bits = bytes[4];
            let tag_count = u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as usize;
            let payload = &bytes[16..];

            let tags = unpack_tags(bits, tag_count, payload)?;
            Ok(CondTags { tags, block_bytes })
        }
    }
}

fn cond_mask_byte(cond_seed: u64, tag: u8, out_index: u64) -> u8 {
    let x = cond_seed ^ ((tag as u64) << 56) ^ out_index;
    (splitmix64(x) & 0xFF) as u8
}

pub fn apply_conditioning_if_enabled(
    mapped: u8,
    cond: &Option<CondTags>,
    cond_seed: u64,
    out_index: usize,
) -> u8 {
    if let Some(ct) = cond {
        let blk = out_index / ct.block_bytes;
        if blk < ct.tags.len() {
            let tag = ct.tags[blk];
            let m = cond_mask_byte(cond_seed, tag, out_index as u64);
            return mapped ^ m;
        }
    }
    mapped
}
