use anyhow::{anyhow, Result};

use super::ws_lane_types::WsLaneChunkedBest;

const MAGIC: &[u8; 4] = b"AKCM";
const VERSION: u8 = 1;
const FLAGS: u8 = 0;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompactChunkKey {
    pub root_quadrant: u8,
    pub root_seed: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompactChunkManifest {
    pub total_len: u64,
    pub chunk_bytes: u64,
    pub recipe_seed: u64,
    pub keys: Vec<CompactChunkKey>,
}

impl CompactChunkManifest {
    pub fn from_chunked(chunked: &WsLaneChunkedBest) -> Result<Self> {
        let recipe_seed = chunked
            .chunks
            .first()
            .map(|chunk| chunk.key.recipe_seed)
            .unwrap_or(0);

        for chunk in &chunked.chunks {
            if chunk.key.recipe_seed != recipe_seed {
                return Err(anyhow!(
                    "compact manifest requires shared recipe_seed across chunks"
                ));
            }
        }

        let keys = chunked
            .chunks
            .iter()
            .map(|chunk| CompactChunkKey {
                root_quadrant: chunk.key.root_quadrant,
                root_seed: chunk.key.root_seed,
            })
            .collect::<Vec<_>>();

        Ok(Self {
            total_len: chunked.predicted.len() as u64,
            chunk_bytes: chunked.chunk_bytes as u64,
            recipe_seed,
            keys,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(MAGIC);
        out.push(VERSION);
        out.push(FLAGS);
        put_varint(self.total_len, &mut out);
        put_varint(self.chunk_bytes, &mut out);
        put_varint(self.keys.len() as u64, &mut out);
        out.extend_from_slice(&self.recipe_seed.to_le_bytes());

        for key in &self.keys {
            out.push(key.root_quadrant);
            out.extend_from_slice(&key.root_seed.to_le_bytes());
        }

        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 4 + 1 + 1 + 8 {
            return Err(anyhow!("compact manifest too short"));
        }
        if &bytes[0..4] != MAGIC {
            return Err(anyhow!("compact manifest bad magic"));
        }
        if bytes[4] != VERSION {
            return Err(anyhow!("compact manifest unsupported version"));
        }

        let mut i = 6usize;

        let total_len = get_varint(bytes, &mut i)?;
        let chunk_bytes = get_varint(bytes, &mut i)?;
        let chunk_count = get_varint(bytes, &mut i)? as usize;

        if bytes.len() < i + 8 {
            return Err(anyhow!("compact manifest missing recipe_seed"));
        }
        let recipe_seed = u64::from_le_bytes(
            bytes[i..i + 8]
                .try_into()
                .map_err(|_| anyhow!("compact manifest bad recipe_seed"))?,
        );
        i += 8;

        let need = chunk_count
            .checked_mul(9)
            .ok_or_else(|| anyhow!("compact manifest chunk count overflow"))?;
        if bytes.len() < i + need {
            return Err(anyhow!("compact manifest truncated keys"));
        }

        let mut keys = Vec::with_capacity(chunk_count);
        for _ in 0..chunk_count {
            let root_quadrant = bytes[i];
            i += 1;
            let root_seed = u64::from_le_bytes(
                bytes[i..i + 8]
                    .try_into()
                    .map_err(|_| anyhow!("compact manifest bad root_seed"))?,
            );
            i += 8;
            keys.push(CompactChunkKey {
                root_quadrant,
                root_seed,
            });
        }

        if i != bytes.len() {
            return Err(anyhow!("compact manifest trailing bytes"));
        }

        Ok(Self {
            total_len,
            chunk_bytes,
            recipe_seed,
            keys,
        })
    }

    pub fn encoded_len(&self) -> usize {
        self.encode().len()
    }
}

pub fn render_compact_manifest_csv(manifest: &CompactChunkManifest) -> String {
    let mut out = String::from(
        "chunk_bytes,total_len,chunk_count,shared_recipe_seed_hex,chunk_index,root_quadrant,root_seed_hex\n",
    );

    for (idx, key) in manifest.keys.iter().enumerate() {
        out.push_str(&format!(
            "{},{},{},0x{:016X},{},{},0x{:016X}\n",
            manifest.chunk_bytes,
            manifest.total_len,
            manifest.keys.len(),
            manifest.recipe_seed,
            idx,
            key.root_quadrant,
            key.root_seed,
        ));
    }

    out
}

fn put_varint(mut v: u64, out: &mut Vec<u8>) {
    loop {
        let byte = (v & 0x7F) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            break;
        } else {
            out.push(byte | 0x80);
        }
    }
}

fn get_varint(bytes: &[u8], i: &mut usize) -> Result<u64> {
    let mut shift = 0u32;
    let mut out = 0u64;

    loop {
        if *i >= bytes.len() {
            return Err(anyhow!("compact manifest truncated varint"));
        }
        let b = bytes[*i];
        *i += 1;

        out |= ((b & 0x7F) as u64) << shift;

        if (b & 0x80) == 0 {
            return Ok(out);
        }

        shift += 7;
        if shift >= 64 {
            return Err(anyhow!("compact manifest varint overflow"));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CompactChunkKey, CompactChunkManifest};

    #[test]
    fn compact_manifest_roundtrips() {
        let m = CompactChunkManifest {
            total_len: 4201,
            chunk_bytes: 128,
            recipe_seed: 7,
            keys: vec![
                CompactChunkKey {
                    root_quadrant: 0,
                    root_seed: 11,
                },
                CompactChunkKey {
                    root_quadrant: 3,
                    root_seed: 22,
                },
            ],
        };

        let enc = m.encode();
        let dec = CompactChunkManifest::decode(&enc).unwrap();
        assert_eq!(m, dec);
    }
}