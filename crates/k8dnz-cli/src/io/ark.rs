use anyhow::Context;
use k8dnz_core::recipe::format as recipe_format;
use k8dnz_core::Recipe;

const MAGIC: &[u8; 4] = b"ARK1";

/// .ark layout (little-endian):
/// MAGIC[4]
/// recipe_len:u32
/// recipe_bytes[recipe_len]   (this is the K8R1 recipe blob, includes its own crc + blake3_16)
/// data_len:u64
/// data_bytes[data_len]       (ciphertext)
/// crc32:u32                  (over everything before crc32)
pub fn write_ark(path: &str, recipe: &Recipe, data: &[u8]) -> anyhow::Result<()> {
    let recipe_bytes = recipe_format::encode(recipe);

    let mut out = Vec::with_capacity(4 + 4 + recipe_bytes.len() + 8 + data.len() + 4);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&(recipe_bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(&recipe_bytes);
    out.extend_from_slice(&(data.len() as u64).to_le_bytes());
    out.extend_from_slice(data);

    let crc = crc32(&out);
    out.extend_from_slice(&crc.to_le_bytes());

    std::fs::write(path, out)?;
    Ok(())
}

/// Read ark and return (Recipe, data).
/// Validates:
/// - ARK1 magic
/// - ark crc32
/// - embedded recipe blob (via recipe_format::decode: magic + crc + blake3_16)
#[allow(dead_code)]
pub fn read_ark(path: &str) -> anyhow::Result<(Recipe, Vec<u8>)> {
    let (_rid, recipe, data) = read_ark_with_id(path)?;
    Ok((recipe, data))
}

/// Read ark and return (recipe_id_hex, Recipe, data).
pub fn read_ark_with_id(path: &str) -> anyhow::Result<(String, Recipe, Vec<u8>)> {
    let bytes = std::fs::read(path).with_context(|| format!("read {path}"))?;
    if bytes.len() < 4 + 4 + 8 + 4 {
        anyhow::bail!("ark too small");
    }
    if &bytes[0..4] != MAGIC {
        anyhow::bail!("bad ark magic");
    }

    // Verify ark crc
    let crc_off = bytes.len() - 4;
    let crc_expected = u32::from_le_bytes(bytes[crc_off..].try_into().unwrap());
    let crc_actual = crc32(&bytes[..crc_off]);
    if crc_expected != crc_actual {
        anyhow::bail!("ark crc32 mismatch");
    }

    let mut i = 4usize;

    // recipe_len + recipe bytes slice
    let recipe_len = read_u32(&bytes, &mut i)? as usize;
    let recipe_start = i;
    let recipe_end = recipe_start + recipe_len;
    if recipe_end > crc_off {
        anyhow::bail!("ark recipe_len out of range");
    }

    // Extract embedded recipe_id directly from the recipe blob (last 16 bytes of K8R1 recipe encoding)
    let rid = {
        let id16 = recipe_format::recipe_id_16_from_encoded(&bytes[recipe_start..recipe_end])?;
        hex16(&id16)
    };

    // Decode recipe (verifies embedded recipe crc/blake3)
    let recipe = recipe_format::decode(&bytes[recipe_start..recipe_end])?;
    i = recipe_end;

    // data_len + data bytes slice
    let data_len = read_u64(&bytes, &mut i)? as usize;
    let data_end = i + data_len;
    if data_end != crc_off {
        anyhow::bail!("ark data_len mismatch");
    }

    let data = bytes[i..data_end].to_vec();
    Ok((rid, recipe, data))
}

/// Return the embedded recipe_id (hex) from an ark file without decoding the recipe.
#[allow(dead_code)]
pub fn ark_recipe_id_hex(path: &str) -> anyhow::Result<String> {
    let bytes = std::fs::read(path).with_context(|| format!("read {path}"))?;
    if bytes.len() < 4 + 4 + 8 + 4 {
        anyhow::bail!("ark too small");
    }
    if &bytes[0..4] != MAGIC {
        anyhow::bail!("bad ark magic");
    }

    // Verify ark crc
    let crc_off = bytes.len() - 4;
    let crc_expected = u32::from_le_bytes(bytes[crc_off..].try_into().unwrap());
    let crc_actual = crc32(&bytes[..crc_off]);
    if crc_expected != crc_actual {
        anyhow::bail!("ark crc32 mismatch");
    }

    let mut i = 4usize;
    let recipe_len = read_u32(&bytes, &mut i)? as usize;
    let recipe_start = i;
    let recipe_end = recipe_start + recipe_len;
    if recipe_end > crc_off {
        anyhow::bail!("ark recipe_len out of range");
    }

    let id16 = recipe_format::recipe_id_16_from_encoded(&bytes[recipe_start..recipe_end])?;
    Ok(hex16(&id16))
}

/// Generate N keystream bytes from the engine.
/// One emission -> one byte (N=16 packed nybbles).
pub fn keystream_bytes(
    engine: &mut k8dnz_core::Engine,
    n: usize,
    max_ticks: u64,
) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(n);
    while out.len() < n && engine.stats.ticks < max_ticks {
        if let Some(tok) = engine.step() {
            out.push(((tok.a & 0x0F) << 4) | (tok.b & 0x0F));
        }
    }
    if out.len() != n {
        anyhow::bail!(
            "keystream short: need {} bytes, got {} (ticks={}, emissions={})",
            n,
            out.len(),
            engine.stats.ticks,
            engine.stats.emissions
        );
    }
    Ok(out)
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut h = crc32fast::Hasher::new();
    h.update(bytes);
    h.finalize()
}

fn read_u32(bytes: &[u8], i: &mut usize) -> anyhow::Result<u32> {
    if bytes.len() < *i + 4 {
        anyhow::bail!("unexpected eof");
    }
    let v = u32::from_le_bytes(bytes[*i..*i + 4].try_into().unwrap());
    *i += 4;
    Ok(v)
}

fn read_u64(bytes: &[u8], i: &mut usize) -> anyhow::Result<u64> {
    if bytes.len() < *i + 8 {
        anyhow::bail!("unexpected eof");
    }
    let v = u64::from_le_bytes(bytes[*i..*i + 8].try_into().unwrap());
    *i += 8;
    Ok(v)
}

fn hex16(id: &[u8; 16]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(32);
    for &b in id {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0F) as usize] as char);
    }
    s
}
