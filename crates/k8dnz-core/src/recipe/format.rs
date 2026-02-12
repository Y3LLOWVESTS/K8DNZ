// crates/k8dnz-core/src/recipe/format.rs

use crate::error::{K8Error, Result};
use crate::fixed::turn32::Turn32;
use crate::recipe::checksum::{blake3_16, crc32};
use crate::recipe::recipe::*;

const MAGIC: &[u8; 4] = b"K8R1";

/// Minimal binary-stable format (owned).
/// Layout (little-endian):
/// MAGIC[4]
/// version:u16
/// flags:u16          (alphabet/reset_mode)
/// seed:u64
/// free: phi_a0:u32 phi_c0:u32 v_a:u32 v_c:u32 epsilon:u32
/// lock: v_l:u32 delta:u32 t_step:u32
/// [v3+] field_clamp: fmin:i64 fmax:i64
/// [v2+] quant: qmin:i64 qmax:i64
/// [v4+] qshift:i64
/// waves_len:u16
/// waves: repeated { k_phi:u32 k_t:u32 k_time:u32 phase:u32 amp:i32 }
/// crc32:u32          (over everything before crc32)
/// blake3_16:[16]     (over everything before blake3)
///
/// NOTE: RGB params are currently NOT encoded in this binary format.
/// They are injected via defaults on decode() for back-compat.
/// When we bump the format, we’ll extend the layout under a new version gate.
pub fn encode(r: &Recipe) -> Vec<u8> {
    let mut b = Vec::with_capacity(256);
    b.extend_from_slice(MAGIC);

    b.extend_from_slice(&r.version.to_le_bytes());

    let flags: u16 = pack_flags(r.alphabet, r.reset_mode);
    b.extend_from_slice(&flags.to_le_bytes());

    b.extend_from_slice(&r.seed.to_le_bytes());

    b.extend_from_slice(&r.free.phi_a0.0.to_le_bytes());
    b.extend_from_slice(&r.free.phi_c0.0.to_le_bytes());
    b.extend_from_slice(&r.free.v_a.0.to_le_bytes());
    b.extend_from_slice(&r.free.v_c.0.to_le_bytes());
    b.extend_from_slice(&r.free.epsilon.0.to_le_bytes());

    b.extend_from_slice(&r.lock.v_l.0.to_le_bytes());
    b.extend_from_slice(&r.lock.delta.0.to_le_bytes());
    b.extend_from_slice(&r.lock.t_step.to_le_bytes());

    // v3+ field clamp
    if r.version >= 3 {
        b.extend_from_slice(&r.field_clamp.min.to_le_bytes());
        b.extend_from_slice(&r.field_clamp.max.to_le_bytes());
    }

    // v2+ quant range
    if r.version >= 2 {
        b.extend_from_slice(&r.quant.min.to_le_bytes());
        b.extend_from_slice(&r.quant.max.to_le_bytes());
    }

    // v4+ quant shift
    if r.version >= 4 {
        b.extend_from_slice(&r.quant.shift.to_le_bytes());
    }

    let waves_len: u16 = r.field.waves.len().min(u16::MAX as usize) as u16;
    b.extend_from_slice(&waves_len.to_le_bytes());
    for w in r.field.waves.iter().take(waves_len as usize) {
        b.extend_from_slice(&w.k_phi.to_le_bytes());
        b.extend_from_slice(&w.k_t.to_le_bytes());
        b.extend_from_slice(&w.k_time.to_le_bytes());
        b.extend_from_slice(&w.phase.to_le_bytes());
        b.extend_from_slice(&w.amp.to_le_bytes());
    }

    let c = crc32(&b);
    b.extend_from_slice(&c.to_le_bytes());

    let h = blake3_16(&b);
    b.extend_from_slice(&h);

    b
}

pub fn decode(bytes: &[u8]) -> Result<Recipe> {
    let mut i = 0usize;
    if bytes.len() < 4 || &bytes[0..4] != MAGIC {
        return Err(K8Error::RecipeFormat("bad magic".into()));
    }
    i += 4;

    let version = read_u16(bytes, &mut i)?;
    let flags = read_u16(bytes, &mut i)?;
    let (alphabet, reset_mode) = unpack_flags(flags)?;

    let seed = read_u64(bytes, &mut i)?;

    let phi_a0 = Turn32(read_u32(bytes, &mut i)?);
    let phi_c0 = Turn32(read_u32(bytes, &mut i)?);
    let v_a = Turn32(read_u32(bytes, &mut i)?);
    let v_c = Turn32(read_u32(bytes, &mut i)?);
    let epsilon = Turn32(read_u32(bytes, &mut i)?);

    let v_l = Turn32(read_u32(bytes, &mut i)?);
    let delta = Turn32(read_u32(bytes, &mut i)?);
    let t_step = read_u32(bytes, &mut i)?;

    // Back-compat defaults
    let mut field_clamp = FieldClampParams { min: -100_000_000, max: 100_000_000 };
    let mut quant = QuantParams { min: -100_000_000, max: 100_000_000, shift: 0 };

    if version >= 3 {
        if bytes.len() < i + 16 {
            return Err(K8Error::RecipeFormat("unexpected eof reading field_clamp".into()));
        }
        field_clamp.min = read_i64(bytes, &mut i)?;
        field_clamp.max = read_i64(bytes, &mut i)?;
    }

    if version >= 2 {
        if bytes.len() < i + 16 {
            return Err(K8Error::RecipeFormat("unexpected eof reading quant".into()));
        }
        quant.min = read_i64(bytes, &mut i)?;
        quant.max = read_i64(bytes, &mut i)?;
    }

    // v4+ quant shift (applies to BOTH min and max at quantization time)
    if version >= 4 {
        if bytes.len() < i + 8 {
            return Err(K8Error::RecipeFormat("unexpected eof reading qshift".into()));
        }
        quant.shift = read_i64(bytes, &mut i)?;
    } else {
        // v1..v3 recipes have no shift; default to 0 for back-compat.
        quant.shift = 0;
    }

    let waves_len = read_u16(bytes, &mut i)? as usize;
    let mut waves = Vec::with_capacity(waves_len);
    for _ in 0..waves_len {
        let k_phi = read_u32(bytes, &mut i)?;
        let k_t = read_u32(bytes, &mut i)?;
        let k_time = read_u32(bytes, &mut i)?;
        let phase = read_u32(bytes, &mut i)?;
        let amp = read_i32(bytes, &mut i)?;
        waves.push(FieldWave { k_phi, k_t, k_time, phase, amp });
    }

    // Verify crc32
    let crc_expected = read_u32(bytes, &mut i)?;
    let crc_actual = crc32(&bytes[0..(i - 4)]);
    if crc_expected != crc_actual {
        return Err(K8Error::RecipeFormat("crc32 mismatch".into()));
    }

    // Verify blake3_16
    if bytes.len() < i + 16 {
        return Err(K8Error::RecipeFormat("missing blake3".into()));
    }
    let mut h_expected = [0u8; 16];
    h_expected.copy_from_slice(&bytes[i..i + 16]);
    let h_actual = blake3_16(&bytes[0..i]);
    if h_expected != h_actual {
        return Err(K8Error::RecipeFormat("blake3 mismatch".into()));
    }

    Ok(Recipe {
        version,
        seed,
        alphabet,
        reset_mode,
        free: FreeOrbitParams { phi_a0, phi_c0, v_a, v_c, epsilon },
        lock: LockstepParams { v_l, delta, t_step },
        field: FieldParams { waves },
        field_clamp,
        quant,

        // Back-compat injection: RGB params are not yet in the binary format.
        // Deterministic defaults keep old recipes loading cleanly.
        rgb: RgbRecipe::default(),
    })
}

/// A stable recipe identifier: the trailing blake3_16 that `encode()` appends.
/// This is canonical because it is computed over the CRC-inclusive payload bytes.
pub fn recipe_id_16(r: &Recipe) -> [u8; 16] {
    let enc = encode(r);
    // encode() always appends 16 bytes.
    enc[enc.len() - 16..].try_into().unwrap()
}

pub fn recipe_id_hex(r: &Recipe) -> String {
    let id = recipe_id_16(r);
    hex16(&id)
}

pub fn recipe_id_16_from_encoded(encoded: &[u8]) -> Result<[u8; 16]> {
    if encoded.len() < 16 {
        return Err(K8Error::RecipeFormat("encoded recipe too small for id".into()));
    }
    let mut out = [0u8; 16];
    out.copy_from_slice(&encoded[encoded.len() - 16..]);
    Ok(out)
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

fn pack_flags(a: Alphabet, r: ResetMode) -> u16 {
    let a_bits = match a {
        Alphabet::N16 => 0u16,
    };
    let r_bits = match r {
        ResetMode::HoldAandC => 0u16,
        ResetMode::FromLockstep => 1u16,
    };
    a_bits | (r_bits << 8)
}

fn unpack_flags(flags: u16) -> Result<(Alphabet, ResetMode)> {
    let a = match flags & 0x00FF {
        0 => Alphabet::N16,
        _ => return Err(K8Error::RecipeFormat("unknown alphabet".into())),
    };
    let r = match (flags >> 8) & 0x00FF {
        0 => ResetMode::HoldAandC,
        1 => ResetMode::FromLockstep,
        _ => return Err(K8Error::RecipeFormat("unknown reset mode".into())),
    };
    Ok((a, r))
}

fn need(bytes: &[u8], i: usize, n: usize) -> Result<()> {
    if bytes.len() < i + n {
        return Err(K8Error::RecipeFormat("unexpected eof".into()));
    }
    Ok(())
}

fn read_u16(bytes: &[u8], i: &mut usize) -> Result<u16> {
    need(bytes, *i, 2)?;
    let v = u16::from_le_bytes(bytes[*i..*i + 2].try_into().unwrap());
    *i += 2;
    Ok(v)
}

fn read_u32(bytes: &[u8], i: &mut usize) -> Result<u32> {
    need(bytes, *i, 4)?;
    let v = u32::from_le_bytes(bytes[*i..*i + 4].try_into().unwrap());
    *i += 4;
    Ok(v)
}

fn read_u64(bytes: &[u8], i: &mut usize) -> Result<u64> {
    need(bytes, *i, 8)?;
    let v = u64::from_le_bytes(bytes[*i..*i + 8].try_into().unwrap());
    *i += 8;
    Ok(v)
}

fn read_i32(bytes: &[u8], i: &mut usize) -> Result<i32> {
    need(bytes, *i, 4)?;
    let v = i32::from_le_bytes(bytes[*i..*i + 4].try_into().unwrap());
    *i += 4;
    Ok(v)
}

fn read_i64(bytes: &[u8], i: &mut usize) -> Result<i64> {
    need(bytes, *i, 8)?;
    let v = i64::from_le_bytes(bytes[*i..*i + 8].try_into().unwrap());
    *i += 8;
    Ok(v)
}
