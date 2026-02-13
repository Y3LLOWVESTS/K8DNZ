// crates/k8dnz-core/src/recipe/ark_key.rs

use crate::error::{K8Error, Result};
use crate::fixed::turn32::Turn32;
use crate::recipe::checksum::crc32;
use crate::recipe::recipe::{Alphabet, KeystreamMix, PayloadKind, Recipe, ResetMode};

const PREFIX: &str = "ARK1S:";

pub fn encode_ark1s(recipe: &Recipe) -> String {
    let mut b = Vec::with_capacity(160);

    // string-format version (not recipe.version)
    b.push(0u8);

    // recipe.version is u16
    b.extend_from_slice(&recipe.version.to_le_bytes());

    // enums as explicit bytes (stable)
    b.push(match recipe.alphabet {
        Alphabet::N16 => 0,
    });
    b.push(match recipe.reset_mode {
        ResetMode::HoldAandC => 0,
        ResetMode::FromLockstep => 1,
    });
    b.push(match recipe.keystream_mix {
        KeystreamMix::None => 0,
        KeystreamMix::SplitMix64 => 1,
    });
    b.push(match recipe.payload_kind {
        PayloadKind::CipherXor => 0,
        PayloadKind::ResidualXor => 1,
    });

    b.extend_from_slice(&recipe.seed.to_le_bytes());

    // free orbit
    put_turn32(&mut b, recipe.free.phi_a0);
    put_turn32(&mut b, recipe.free.phi_c0);
    put_turn32(&mut b, recipe.free.v_a);
    put_turn32(&mut b, recipe.free.v_c);
    put_turn32(&mut b, recipe.free.epsilon);

    // lockstep
    put_turn32(&mut b, recipe.lock.v_l);
    put_turn32(&mut b, recipe.lock.delta);
    b.extend_from_slice(&recipe.lock.t_step.to_le_bytes());

    // field clamp + quant
    b.extend_from_slice(&recipe.field_clamp.min.to_le_bytes());
    b.extend_from_slice(&recipe.field_clamp.max.to_le_bytes());

    b.extend_from_slice(&recipe.quant.min.to_le_bytes());
    b.extend_from_slice(&recipe.quant.max.to_le_bytes());
    b.extend_from_slice(&recipe.quant.shift.to_le_bytes());

    // field waves
    let waves_len: u16 = recipe
        .field
        .waves
        .len()
        .try_into()
        .map_err(|_| K8Error::Validation("ark1s: too many waves".into()))
        .unwrap();
    b.extend_from_slice(&waves_len.to_le_bytes());
    for w in &recipe.field.waves {
        b.extend_from_slice(&w.k_phi.to_le_bytes());
        b.extend_from_slice(&w.k_t.to_le_bytes());
        b.extend_from_slice(&w.k_time.to_le_bytes());
        b.extend_from_slice(&w.phase.to_le_bytes());
        b.extend_from_slice(&w.amp.to_le_bytes());
    }

    // crc32 over everything so far
    let c = crc32(&b);
    b.extend_from_slice(&c.to_le_bytes());

    let body = crock32_encode(&b);
    format!("{PREFIX}{body}")
}

pub fn decode_ark1s(s: &str) -> Result<Recipe> {
    let body = s
        .strip_prefix(PREFIX)
        .ok_or_else(|| K8Error::Validation("ark1s: missing ARK1S: prefix".into()))?;

    let bytes = crock32_decode(body)?;
    if bytes.len() < 1 + 2 + 4 {
        return Err(K8Error::Validation("ark1s: too small".into()));
    }

    let crc_off = bytes.len() - 4;
    let crc_expected = u32::from_le_bytes(bytes[crc_off..].try_into().unwrap());
    let crc_actual = crc32(&bytes[..crc_off]);
    if crc_expected != crc_actual {
        return Err(K8Error::Validation("ark1s: crc32 mismatch".into()));
    }

    let mut i = 0usize;

    let _fmt_ver = read_u8(&bytes, &mut i)?;
    let recipe_ver = read_u16(&bytes, &mut i)?;

    let alphabet = match read_u8(&bytes, &mut i)? {
        0 => Alphabet::N16,
        _ => return Err(K8Error::Validation("ark1s: bad alphabet".into())),
    };
    let reset_mode = match read_u8(&bytes, &mut i)? {
        0 => ResetMode::HoldAandC,
        1 => ResetMode::FromLockstep,
        _ => return Err(K8Error::Validation("ark1s: bad reset_mode".into())),
    };
    let keystream_mix = match read_u8(&bytes, &mut i)? {
        0 => KeystreamMix::None,
        1 => KeystreamMix::SplitMix64,
        _ => return Err(K8Error::Validation("ark1s: bad keystream_mix".into())),
    };
    let payload_kind = match read_u8(&bytes, &mut i)? {
        0 => PayloadKind::CipherXor,
        1 => PayloadKind::ResidualXor,
        _ => return Err(K8Error::Validation("ark1s: bad payload_kind".into())),
    };

    let seed = read_u64(&bytes, &mut i)?;

    let phi_a0 = read_turn32(&bytes, &mut i)?;
    let phi_c0 = read_turn32(&bytes, &mut i)?;
    let v_a = read_turn32(&bytes, &mut i)?;
    let v_c = read_turn32(&bytes, &mut i)?;
    let epsilon = read_turn32(&bytes, &mut i)?;

    let v_l = read_turn32(&bytes, &mut i)?;
    let delta = read_turn32(&bytes, &mut i)?;
    let t_step = read_u32(&bytes, &mut i)?;

    let field_clamp_min = read_i64(&bytes, &mut i)?;
    let field_clamp_max = read_i64(&bytes, &mut i)?;

    let quant_min = read_i64(&bytes, &mut i)?;
    let quant_max = read_i64(&bytes, &mut i)?;
    let quant_shift = read_i64(&bytes, &mut i)?;

    let waves_len = read_u16(&bytes, &mut i)? as usize;
    let mut waves = Vec::with_capacity(waves_len);
    for _ in 0..waves_len {
        let k_phi = read_u32(&bytes, &mut i)?;
        let k_t = read_u32(&bytes, &mut i)?;
        let k_time = read_u32(&bytes, &mut i)?;
        let phase = read_u32(&bytes, &mut i)?;
        let amp = read_i32(&bytes, &mut i)?;
        waves.push(crate::recipe::recipe::FieldWave {
            k_phi,
            k_t,
            k_time,
            phase,
            amp,
        });
    }

    Ok(Recipe {
        version: recipe_ver,
        seed,
        alphabet,
        reset_mode,
        keystream_mix,
        payload_kind,
        free: crate::recipe::recipe::FreeOrbitParams {
            v_a,
            v_c,
            phi_a0,
            phi_c0,
            epsilon,
        },
        lock: crate::recipe::recipe::LockstepParams { v_l, delta, t_step },
        field: crate::recipe::recipe::FieldParams { waves },
        field_clamp: crate::recipe::recipe::FieldClampParams {
            min: field_clamp_min,
            max: field_clamp_max,
        },
        quant: crate::recipe::recipe::QuantParams {
            min: quant_min,
            max: quant_max,
            shift: quant_shift,
        },
        rgb: Default::default(),
    })
}

fn put_turn32(b: &mut Vec<u8>, t: Turn32) {
    b.extend_from_slice(&t.0.to_le_bytes());
}

fn read_turn32(bytes: &[u8], i: &mut usize) -> Result<Turn32> {
    Ok(Turn32(read_u32(bytes, i)?))
}

fn read_u8(bytes: &[u8], i: &mut usize) -> Result<u8> {
    if *i + 1 > bytes.len() {
        return Err(K8Error::Validation("ark1s: unexpected eof".into()));
    }
    let v = bytes[*i];
    *i += 1;
    Ok(v)
}

fn read_u16(bytes: &[u8], i: &mut usize) -> Result<u16> {
    if *i + 2 > bytes.len() {
        return Err(K8Error::Validation("ark1s: unexpected eof".into()));
    }
    let v = u16::from_le_bytes(bytes[*i..*i + 2].try_into().unwrap());
    *i += 2;
    Ok(v)
}

fn read_u32(bytes: &[u8], i: &mut usize) -> Result<u32> {
    if *i + 4 > bytes.len() {
        return Err(K8Error::Validation("ark1s: unexpected eof".into()));
    }
    let v = u32::from_le_bytes(bytes[*i..*i + 4].try_into().unwrap());
    *i += 4;
    Ok(v)
}

fn read_i32(bytes: &[u8], i: &mut usize) -> Result<i32> {
    if *i + 4 > bytes.len() {
        return Err(K8Error::Validation("ark1s: unexpected eof".into()));
    }
    let v = i32::from_le_bytes(bytes[*i..*i + 4].try_into().unwrap());
    *i += 4;
    Ok(v)
}

fn read_u64(bytes: &[u8], i: &mut usize) -> Result<u64> {
    if *i + 8 > bytes.len() {
        return Err(K8Error::Validation("ark1s: unexpected eof".into()));
    }
    let v = u64::from_le_bytes(bytes[*i..*i + 8].try_into().unwrap());
    *i += 8;
    Ok(v)
}

fn read_i64(bytes: &[u8], i: &mut usize) -> Result<i64> {
    if *i + 8 > bytes.len() {
        return Err(K8Error::Validation("ark1s: unexpected eof".into()));
    }
    let v = i64::from_le_bytes(bytes[*i..*i + 8].try_into().unwrap());
    *i += 8;
    Ok(v)
}

// --- Crockford Base32 (no padding) ---
const CROCK: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

fn crock32_encode(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return String::new();
    }

    let mut out = String::new();
    let mut acc: u32 = 0;
    let mut bits: u8 = 0;

    for &b in bytes {
        acc = (acc << 8) | (b as u32);
        bits += 8;

        while bits >= 5 {
            let shift = bits - 5;
            let idx = ((acc >> shift) & 0x1F) as usize;
            out.push(CROCK[idx] as char);
            bits -= 5;
            acc &= (1u32 << bits) - 1;
        }
    }

    if bits > 0 {
        let idx = ((acc << (5 - bits)) & 0x1F) as usize;
        out.push(CROCK[idx] as char);
    }

    out
}

fn crock32_decode(s: &str) -> Result<Vec<u8>> {
    if s.is_empty() {
        return Ok(Vec::new());
    }

    let mut out: Vec<u8> = Vec::with_capacity(s.len() * 5 / 8);
    let mut acc: u32 = 0;
    let mut bits: u8 = 0;

    for ch in s.bytes() {
        let v = crock_val(ch)
            .ok_or_else(|| K8Error::Validation("ark1s: invalid base32 char".into()))?;
        acc = (acc << 5) | (v as u32);
        bits += 5;

        while bits >= 8 {
            let shift = bits - 8;
            let b = ((acc >> shift) & 0xFF) as u8;
            out.push(b);
            bits -= 8;
            acc &= (1u32 << bits) - 1;
        }
    }

    Ok(out)
}

fn crock_val(ch: u8) -> Option<u8> {
    let c = match ch {
        b'a'..=b'z' => ch - 32,
        _ => ch,
    };

    match c {
        b'0' => Some(0),
        b'1' => Some(1),
        b'2' => Some(2),
        b'3' => Some(3),
        b'4' => Some(4),
        b'5' => Some(5),
        b'6' => Some(6),
        b'7' => Some(7),
        b'8' => Some(8),
        b'9' => Some(9),

        b'A' => Some(10),
        b'B' => Some(11),
        b'C' => Some(12),
        b'D' => Some(13),
        b'E' => Some(14),
        b'F' => Some(15),
        b'G' => Some(16),
        b'H' => Some(17),

        b'J' => Some(18),
        b'K' => Some(19),
        b'M' => Some(20),
        b'N' => Some(21),
        b'P' => Some(22),
        b'Q' => Some(23),
        b'R' => Some(24),
        b'S' => Some(25),
        b'T' => Some(26),
        b'V' => Some(27),
        b'W' => Some(28),
        b'X' => Some(29),
        b'Y' => Some(30),
        b'Z' => Some(31),

        _ => None,
    }
}
