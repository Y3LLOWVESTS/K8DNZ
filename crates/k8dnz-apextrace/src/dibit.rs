use crate::error::{ApexError, Result};

pub const QUAT_1: u8 = 1;
pub const QUAT_2: u8 = 2;
pub const QUAT_3: u8 = 3;
pub const QUAT_4: u8 = 4;

#[inline]
pub fn dibit_to_quat(v: u8) -> Result<u8> {
    match v & 0b11 {
        0b00 => Ok(QUAT_1),
        0b01 => Ok(QUAT_2),
        0b10 => Ok(QUAT_3),
        0b11 => Ok(QUAT_4),
        _ => Err(ApexError::Validation("invalid dibit".into())),
    }
}

#[inline]
pub fn quat_to_dibit(v: u8) -> Result<u8> {
    match v {
        QUAT_1 => Ok(0b00),
        QUAT_2 => Ok(0b01),
        QUAT_3 => Ok(0b10),
        QUAT_4 => Ok(0b11),
        _ => Err(ApexError::Validation(format!(
            "invalid quaternary symbol {v}; expected 1..=4"
        ))),
    }
}

pub fn bytes_to_quats(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(bytes.len().saturating_mul(4));
    for &b in bytes {
        out.push(dibit_to_quat((b >> 6) & 0b11)?);
        out.push(dibit_to_quat((b >> 4) & 0b11)?);
        out.push(dibit_to_quat((b >> 2) & 0b11)?);
        out.push(dibit_to_quat(b & 0b11)?);
    }
    Ok(out)
}

pub fn quats_to_bytes(quats: &[u8]) -> Result<Vec<u8>> {
    if quats.len() % 4 != 0 {
        return Err(ApexError::Validation(format!(
            "quaternary stream length {} is not divisible by 4",
            quats.len()
        )));
    }

    let mut out = Vec::with_capacity(quats.len() / 4);
    for chunk in quats.chunks_exact(4) {
        let mut b = 0u8;
        b |= quat_to_dibit(chunk[0])? << 6;
        b |= quat_to_dibit(chunk[1])? << 4;
        b |= quat_to_dibit(chunk[2])? << 2;
        b |= quat_to_dibit(chunk[3])?;
        out.push(b);
    }

    Ok(out)
}