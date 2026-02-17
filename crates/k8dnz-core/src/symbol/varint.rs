// crates/k8dnz-core/src/symbol/varint.rs
//
// Minimal unsigned varint (LEB128-like) for compact patch encoding.

use crate::error::{K8Error, Result};

pub fn put_u64(mut v: u64, out: &mut Vec<u8>) {
    while v >= 0x80 {
        out.push(((v as u8) & 0x7F) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

pub fn get_u64(bytes: &[u8], i: &mut usize) -> Result<u64> {
    let mut acc: u64 = 0;
    let mut shift: u32 = 0;

    loop {
        if *i >= bytes.len() {
            return Err(K8Error::Validation("varint: eof".into()));
        }
        let b = bytes[*i];
        *i += 1;

        let low = (b & 0x7F) as u64;
        if shift >= 64 || ((low << shift) >> shift) != low {
            return Err(K8Error::Validation("varint: overflow".into()));
        }
        acc |= low << shift;

        if (b & 0x80) == 0 {
            return Ok(acc);
        }
        shift += 7;
        if shift > 63 {
            return Err(K8Error::Validation("varint: too long".into()));
        }
    }
}
