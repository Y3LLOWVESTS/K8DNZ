// crates/k8dnz-core/src/symbol/patch.rs
//
// Sparse mismatch patch list:
// Store only mismatches between predicted and actual symbol streams.
//
// Encoding (bytes):
//   count: varint
//   repeated count times:
//     pos_delta: varint   (delta from previous mismatch pos; first is absolute pos)
//     value: varint       (actual symbol value)
//
// This is very small when mismatches are rare.

use crate::error::{K8Error, Result};
use crate::symbol::varint;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PatchList {
    pub entries: Vec<(u64, u64)>, // (pos, value)
}

impl PatchList {
    pub fn new() -> Self {
        Self { entries: Vec::new() }
    }

    pub fn from_pred_actual(pred: &[u8], actual: &[u8]) -> Result<Self> {
        if pred.len() != actual.len() {
            return Err(K8Error::Validation("patch: pred/actual len mismatch".into()));
        }
        let mut pl = PatchList::new();
        for (i, (&p, &a)) in pred.iter().zip(actual.iter()).enumerate() {
            if p != a {
                pl.entries.push((i as u64, a as u64));
            }
        }
        Ok(pl)
    }

    pub fn apply_to_pred(&self, pred: &mut [u8]) -> Result<()> {
        for &(pos, value) in &self.entries {
            let idx = pos as usize;
            if idx >= pred.len() {
                return Err(K8Error::Validation("patch: position out of range".into()));
            }
            pred[idx] = (value & 0xFF) as u8;
        }
        Ok(())
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        varint::put_u64(self.entries.len() as u64, &mut out);

        let mut prev: u64 = 0;
        for (k, &(pos, value)) in self.entries.iter().enumerate() {
            let delta = if k == 0 { pos } else { pos.saturating_sub(prev) };
            varint::put_u64(delta, &mut out);
            varint::put_u64(value, &mut out);
            prev = pos;
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut i = 0usize;
        let count = varint::get_u64(bytes, &mut i)? as usize;

        let mut entries = Vec::with_capacity(count);
        let mut pos: u64 = 0;

        for k in 0..count {
            let delta = varint::get_u64(bytes, &mut i)?;
            if k == 0 {
                pos = delta;
            } else {
                pos = pos.saturating_add(delta);
            }
            let value = varint::get_u64(bytes, &mut i)?;
            entries.push((pos, value));
        }

        if i != bytes.len() {
            return Err(K8Error::Validation("patch: trailing bytes".into()));
        }

        Ok(Self { entries })
    }
}
