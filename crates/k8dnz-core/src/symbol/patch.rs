// crates/k8dnz-core/src/symbol/patch.rs
//
// Patch encoding for correcting predicted symbol streams.
//
// We support two wire formats:
//
// 1) Legacy SPARSE (backward-compatible):
//   count: varint
//   repeated count times:
//     pos_delta: varint   (delta from previous mismatch pos; first is absolute pos)
//     value: varint       (actual symbol value)
//
// 2) DENSE (new; auto-selected when smaller):
//   sentinel: varint = u64::MAX
//   fmt: varint = 2
//   len: varint          (stream length in symbols)
//   bitmap_len: varint   (bytes, should be ceil(len/8))
//   bitmap[bitmap_len]   (1 bit per position; 1 => mismatch at that position)
//   values_count: varint (must equal popcount(bitmap))
//   values[values_count] (u8 each; actual symbol values at mismatch positions, in increasing pos order)
//
// Notes:
// - New decode can read legacy sparse and new dense.
// - Old decode cannot read new dense (that’s fine; we only require forward-compat).
// - In-memory representation remains a sparse list of (pos,value) to avoid changing callers.

use crate::error::{K8Error, Result};
use crate::symbol::varint;

const SENTINEL_NEWFMT: u64 = u64::MAX;
const FMT_SPARSE: u64 = 1;
const FMT_DENSE: u64 = 2;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PatchList {
    /// (pos, value) where value is the ACTUAL symbol at that position.
    pub entries: Vec<(u64, u64)>,
    /// Stream length in symbols (needed to evaluate dense encoding).
    /// For legacy-decoded patches this may be 0 (unknown).
    pub len: u64,
}

impl PatchList {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            len: 0,
        }
    }

    pub fn from_pred_actual(pred: &[u8], actual: &[u8]) -> Result<Self> {
        if pred.len() != actual.len() {
            return Err(K8Error::Validation("patch: pred/actual len mismatch".into()));
        }
        let mut pl = PatchList {
            entries: Vec::new(),
            len: pred.len() as u64,
        };
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

    /// Encodes using whichever format is smaller (legacy sparse vs new dense),
    /// when `self.len` is known (>0). If len is unknown, falls back to sparse.
    pub fn encode(&self) -> Vec<u8> {
        let sparse = self.encode_sparse_legacy();

        // If we don't know the stream length, we can't build a correct dense mask.
        if self.len == 0 {
            return sparse;
        }

        let dense = self.encode_dense();

        if dense.len() < sparse.len() {
            dense
        } else {
            sparse
        }
    }

    /// Legacy sparse encoding (exactly the old format).
    pub fn encode_sparse_legacy(&self) -> Vec<u8> {
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

    /// Dense encoding (bitmask + u8 values), with a sentinel header.
    pub fn encode_dense(&self) -> Vec<u8> {
        let len = self.len as usize;
        let m = self.entries.len();

        // Bitmap is ceil(len/8).
        let bitmap_len = (len + 7) / 8;
        let mut bitmap = vec![0u8; bitmap_len];

        // Fill bitmap from entries (assume entries are in increasing order; they are by construction).
        for &(pos, _value) in &self.entries {
            let p = pos as usize;
            if p >= len {
                // Should never happen if caller built from pred/actual of the same length.
                // But keep encoding safe/total.
                continue;
            }
            bitmap[p >> 3] |= 1u8 << (p & 7);
        }

        // Values as u8 (all your lane symbols are u8-range).
        let mut values: Vec<u8> = Vec::with_capacity(m);
        for &(_pos, value) in &self.entries {
            values.push((value & 0xFF) as u8);
        }

        let mut out = Vec::new();
        // sentinel + fmt + len
        varint::put_u64(SENTINEL_NEWFMT, &mut out);
        varint::put_u64(FMT_DENSE, &mut out);
        varint::put_u64(self.len, &mut out);

        // bitmap
        varint::put_u64(bitmap_len as u64, &mut out);
        out.extend_from_slice(&bitmap);

        // values
        varint::put_u64(values.len() as u64, &mut out);
        out.extend_from_slice(&values);

        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut i = 0usize;

        // Read first varint; it is either:
        // - legacy "count"
        // - or sentinel u64::MAX for new formats
        let first = varint::get_u64(bytes, &mut i)?;

        if first == SENTINEL_NEWFMT {
            // New format
            let fmt = varint::get_u64(bytes, &mut i)?;
            match fmt {
                FMT_SPARSE => {
                    // Reserved for future (a tagged sparse could carry len). For now treat as error.
                    return Err(K8Error::Validation("patch: tagged sparse not implemented".into()));
                }
                FMT_DENSE => {
                    let len = varint::get_u64(bytes, &mut i)?;
                    let bitmap_len = varint::get_u64(bytes, &mut i)? as usize;

                    if i + bitmap_len > bytes.len() {
                        return Err(K8Error::Validation("patch: dense bitmap OOB".into()));
                    }
                    let bitmap = &bytes[i..i + bitmap_len];
                    i += bitmap_len;

                    let values_count = varint::get_u64(bytes, &mut i)? as usize;
                    if i + values_count > bytes.len() {
                        return Err(K8Error::Validation("patch: dense values OOB".into()));
                    }
                    let values = &bytes[i..i + values_count];
                    i += values_count;

                    if i != bytes.len() {
                        return Err(K8Error::Validation("patch: trailing bytes".into()));
                    }

                    // Validate bitmap length matches len (allow a larger bitmap only if extra bits are zero).
                    let need_bitmap_len = ((len as usize) + 7) / 8;
                    if bitmap_len < need_bitmap_len {
                        return Err(K8Error::Validation("patch: dense bitmap too short".into()));
                    }
                    // Count bits only up to len.
                    let pop = popcount_bitmap_prefix(bitmap, len as usize);
                    if pop != values_count {
                        return Err(K8Error::Validation(format!(
                            "patch: dense values_count mismatch (popcount={} values_count={})",
                            pop, values_count
                        )));
                    }

                    // Expand into sparse entries for compatibility with existing callers.
                    let mut entries: Vec<(u64, u64)> = Vec::with_capacity(values_count);
                    let mut v_ix = 0usize;

                    let n = len as usize;
                    for pos in 0..n {
                        let b = bitmap[pos >> 3];
                        let bit = (b >> (pos & 7)) & 1;
                        if bit == 1 {
                            let val = values[v_ix] as u64;
                            v_ix += 1;
                            entries.push((pos as u64, val));
                        }
                    }

                    return Ok(Self { entries, len });
                }
                _ => {
                    return Err(K8Error::Validation(format!("patch: unknown fmt={}", fmt)));
                }
            }
        }

        // Legacy sparse decode: `first` is the mismatch count.
        let count = first as usize;
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

        // Length unknown in legacy format.
        Ok(Self { entries, len: 0 })
    }
}

// Popcount only up to `n_bits` bits (ignore trailing bits in last byte).
fn popcount_bitmap_prefix(bitmap: &[u8], n_bits: usize) -> usize {
    if n_bits == 0 {
        return 0;
    }
    let full_bytes = n_bits / 8;
    let tail_bits = n_bits % 8;

    let mut c = 0usize;

    for &b in bitmap.iter().take(full_bytes) {
        c += b.count_ones() as usize;
    }

    if tail_bits != 0 {
        if let Some(&b) = bitmap.get(full_bytes) {
            let mask = (1u16 << tail_bits) - 1;
            let bb = (b as u16) & mask;
            c += (bb as u8).count_ones() as usize;
        }
    }

    c
}
