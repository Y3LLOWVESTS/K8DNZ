use crate::error::{K8Error, Result};

const MAGIC: &[u8; 4] = b"TM1\0";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimingMap {
    pub indices: Vec<u64>,
}

impl TimingMap {
    pub fn new(mut indices: Vec<u64>) -> Result<Self> {
        indices.sort_unstable();
        indices.dedup();
        // Strictly increasing (after dedup, monotonic holds)
        Ok(Self { indices })
    }

    pub fn stride(len: u64, start: u64, step: u64) -> Result<Self> {
        if step == 0 {
            return Err(K8Error::Validation("timemap stride step must be > 0".into()));
        }
        let mut v = Vec::with_capacity(len as usize);
        let mut x = start;
        for _ in 0..len {
            v.push(x);
            x = x.wrapping_add(step);
        }
        Self::new(v)
    }

    pub fn last_index(&self) -> Option<u64> {
        self.indices.last().copied()
    }

    /// TM1 binary encoding:
    /// MAGIC[4] = "TM1\0"
    /// count: varint(u64)
    /// deltas[count]: varint(u64) where
    ///   idx0 = delta0
    ///   idxi = idx(i-1) + delta(i)
    pub fn encode_tm1(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(16 + self.indices.len() * 2);
        out.extend_from_slice(MAGIC);

        write_var_u64(&mut out, self.indices.len() as u64);

        let mut prev: u64 = 0;
        for (i, &idx) in self.indices.iter().enumerate() {
            let delta = if i == 0 { idx } else { idx.saturating_sub(prev) };
            write_var_u64(&mut out, delta);
            prev = idx;
        }
        out
    }

    pub fn decode_tm1(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 4 || &bytes[0..4] != MAGIC {
            return Err(K8Error::Validation("timemap: bad magic".into()));
        }
        let mut i = 4usize;

        let count = read_var_u64(bytes, &mut i)? as usize;
        let mut indices = Vec::with_capacity(count);

        let mut prev: u64 = 0;
        for n in 0..count {
            let delta = read_var_u64(bytes, &mut i)?;
            let idx = if n == 0 {
                delta
            } else {
                prev.checked_add(delta)
                    .ok_or_else(|| K8Error::Validation("timemap: u64 overflow".into()))?
            };
            if n > 0 && idx <= prev {
                return Err(K8Error::Validation("timemap: non-increasing indices".into()));
            }
            indices.push(idx);
            prev = idx;
        }

        Ok(TimingMap { indices })
    }
}

// --- u64 varint (LEB128-like, 7-bit groups) ---

fn write_var_u64(out: &mut Vec<u8>, mut x: u64) {
    while x >= 0x80 {
        out.push(((x as u8) & 0x7F) | 0x80);
        x >>= 7;
    }
    out.push(x as u8);
}

fn read_var_u64(bytes: &[u8], i: &mut usize) -> Result<u64> {
    let mut shift: u32 = 0;
    let mut acc: u64 = 0;

    loop {
        if *i >= bytes.len() {
            return Err(K8Error::Validation("timemap: unexpected eof".into()));
        }
        let b = bytes[*i];
        *i += 1;

        let low = (b & 0x7F) as u64;
        if shift >= 64 || (low << shift) >> shift != low {
            return Err(K8Error::Validation("timemap: varint overflow".into()));
        }
        acc |= low << shift;

        if (b & 0x80) == 0 {
            return Ok(acc);
        }
        shift += 7;
        if shift > 63 {
            return Err(K8Error::Validation("timemap: varint too long".into()));
        }
    }
}
