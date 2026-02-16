// crates/k8dnz-core/src/signal/timing_map.rs

use crate::error::{K8Error, Result};

const MAGIC_TM1: &[u8; 4] = b"TM1\0";
const MAGIC_TM0: &[u8; 4] = b"TM0\0";
const MAGIC_TM2: &[u8; 4] = b"TM2\0"; // piecewise runs (stride=1 segments)

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimingMap {
    pub indices: Vec<u64>,
}

impl TimingMap {
    pub fn new(indices: Vec<u64>) -> Result<Self> {
        // invariant: strictly increasing
        for w in indices.windows(2) {
            if w[1] <= w[0] {
                return Err(K8Error::Validation("timemap: non-increasing indices".into()));
            }
        }
        Ok(TimingMap { indices })
    }

    pub fn stride(count: u64, start: u64, step: u64) -> Result<Self> {
        if step == 0 {
            return Err(K8Error::Validation("timemap: step must be > 0".into()));
        }
        let mut indices = Vec::with_capacity(count as usize);
        let mut cur = start;
        for _ in 0..count {
            indices.push(cur);
            cur = cur
                .checked_add(step)
                .ok_or_else(|| K8Error::Validation("timemap: u64 overflow".into()))?;
        }
        TimingMap::new(indices)
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
        out.extend_from_slice(MAGIC_TM1);

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
        if bytes.len() < 4 || &bytes[0..4] != MAGIC_TM1 {
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

    /// If this TimingMap is an arithmetic progression, return (start, len, step).
    /// For len 0/1, we treat it as step=1.
    pub fn as_arith_prog(&self) -> Option<(u64, u64, u64)> {
        let n = self.indices.len();
        if n == 0 {
            return Some((0, 0, 1));
        }
        if n == 1 {
            return Some((self.indices[0], 1, 1));
        }
        let start = self.indices[0];
        let step = self.indices[1].checked_sub(self.indices[0])?;
        if step == 0 {
            return None;
        }
        let mut prev = self.indices[1];
        for &x in self.indices.iter().skip(2) {
            if x <= prev {
                return None;
            }
            if x.checked_sub(prev)? != step {
                return None;
            }
            prev = x;
        }
        Some((start, n as u64, step))
    }

    /// TM0 binary encoding (implicit stride program):
    /// MAGIC[4] = "TM0\0"
    /// len: varint(u64)
    /// start: varint(u64)
    /// step: varint(u64)
    pub fn encode_tm0(len: u64, start: u64, step: u64) -> Vec<u8> {
        let mut out = Vec::with_capacity(32);
        out.extend_from_slice(MAGIC_TM0);
        write_var_u64(&mut out, len);
        write_var_u64(&mut out, start);
        write_var_u64(&mut out, step);
        out
    }

    pub fn decode_tm0(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 4 || &bytes[0..4] != MAGIC_TM0 {
            return Err(K8Error::Validation("timemap: bad magic".into()));
        }
        let mut i = 4usize;
        let len = read_var_u64(bytes, &mut i)?;
        let start = read_var_u64(bytes, &mut i)?;
        let step = read_var_u64(bytes, &mut i)?;

        if step == 0 {
            return Err(K8Error::Validation("timemap: step must be > 0".into()));
        }
        TimingMap::stride(len, start, step)
    }

    /// TM2: piecewise stride=1 runs.
    ///
    /// MAGIC[4] = "TM2\0"
    /// seg_count: varint(u64)
    /// segments[seg_count]:
    ///   start: varint(u64)
    ///   len:   varint(u64)   (>= 1)
    ///
    /// Reconstruct as concatenation of runs:
    ///   [start, start+1, ..., start+len-1] for each segment
    pub fn encode_tm2_runs(&self) -> Vec<u8> {
        let segs = self.as_runs_step1();
        let mut out = Vec::with_capacity(16 + segs.len() * 6);
        out.extend_from_slice(MAGIC_TM2);
        write_var_u64(&mut out, segs.len() as u64);
        for (start, len) in segs {
            write_var_u64(&mut out, start);
            write_var_u64(&mut out, len);
        }
        out
    }

    pub fn decode_tm2(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 4 || &bytes[0..4] != MAGIC_TM2 {
            return Err(K8Error::Validation("timemap: bad magic".into()));
        }
        let mut i = 4usize;

        let seg_count = read_var_u64(bytes, &mut i)? as usize;
        let mut indices: Vec<u64> = Vec::new();

        let mut last: Option<u64> = None;

        for _ in 0..seg_count {
            let start = read_var_u64(bytes, &mut i)?;
            let len = read_var_u64(bytes, &mut i)? as usize;

            if len == 0 {
                return Err(K8Error::Validation("timemap: TM2 segment len=0".into()));
            }

            if let Some(prev_last) = last {
                // segments must be strictly increasing overall
                if start <= prev_last {
                    return Err(K8Error::Validation(
                        "timemap: TM2 non-increasing segment start".into(),
                    ));
                }
            }

            let mut cur = start;
            for _ in 0..len {
                if let Some(prev) = last {
                    if cur <= prev {
                        return Err(K8Error::Validation(
                            "timemap: TM2 non-increasing indices".into(),
                        ));
                    }
                }
                indices.push(cur);
                last = Some(cur);
                cur = cur
                    .checked_add(1)
                    .ok_or_else(|| K8Error::Validation("timemap: u64 overflow".into()))?;
            }
        }

        Ok(TimingMap { indices })
    }

    /// Derive stride=1 runs from indices.
    /// Assumes indices are strictly increasing (invariant already holds).
    fn as_runs_step1(&self) -> Vec<(u64, u64)> {
        let n = self.indices.len();
        if n == 0 {
            return Vec::new();
        }

        let mut segs: Vec<(u64, u64)> = Vec::new();

        let mut start = self.indices[0];
        let mut last = self.indices[0];
        let mut len: u64 = 1;

        for &x in self.indices.iter().skip(1) {
            if x == last.saturating_add(1) {
                len = len.saturating_add(1);
                last = x;
            } else {
                segs.push((start, len));
                start = x;
                last = x;
                len = 1;
            }
        }
        segs.push((start, len));
        segs
    }

    fn var_u64_len(mut x: u64) -> usize {
        let mut n = 1usize;
        while x >= 0x80 {
            n += 1;
            x >>= 7;
        }
        n
    }

    fn estimate_tm1_len_bytes(&self) -> usize {
        // magic + count + deltas(varints)
        let mut n = 4 + Self::var_u64_len(self.indices.len() as u64);
        let mut prev = 0u64;
        for (i, &idx) in self.indices.iter().enumerate() {
            let d = if i == 0 { idx } else { idx.saturating_sub(prev) };
            n += Self::var_u64_len(d);
            prev = idx;
        }
        n
    }

    fn estimate_tm2_len_bytes(&self, segs: &[(u64, u64)]) -> usize {
        // magic + seg_count + each(start,len)
        let mut n = 4 + Self::var_u64_len(segs.len() as u64);
        for &(s, l) in segs.iter() {
            n += Self::var_u64_len(s);
            n += Self::var_u64_len(l);
        }
        n
    }

    /// Auto-encoding:
    /// - TM0 if global arithmetic progression
    /// - else TM2 if runs encoding is smaller than TM1 (and meaningfully segments)
    /// - else TM1
    pub fn encode_auto(&self) -> Vec<u8> {
        if let Some((start, len, step)) = self.as_arith_prog() {
            return TimingMap::encode_tm0(len, start, step);
        }

        let segs = self.as_runs_step1();
        if !segs.is_empty() {
            let est2 = self.estimate_tm2_len_bytes(&segs);
            let est1 = self.estimate_tm1_len_bytes();
            // Only pick TM2 when it is actually smaller.
            if est2 < est1 {
                return self.encode_tm2_runs();
            }
        }

        self.encode_tm1()
    }

    /// Auto-decoding: detect TM0/TM1/TM2 magic.
    pub fn decode_auto(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 4 {
            return Err(K8Error::Validation("timemap: too short".into()));
        }
        if &bytes[0..4] == MAGIC_TM0 {
            return TimingMap::decode_tm0(bytes);
        }
        if &bytes[0..4] == MAGIC_TM2 {
            return TimingMap::decode_tm2(bytes);
        }
        if &bytes[0..4] == MAGIC_TM1 {
            return TimingMap::decode_tm1(bytes);
        }
        Err(K8Error::Validation("timemap: unknown magic".into()))
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
