// crates/k8dnz-core/src/orbexp/mod.rs
//
// Orbital Experiment Engine (orbexp)
//
// Deterministic integer-only "gear math" orbitals on a modular circle:
//   phaseA(t+1) = (phaseA(t) + stepA) mod MOD
//   phaseC(t+1) = (phaseC(t) + stepC) mod MOD
//
// Exact meet condition: phaseA == phaseC
//
// Closed-form meet time:
//   Let d = (stepA - stepC) mod MOD.
//   If d == 0: already in lockstep => first meet at t=0.
//   Else: first meet period = MOD / gcd(MOD, d).

use crate::error::{K8Error, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeriveMode {
    Int,
    Crc32,
    DecPairs,
}

impl DeriveMode {
    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "int" | "integer" => Ok(DeriveMode::Int),
            "crc32" | "crc" => Ok(DeriveMode::Crc32),
            "decpairs" | "dec" | "bcd" => Ok(DeriveMode::DecPairs),
            _ => Err(K8Error::Validation(format!("unknown derive mode: {s}"))),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrbParams {
    pub modn: u64,
    pub step_a: u64,
    pub step_c: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrbResult {
    pub d: u64,
    pub gcd: u64,
    pub t_first_meet: u64,
}

pub fn compute_first_meet(params: OrbParams) -> Result<OrbResult> {
    if params.modn == 0 {
        return Err(K8Error::Validation("mod must be non-zero".to_string()));
    }
    let modn = params.modn;

    let d = params.step_a.wrapping_sub(params.step_c) % modn;

    if d == 0 {
        return Ok(OrbResult {
            d,
            gcd: modn,
            t_first_meet: 0,
        });
    }

    let g = gcd_u64(modn, d);
    let t = modn / g;

    Ok(OrbResult {
        d,
        gcd: g,
        t_first_meet: t,
    })
}

pub fn simulate_first_meet(params: OrbParams, max_ticks: u64) -> Result<Option<u64>> {
    if params.modn == 0 {
        return Err(K8Error::Validation("mod must be non-zero".to_string()));
    }
    let modn = params.modn;

    let mut a = 0u64;
    let mut c = 0u64;

    if max_ticks == 0 {
        return Ok(Some(0));
    }

    for t in 0..=max_ticks {
        if a == c {
            return Ok(Some(t));
        }
        a = (a + (params.step_a % modn)) % modn;
        c = (c + (params.step_c % modn)) % modn;
    }
    Ok(None)
}

pub fn derive_steps(
    p: u64,
    block: &[u8],
    block_bits: usize,
    derive: DeriveMode,
    modn: u64,
) -> Result<(u64, u64, u64)> {
    if modn == 0 {
        return Err(K8Error::Validation("mod must be non-zero".to_string()));
    }
    if block_bits == 0 {
        return Err(K8Error::Validation(
            "block_bits must be non-zero".to_string(),
        ));
    }

    let need_bytes = (block_bits + 7) / 8;
    if block.len() < need_bytes {
        return Err(K8Error::Validation(format!(
            "block too small: need {need_bytes} bytes for block_bits={block_bits}, got {}",
            block.len()
        )));
    }

    let delta = match derive {
        DeriveMode::Int => derive_int_msb_first(&block[..need_bytes], block_bits)?,
        DeriveMode::Crc32 => crc32_ieee(&block[..need_bytes]) as u64,
        DeriveMode::DecPairs => derive_dec_pairs(&block[..need_bytes])?,
    };

    let step_a = splitmix64(p) % modn;
    let step_c = splitmix64(p ^ delta) % modn;

    Ok((delta, step_a, step_c))
}

fn derive_int_msb_first(bytes: &[u8], block_bits: usize) -> Result<u64> {
    if block_bits <= 64 {
        let mut v: u64 = 0;
        let full_bytes = block_bits / 8;
        let rem_bits = block_bits % 8;

        for &b in &bytes[..full_bytes] {
            v = (v << 8) | (b as u64);
        }

        if rem_bits != 0 {
            let b = bytes[full_bytes];
            let top = (b as u64) >> (8 - rem_bits);
            v = (v << rem_bits) | top;
        }

        Ok(v)
    } else {
        let mut acc = 0x9e3779b97f4a7c15u64;
        let mut bit_i = 0usize;
        while bit_i < block_bits {
            let mut chunk: u64 = 0;
            let take = usize::min(32, block_bits - bit_i);
            for _ in 0..take {
                chunk <<= 1;
                chunk |= bit_at_msb_first(bytes, bit_i) as u64;
                bit_i += 1;
            }
            acc ^= splitmix64(acc ^ chunk);
        }
        Ok(acc)
    }
}

fn bit_at_msb_first(bytes: &[u8], bit_index: usize) -> u8 {
    let byte_i = bit_index / 8;
    let bit_in = bit_index % 8;
    let b = bytes[byte_i];
    ((b >> (7 - bit_in)) & 1) as u8
}

fn derive_dec_pairs(bytes: &[u8]) -> Result<u64> {
    let mut v: u64 = 0;
    for &b in bytes {
        let x = (b % 100) as u64;
        v = v.wrapping_mul(100).wrapping_add(x);
    }
    Ok(v)
}

fn gcd_u64(mut a: u64, mut b: u64) -> u64 {
    while b != 0 {
        let r = a % b;
        a = b;
        b = r;
    }
    a
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e3779b97f4a7c15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}

fn crc32_ieee(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &b in data {
        crc ^= b as u32;
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320u32 & mask);
        }
    }
    !crc
}

pub fn bitlen_u64(v: u64) -> u32 {
    if v == 0 {
        0
    } else {
        64 - v.leading_zeros()
    }
}
