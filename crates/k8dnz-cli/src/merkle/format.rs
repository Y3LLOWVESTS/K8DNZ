use anyhow::{Context, Result};
use crc32fast::Hasher;

pub const MAGIC_K8B1: [u8; 4] = *b"K8B1";
pub const MAGIC_K8P2: [u8; 4] = *b"K8P2";
pub const MAGIC_ARKM1: [u8; 5] = *b"ARKM1";

pub const K8B1_VERSION: u8 = 1;
pub const K8P2_VERSION: u8 = 1;
pub const ARKM1_VERSION: u8 = 1;

#[derive(Clone, Debug)]
pub struct ReconParams {
    pub max_ticks: u64,
    pub map_seed: u64,
    pub bits_per_emission: u8,
    pub bit_mapping: u8,     // matches args::BitMapping discriminant
    pub bit_tau: u32,
    pub bit_smooth_shift: u8,
    pub residual_mode: u8,   // matches args::ResidualMode discriminant
}

impl ReconParams {
    pub fn default_for_bits(bits_per_emission: u8) -> Self {
        Self {
            max_ticks: 200_000_000,
            map_seed: 1,
            bits_per_emission,
            // You already enforce: lowpass-thresh requires 1 bit【turn10file0†codebundle.md†L75-L77】
            bit_mapping: if bits_per_emission == 1 { 0 } else { 1 },
            bit_tau: 0,
            bit_smooth_shift: 0,
            residual_mode: 0, // xor
        }
    }
}

#[derive(Clone, Debug)]
pub struct K8b1Blob {
    pub payload_len: u32,
    pub recon: ReconParams,
    pub recipe: Vec<u8>,
    pub timemap: Vec<u8>,
    pub residual: Vec<u8>,
}

impl K8b1Blob {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();

        out.extend_from_slice(&MAGIC_K8B1);
        out.push(K8B1_VERSION);

        out.extend_from_slice(&self.payload_len.to_le_bytes());

        out.extend_from_slice(&self.recon.max_ticks.to_le_bytes());
        out.extend_from_slice(&self.recon.map_seed.to_le_bytes());
        out.push(self.recon.bits_per_emission);
        out.push(self.recon.bit_mapping);
        out.extend_from_slice(&self.recon.bit_tau.to_le_bytes());
        out.push(self.recon.bit_smooth_shift);
        out.push(self.recon.residual_mode);

        out.extend_from_slice(&(self.recipe.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.recipe);

        out.extend_from_slice(&(self.timemap.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.timemap);

        out.extend_from_slice(&(self.residual.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.residual);

        let crc = crc32(&out);
        out.extend_from_slice(&crc.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut p = Parser::new(bytes);

        let magic = p.take4()?;
        anyhow::ensure!(magic == MAGIC_K8B1, "K8B1 bad magic");

        let ver = p.take_u8()?;
        anyhow::ensure!(ver == K8B1_VERSION, "K8B1 unsupported version {ver}");

        let payload_len = p.take_u32()?;

        let max_ticks = p.take_u64()?;
        let map_seed = p.take_u64()?;
        let bits_per_emission = p.take_u8()?;
        let bit_mapping = p.take_u8()?;
        let bit_tau = p.take_u32()?;
        let bit_smooth_shift = p.take_u8()?;
        let residual_mode = p.take_u8()?;

        let recipe = p.take_vec_u32_len()?;
        let timemap = p.take_vec_u32_len()?;
        let residual = p.take_vec_u32_len()?;

        let got_crc = p.take_u32()?;
        let want_crc = crc32(&bytes[..bytes.len().saturating_sub(4)]);
        anyhow::ensure!(got_crc == want_crc, "K8B1 crc mismatch");

        anyhow::ensure!(p.is_eof(), "K8B1 trailing bytes");

        Ok(Self {
            payload_len,
            recon: ReconParams {
                max_ticks,
                map_seed,
                bits_per_emission,
                bit_mapping,
                bit_tau,
                bit_smooth_shift,
                residual_mode,
            },
            recipe,
            timemap,
            residual,
        })
    }
}

#[derive(Clone, Debug)]
pub struct K8p2Pair {
    pub a: Vec<u8>,
    pub b: Vec<u8>,
}

impl K8p2Pair {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&MAGIC_K8P2);
        out.push(K8P2_VERSION);
        out.extend_from_slice(&(self.a.len() as u32).to_le_bytes());
        out.extend_from_slice(&(self.b.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.a);
        out.extend_from_slice(&self.b);
        let crc = crc32(&out);
        out.extend_from_slice(&crc.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut p = Parser::new(bytes);

        let magic = p.take4()?;
        anyhow::ensure!(magic == MAGIC_K8P2, "K8P2 bad magic");

        let ver = p.take_u8()?;
        anyhow::ensure!(ver == K8P2_VERSION, "K8P2 unsupported version {ver}");

        let la = p.take_u32()? as usize;
        let lb = p.take_u32()? as usize;
        let a = p.take_exact(la)?.to_vec();
        let b = p.take_exact(lb)?.to_vec();

        let got_crc = p.take_u32()?;
        let want_crc = crc32(&bytes[..bytes.len().saturating_sub(4)]);
        anyhow::ensure!(got_crc == want_crc, "K8P2 crc mismatch");

        anyhow::ensure!(p.is_eof(), "K8P2 trailing bytes");

        Ok(Self { a, b })
    }
}

#[derive(Clone, Debug)]
pub struct Arkm1Root {
    pub original_len: u64,
    pub chunk_bytes: u32,
    pub leaf_count: u32, // padded power-of-two
    pub root_blob: Vec<u8>, // K8B1 bytes
}

impl Arkm1Root {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&MAGIC_ARKM1);
        out.push(ARKM1_VERSION);

        out.extend_from_slice(&self.original_len.to_le_bytes());
        out.extend_from_slice(&self.chunk_bytes.to_le_bytes());
        out.extend_from_slice(&self.leaf_count.to_le_bytes());

        out.extend_from_slice(&(self.root_blob.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.root_blob);

        let crc = crc32(&out);
        out.extend_from_slice(&crc.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut p = Parser::new(bytes);

        let magic5 = p.take5()?;
        anyhow::ensure!(magic5 == MAGIC_ARKM1, "ARKM1 bad magic");

        let ver = p.take_u8()?;
        anyhow::ensure!(ver == ARKM1_VERSION, "ARKM1 unsupported version {ver}");

        let original_len = p.take_u64()?;
        let chunk_bytes = p.take_u32()?;
        let leaf_count = p.take_u32()?;

        let root_len = p.take_u32()? as usize;
        let root_blob = p.take_exact(root_len)?.to_vec();

        let got_crc = p.take_u32()?;
        let want_crc = crc32(&bytes[..bytes.len().saturating_sub(4)]);
        anyhow::ensure!(got_crc == want_crc, "ARKM1 crc mismatch");

        anyhow::ensure!(p.is_eof(), "ARKM1 trailing bytes");

        Ok(Self {
            original_len,
            chunk_bytes,
            leaf_count,
            root_blob,
        })
    }
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut h = Hasher::new();
    h.update(bytes);
    h.finalize()
}

struct Parser<'a> {
    b: &'a [u8],
    i: usize,
}

impl<'a> Parser<'a> {
    fn new(b: &'a [u8]) -> Self {
        Self { b, i: 0 }
    }

    fn is_eof(&self) -> bool {
        self.i == self.b.len()
    }

    fn take_exact(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.i.checked_add(n).context("overflow")?;
        anyhow::ensure!(end <= self.b.len(), "unexpected EOF");
        let s = &self.b[self.i..end];
        self.i = end;
        Ok(s)
    }

    fn take_u8(&mut self) -> Result<u8> {
        Ok(self.take_exact(1)?[0])
    }

    fn take_u32(&mut self) -> Result<u32> {
        let s = self.take_exact(4)?;
        Ok(u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
    }

    fn take_u64(&mut self) -> Result<u64> {
        let s = self.take_exact(8)?;
        Ok(u64::from_le_bytes([s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7]]))
    }

    fn take4(&mut self) -> Result<[u8; 4]> {
        let s = self.take_exact(4)?;
        Ok([s[0], s[1], s[2], s[3]])
    }

    fn take5(&mut self) -> Result<[u8; 5]> {
        let s = self.take_exact(5)?;
        Ok([s[0], s[1], s[2], s[3], s[4]])
    }

    fn take_vec_u32_len(&mut self) -> Result<Vec<u8>> {
        let n = self.take_u32()? as usize;
        Ok(self.take_exact(n)?.to_vec())
    }
}
