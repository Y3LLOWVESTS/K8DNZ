use crc32fast::Hasher;

use crate::error::{ApexError, Result};

pub const MAGIC_ATK1: [u8; 4] = *b"ATK1";
pub const VERSION_V1: u16 = 1;
pub const MODE_DIBIT_V1: u8 = 0;
pub const LAW_QDL1: u8 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApexKey {
    pub version: u16,
    pub mode: u8,
    pub law_id: u8,
    pub root_quadrant: u8,
    pub depth: u16,
    pub byte_len: u64,
    pub quat_len: u64,
    pub root_seed: u64,
    pub recipe_seed: u64,
}

impl ApexKey {
    pub fn new_dibit_v1(
        byte_len: u64,
        root_quadrant: u8,
        root_seed: u64,
        recipe_seed: u64,
    ) -> Result<Self> {
        if root_quadrant > 3 {
            return Err(ApexError::Validation(format!(
                "root_quadrant {} is out of range 0..=3",
                root_quadrant
            )));
        }

        let quat_len = byte_len
            .checked_mul(4)
            .ok_or_else(|| ApexError::Validation("byte_len * 4 overflowed".into()))?;

        let depth = ceil_log2_u64(quat_len.max(1));

        Ok(Self {
            version: VERSION_V1,
            mode: MODE_DIBIT_V1,
            law_id: LAW_QDL1,
            root_quadrant,
            depth,
            byte_len,
            quat_len,
            root_seed,
            recipe_seed,
        })
    }

    pub fn validate(&self) -> Result<()> {
        if self.version != VERSION_V1 {
            return Err(ApexError::Validation(format!(
                "unsupported apextrace version {}",
                self.version
            )));
        }

        if self.mode != MODE_DIBIT_V1 {
            return Err(ApexError::Validation(format!(
                "unsupported apextrace mode {}",
                self.mode
            )));
        }

        if self.law_id != LAW_QDL1 {
            return Err(ApexError::Validation(format!(
                "unsupported apextrace law {}",
                self.law_id
            )));
        }

        if self.root_quadrant > 3 {
            return Err(ApexError::Validation(
                "root_quadrant must be 0..=3".into(),
            ));
        }

        if self.quat_len != self.byte_len.saturating_mul(4) {
            return Err(ApexError::Validation(format!(
                "quat_len {} does not equal byte_len*4 {}",
                self.quat_len,
                self.byte_len.saturating_mul(4)
            )));
        }

        let min_depth = ceil_log2_u64(self.quat_len.max(1));
        if self.depth < min_depth {
            return Err(ApexError::Validation(format!(
                "depth {} is too small for quat_len {} (need at least {})",
                self.depth, self.quat_len, min_depth
            )));
        }

        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;

        let mut out = Vec::with_capacity(48);
        out.extend_from_slice(&MAGIC_ATK1);
        out.extend_from_slice(&self.version.to_le_bytes());
        out.push(self.mode);
        out.push(self.law_id);
        out.push(self.root_quadrant);
        out.push(0u8);
        out.extend_from_slice(&self.depth.to_le_bytes());
        out.extend_from_slice(&self.byte_len.to_le_bytes());
        out.extend_from_slice(&self.quat_len.to_le_bytes());
        out.extend_from_slice(&self.root_seed.to_le_bytes());
        out.extend_from_slice(&self.recipe_seed.to_le_bytes());

        let mut hasher = Hasher::new();
        hasher.update(&out);
        let crc = hasher.finalize();
        out.extend_from_slice(&crc.to_le_bytes());

        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 48 {
            return Err(ApexError::Format(format!(
                "ApexKey must be exactly 48 bytes, got {}",
                bytes.len()
            )));
        }

        if bytes[0..4] != MAGIC_ATK1 {
            return Err(ApexError::Format("bad magic; expected ATK1".into()));
        }

        let stored_crc = u32::from_le_bytes(bytes[44..48].try_into().unwrap());
        let mut hasher = Hasher::new();
        hasher.update(&bytes[..44]);
        let actual_crc = hasher.finalize();

        if stored_crc != actual_crc {
            return Err(ApexError::Format(format!(
                "crc mismatch: stored={} actual={}",
                stored_crc, actual_crc
            )));
        }

        let key = Self {
            version: u16::from_le_bytes(bytes[4..6].try_into().unwrap()),
            mode: bytes[6],
            law_id: bytes[7],
            root_quadrant: bytes[8],
            depth: u16::from_le_bytes(bytes[10..12].try_into().unwrap()),
            byte_len: u64::from_le_bytes(bytes[12..20].try_into().unwrap()),
            quat_len: u64::from_le_bytes(bytes[20..28].try_into().unwrap()),
            root_seed: u64::from_le_bytes(bytes[28..36].try_into().unwrap()),
            recipe_seed: u64::from_le_bytes(bytes[36..44].try_into().unwrap()),
        };

        key.validate()?;
        Ok(key)
    }
}

pub fn ceil_log2_u64(n: u64) -> u16 {
    if n <= 1 {
        return 0;
    }

    let mut depth = 0u16;
    let mut cap = 1u64;
    while cap < n {
        cap <<= 1;
        depth = depth.saturating_add(1);
    }
    depth
}