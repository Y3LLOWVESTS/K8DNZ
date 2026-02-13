// crates/k8dnz-core/src/recipe/recipe.rs

use crate::fixed::turn32::Turn32;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Alphabet {
    /// 16 symbols per channel; packs to one byte (hi/lo nybbles).
    N16,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResetMode {
    HoldAandC,
    FromLockstep,
}

/// Optional, invertible keystream mixing.
/// This is NOT about cryptographic strength; it’s about distribution shaping
/// while preserving perfect determinism + invertibility.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeystreamMix {
    None,
    SplitMix64,
}

/// Semantic payload label for .ark data bytes.
/// For now both are reconstructed with the same XOR law:
///   plain = data XOR keystream
/// But this field is the bridge for “model + residual” next.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PayloadKind {
    /// Data bytes are “ciphertext” (plain XOR keystream)
    CipherXor,
    /// Data bytes are “residual” (plain XOR model_stream), model_stream currently == keystream
    ResidualXor,
}

#[derive(Clone, Copy, Debug)]
pub struct FreeOrbitParams {
    pub phi_a0: Turn32,
    pub phi_c0: Turn32,
    pub v_a: Turn32,
    pub v_c: Turn32,
    pub epsilon: Turn32,
}

#[derive(Clone, Copy, Debug)]
pub struct LockstepParams {
    pub v_l: Turn32,
    pub delta: Turn32,
    /// Saturating Unit32 step per tick.
    pub t_step: u32,
}

#[derive(Clone, Copy, Debug)]
pub struct FieldWave {
    pub k_phi: u32,
    pub k_t: u32,
    pub k_time: u32,
    pub phase: u32,
    pub amp: i32,
}

#[derive(Clone, Debug)]
pub struct FieldParams {
    pub waves: Vec<FieldWave>,
}

#[derive(Clone, Copy, Debug)]
pub struct FieldClampParams {
    /// Inclusive min for field clamp.
    pub min: i64,
    /// Inclusive max for field clamp.
    pub max: i64,
}

#[derive(Clone, Copy, Debug)]
pub struct QuantParams {
    /// Inclusive min for quantization mapping (clamps input).
    pub min: i64,
    /// Inclusive max for quantization mapping (clamps input).
    pub max: i64,
    /// Shift applied to BOTH min and max BEFORE quantization.
    /// This moves bin boundaries without changing dynamics or field sampling.
    ///
    /// Default: 0 (no shift; preserves legacy behavior).
    pub shift: i64,
}

/// RGB emission parameters.
/// Stored in the recipe so ARK keys can carry the “cone law” deterministically.
#[derive(Clone, Debug)]
pub struct RgbRecipe {
    /// 0=AdditiveCone, 1=CoupledAdder
    pub backend: u8,
    /// 0=None, 1=Parity
    pub alt_mode: u8,

    /// Base color for dot A
    pub base_a: [u8; 3],
    /// Base color for dot C
    pub base_c: [u8; 3],

    /// Shared drift step per emission (ordered ramp)
    pub g_step: i16,
    /// Differential scale multiplier (small alphabet)
    pub p_scale: i16,
}

impl Default for RgbRecipe {
    fn default() -> Self {
        Self {
            backend: 1,  // CoupledAdder by default (DNA)
            alt_mode: 1, // Parity alternation on by default
            base_a: [255, 0, 0],   // red
            base_c: [0, 255, 255], // cyan
            g_step: 2,  // gentle ramp
            p_scale: 2, // small modulation
        }
    }
}

#[derive(Clone, Debug)]
pub struct Recipe {
    pub version: u16,
    pub seed: u64,

    pub alphabet: Alphabet,
    pub reset_mode: ResetMode,

    /// NEW (stored in recipe flags; back-compat default = None)
    pub keystream_mix: KeystreamMix,

    /// NEW (stored in recipe flags; back-compat default = CipherXor)
    pub payload_kind: PayloadKind,

    pub free: FreeOrbitParams,
    pub lock: LockstepParams,
    pub field: FieldParams,

    /// Field clamp range (distinct from quant range).
    pub field_clamp: FieldClampParams,

    /// Quantization range (distinct from field clamp).
    pub quant: QuantParams,

    /// RGB emission parameters (cone law / coupled-adder).
    pub rgb: RgbRecipe,
}
