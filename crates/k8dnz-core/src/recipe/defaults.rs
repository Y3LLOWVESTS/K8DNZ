// crates/k8dnz-core/src/recipe/defaults.rs

use crate::fixed::turn32::Turn32;
use crate::recipe::recipe::{
    Alphabet, FieldClampParams, FieldParams, FieldWave, FreeOrbitParams, LockstepParams, QuantParams, Recipe, ResetMode,
};

#[inline]
fn frac_turn(num: u64, den: u64) -> Turn32 {
    // Turn32 is u32 where 1.0 turn == 2^32.
    // floor(num/den * 2^32)
    let v = ((num << 32) / den) as u32;
    Turn32(v)
}

#[inline]
fn frac_unit32(num: u64, den: u64) -> u32 {
    // Unit32 is u32 where 1.0 == 2^32.
    ((num << 32) / den) as u32
}

pub fn default_recipe() -> Recipe {
    // Key idea for diversity with Δ=0.5:
    // Include even k_phi waves to break the half-turn mirror symmetry.
    //
    // v3:
    // - field_clamp moved into Recipe so we can tune clamp without recompiling core.
    // - defaults set clamp to observed raw dynamic range to avoid negative floor saturation.
    //
    // v4:
    // - quant.shift added: moves bin boundaries deterministically without changing field dynamics.
    //
    // IMPORTANT (Cadence decision):
    // - We are making the empirically best shift the DEFAULT: +7_141_012
    //   This improves symbol distribution (lower peak bin, higher entropy, more coverage)
    //   while leaving the cadence mechanics unchanged.
    Recipe {
        version: 4,
        seed: 0xD1CE_BA5E_F00D_CAFE, // deterministic default seed

        alphabet: Alphabet::N16,

        free: FreeOrbitParams {
            v_a: frac_turn(1, 997),
            v_c: frac_turn(1, 1009),

            phi_a0: frac_turn(0, 1),
            phi_c0: frac_turn(1, 7),

            epsilon: frac_turn(1, 4096),
        },

        lock: LockstepParams {
            v_l: frac_turn(1, 256),
            delta: frac_turn(1, 2),
            t_step: frac_unit32(1, 128),
        },

        reset_mode: ResetMode::FromLockstep,

        field: FieldParams {
            waves: vec![
                FieldWave { k_phi: 2, k_t: 3, k_time: 1,  phase: 0x1357_9BDF, amp:  3200 },
                FieldWave { k_phi: 3, k_t: 5, k_time: 2,  phase: 0x2468_ACED, amp:  2600 },
                FieldWave { k_phi: 4, k_t: 2, k_time: 3,  phase: 0x0BAD_F00D, amp: -2100 },
                FieldWave { k_phi: 1, k_t: 1, k_time: 13, phase: 0xC001_D00D, amp:   900 },
                FieldWave { k_phi: 6, k_t: 7, k_time: 5,  phase: 0xA5A5_5A5A, amp: -1700 },
            ],
        },

        // Observed from sim stats (emission-time sampling):
        // raw min=-147_728_900, raw max=80_783_500
        // Use this as clamp to avoid -100M floor plateau artifacts.
        field_clamp: FieldClampParams {
            min: -147_728_900,
            max: 80_783_500,
        },

        // Quant range equals clamp range; shift moves bin boundaries (does NOT change width).
        // Default shift: tuned winner (2026-02-10).
        quant: QuantParams {
            min: -147_728_900,
            max: 80_783_500,
            shift: 7_141_012,
        },

        // RGB emission parameters (DNA/coupled-adder defaults).
        rgb: Default::default(),
    }
}
