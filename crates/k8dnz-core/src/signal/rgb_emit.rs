// crates/k8dnz-core/src/signal/rgb_emit.rs
//
// Deterministic RGB pair emission backends.
// No floats. No trig. Wrap arithmetic (mod 256) for proof-friendly invariants.

use crate::recipe::recipe::RgbRecipe;
use crate::signal::token::{Rgb, RgbPairToken};

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum AltMode {
    None,
    Parity,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum RgbBackend {
    /// base colors + shared drift g + field modulation p
    AdditiveCone,
    /// DNA-style: A/C remain paired, but modulation "twists" across channels over time
    CoupledAdder,
}

/// Wrap add in u8 space (mod 256) using i16 math.
#[inline]
fn add_wrap_u8(x: u8, delta: i16) -> u8 {
    let v = (x as i16) + delta;
    (v & 0xFF) as u8
}

/// Convert a signed field sample into a small signed symbol in [-8..8].
/// This is intentionally small-alphabet and deterministic.
#[inline]
fn field_to_sym(field: i64, spread: i64) -> i16 {
    // spread is a positive scale, typically quant width or clamp width.
    // Map field into [-8..8] using a safe integer normalization.
    if spread <= 0 {
        return 0;
    }
    // clamp field to [-spread..spread]
    let mut f = field;
    if f > spread {
        f = spread;
    } else if f < -spread {
        f = -spread;
    }
    // normalized in [-1..1], then *8
    // integer: sym = round((f * 8) / spread)
    let num = f.saturating_mul(8);
    let sym = if num >= 0 {
        (num + spread / 2) / spread
    } else {
        (num - spread / 2) / spread
    };
    // bound
    let mut s = sym as i16;
    if s > 8 {
        s = 8;
    }
    if s < -8 {
        s = -8;
    }
    s
}

/// Compute a shared drift term g for the emission index (monotonic ramp).
/// This is the "ordered cone backbone".
#[inline]
fn drift_g(emission_idx: u64, g_step: i16) -> i16 {
    // g_step is small (e.g., 1..8). Use wrap semantics by returning i16.
    // We only add this into u8 channels with wrap add.
    let k = (emission_idx & 0x7FFF) as i16; // bound
    k.saturating_mul(g_step)
}

/// Deterministic “cone/DNA” RGB pair emission from emission-time field samples.
///
/// - `field_a`, `field_c`: clamped field samples (i64)
/// - `spread`: a positive scale (typically clamp width or quant width)
pub fn emit_rgbpair_from_fields(
    cfg: &RgbRecipe,
    emission_idx: u64,
    field_a: i64,
    field_c: i64,
    spread: i64,
) -> RgbPairToken {
    let alt = match cfg.alt_mode {
        0 => AltMode::None,
        _ => AltMode::Parity,
    };
    let backend = match cfg.backend {
        0 => RgbBackend::AdditiveCone,
        _ => RgbBackend::CoupledAdder,
    };

    let base_a = Rgb::new(cfg.base_a[0], cfg.base_a[1], cfg.base_a[2]);
    let base_c = Rgb::new(cfg.base_c[0], cfg.base_c[1], cfg.base_c[2]);

    // shared drift (ordered ramp)
    let g = drift_g(emission_idx, cfg.g_step);

    // differential symbols from field samples
    // Use BOTH fields so A/C stay related, but allow asymmetry.
    let pa = field_to_sym(field_a, spread).saturating_mul(cfg.p_scale);
    let pc = field_to_sym(field_c, spread).saturating_mul(cfg.p_scale);

    // combine into one p for RGB channels (small alphabet)
    // deterministic: average of pa and pc, bounded.
    let mut p = ((pa as i32 + pc as i32) / 2) as i16;

    // Optional: parity alternation flips differential assignment each emission.
    let flip = matches!(alt, AltMode::Parity) && (emission_idx & 1) == 1;
    if flip {
        p = -p;
    }

    match backend {
        RgbBackend::AdditiveCone => {
            // Additive cone: both colors drift upward; each gets a slight different modulation.
            // A gets +p on R, -p on B; C gets -p on R, +p on B (keeps them paired).
            let a = Rgb::new(
                add_wrap_u8(base_a.r, g + p),
                add_wrap_u8(base_a.g, g),
                add_wrap_u8(base_a.b, g - p),
            );
            let c = Rgb::new(
                add_wrap_u8(base_c.r, g - p),
                add_wrap_u8(base_c.g, g),
                add_wrap_u8(base_c.b, g + p),
            );
            RgbPairToken { a, c }
        }
        RgbBackend::CoupledAdder => {
            // DNA coupled adder (HELIX TWIST):
            // - Still paired: A gets +delta per channel, C gets -delta per channel.
            // - But the “emphasis” rotates across RGB channels over time (emission_idx % 3),
            //   mimicking a helix twist rather than a fixed cone gradient.
            //
            // delta patterns (before twist):
            //   twist=0: [ +p,  0, -p ]
            //   twist=1: [ -p, +p,  0 ]
            //   twist=2: [  0, -p, +p ]
            let twist = (emission_idx % 3) as u8;
            let (dr, dg, db) = match twist {
                0 => (p, 0, -p),
                1 => (-p, p, 0),
                _ => (0, -p, p),
            };

            let a = Rgb::new(
                add_wrap_u8(base_a.r, g + dr),
                add_wrap_u8(base_a.g, g + dg),
                add_wrap_u8(base_a.b, g + db),
            );
            let c = Rgb::new(
                add_wrap_u8(base_c.r, g - dr),
                add_wrap_u8(base_c.g, g - dg),
                add_wrap_u8(base_c.b, g - db),
            );
            RgbPairToken { a, c }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(backend: u8) -> RgbRecipe {
        RgbRecipe {
            backend,
            alt_mode: 0,
            base_a: [255, 0, 0],
            base_c: [0, 255, 255],
            g_step: 1,
            p_scale: 1,
        }
    }

    #[test]
    fn backends_differ_on_some_emission() {
        let cone = cfg(0);
        let dna = cfg(1);

        // Pick a stable spread and non-zero fields so p != 0.
        let spread = 1_000_000;
        let field_a = 500_000;
        let field_c = -250_000;

        // Scan a few emissions; we expect at least one difference.
        let mut any_diff = false;
        for i in 0..32u64 {
            let a = emit_rgbpair_from_fields(&cone, i, field_a, field_c, spread);
            let b = emit_rgbpair_from_fields(&dna, i, field_a, field_c, spread);
            if a != b {
                any_diff = true;
                break;
            }
        }
        assert!(any_diff, "expected Cone and DNA to differ for some emission_idx");
    }
}
