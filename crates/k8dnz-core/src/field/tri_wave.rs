use crate::fixed::{turn32::Turn32, unit32::Unit32};
use crate::recipe::recipe::FieldWave;
use crate::field::params::FieldModel;

/// Triangle wave over u32 phase space -> i32 in [-32768, 32767]
#[inline]
fn tri_u32(x: u32) -> i32 {
    // Use top bit as "which half" and next bits for ramp.
    // Make a symmetric triangle wave without floats.
    let half = x & 0x8000_0000 != 0;
    let ramp = (x >> 16) as i32; // 0..65535
    let y = if half { 65535 - ramp } else { ramp }; // triangle 0..65535
    y - 32768
}

/// Evaluate field at (phi, t, time) WITHOUT clamping.
/// Useful for measuring true dynamic range vs clamp/quant ranges.
pub fn eval_raw(model: &FieldModel, phi: Turn32, t: Unit32, time: u64) -> i64 {
    let mut acc: i64 = 0;
    for w in &model.params.waves {
        acc = acc.saturating_add(eval_wave(w, phi, t, time));
    }
    acc
}

/// Evaluate field at (phi, t, time) and clamp to model cfg.
pub fn eval(model: &FieldModel, phi: Turn32, t: Unit32, time: u64) -> i64 {
    eval_raw(model, phi, t, time).clamp(model.cfg.clamp_min, model.cfg.clamp_max)
}

#[inline]
fn eval_wave(w: &FieldWave, phi: Turn32, t: Unit32, time: u64) -> i64 {
    // Linear mix in u32 space: x = k_phi*phi + k_t*t + k_time*time + phase
    // All wrapping, then triangle.
    let mut x = w.phase;
    x = x.wrapping_add(phi.0.wrapping_mul(w.k_phi));
    x = x.wrapping_add(t.0.wrapping_mul(w.k_t));
    x = x.wrapping_add((time as u32).wrapping_mul(w.k_time));
    let tri = tri_u32(x) as i64;
    tri.saturating_mul(w.amp as i64)
}
