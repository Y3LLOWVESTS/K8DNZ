use crate::fixed::turn32::Turn32;

/// Minimal circular distance on Turn32.
#[inline]
pub fn turn32_dist(a: Turn32, b: Turn32) -> Turn32 {
    let d = a.0.wrapping_sub(b.0);
    let d_rev = b.0.wrapping_sub(a.0);
    Turn32(d.min(d_rev))
}
