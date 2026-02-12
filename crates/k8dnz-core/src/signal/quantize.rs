use crate::signal::sample::FieldSample;

/// Deterministic quantization (round-to-nearest).
///
/// Maps an inclusive input range `min..=max` into `0..=(n-1)`.
/// - Guarantees `min -> 0`
/// - Guarantees `max -> n-1`
/// - Uses integer rounding to reduce systematic floor bias
pub fn quantize(sample: FieldSample, min: i64, max: i64, n: u8) -> u8 {
    debug_assert!(n >= 2);
    let n_i = n as i64;

    // Be defensive: if caller ever passes inverted bounds, normalize.
    let (min, max) = if min <= max { (min, max) } else { (max, min) };

    // Degenerate range: everything maps to 0.
    if min == max {
        return 0;
    }

    let s = sample.0.clamp(min, max);

    // range is strictly positive here
    let range: i64 = max - min;
    let shifted: i64 = s - min; // 0..=range

    // Ensure inclusive top end maps to the final bin.
    if shifted >= range {
        return (n - 1) as u8;
    }

    // Map [0, range) -> [0, n) with rounding.
    // bin = round( shifted * n / range )
    // Use +range/2 for round-to-nearest in integer arithmetic.
    let num = shifted.saturating_mul(n_i);
    let mut bin = (num + (range / 2)) / range;

    // Clamp for safety (should already be within bounds).
    if bin < 0 {
        bin = 0;
    } else if bin > (n_i - 1) {
        bin = n_i - 1;
    }

    bin as u8
}

/// Apply a shift to both bounds (min/max) using saturating arithmetic.
/// This preserves the range width and only moves bin boundaries.
///
/// This is the correct "Cadence" knob: dynamics and field sampling are unchanged;
/// only the labeling / bin edges move.
///
/// Returns (min+shift, max+shift).
#[inline]
pub fn shifted_bounds(min: i64, max: i64, shift: i64) -> (i64, i64) {
    (min.saturating_add(shift), max.saturating_add(shift))
}
