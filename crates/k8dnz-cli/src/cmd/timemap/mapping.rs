// crates/k8dnz-cli/src/cmd/timemap/mapping.rs

use super::args::MapMode;
use super::util::splitmix64;

pub fn map_byte(mode: MapMode, seed: u64, pos: u64, raw: u8) -> u8 {
    match mode {
        MapMode::None => raw,
        MapMode::Splitmix64 => {
            let k = splitmix64(seed ^ pos) as u8;
            raw ^ k
        }
        MapMode::Ascii7 => ascii7(raw),
        MapMode::Ascii7Splitmix => {
            let k = splitmix64(seed ^ pos) as u8;
            ascii7(raw ^ k)
        }
        MapMode::Text40 => text_from_alphabet(TEXT40_ALPHABET, raw),
        MapMode::Text40Weighted => {
            text_from_weighted_alphabet(TEXT40_ALPHABET, TEXT40_WEIGHTS, raw)
        }
        MapMode::Text40Lane => text40_lane(pos, raw),
        MapMode::Text40Field => text40_field(seed, pos, raw),
        MapMode::Bitfield => raw, // not used in byte pipeline
        MapMode::Text64 => text_from_alphabet(TEXT64_ALPHABET, raw),
    }
}

fn ascii7(b: u8) -> u8 {
    let x = b & 0x7F;
    if (0x20..=0x7E).contains(&x) {
        x
    } else {
        0x20u8 + (x % 95)
    }
}

fn text_from_alphabet(alpha: &[u8], raw: u8) -> u8 {
    let idx = (raw as usize) % alpha.len();
    alpha[idx]
}

fn text_from_weighted_alphabet(alpha: &[u8], weights: &[u8], raw: u8) -> u8 {
    debug_assert_eq!(alpha.len(), weights.len());
    let mut x: u16 = raw as u16;

    for (i, &w) in weights.iter().enumerate() {
        let ww = w as u16;
        if x < ww {
            return alpha[i];
        }
        x -= ww;
    }

    let idx = (raw as usize) % alpha.len();
    alpha[idx]
}

fn text40_lane(pos: u64, raw: u8) -> u8 {
    let lane = (pos % 6) as u8;
    match lane {
        0 => text_from_weighted_alphabet(LANE0_ALPHA, LANE0_W, raw),
        1 => text_from_weighted_alphabet(LANE1_ALPHA, LANE1_W, raw),
        2 => text_from_weighted_alphabet(LANE2_ALPHA, LANE2_W, raw),
        3 => text_from_weighted_alphabet(LANE3_ALPHA, LANE3_W, raw),
        4 => text_from_weighted_alphabet(LANE4_ALPHA, LANE4_W, raw),
        _ => text_from_weighted_alphabet(LANE5_ALPHA, LANE5_W, raw),
    }
}

/// Text40Field mapping with TIME-EVOLVING “intensity” + “shift-wave”.
///
/// `seed` packs extra params:
/// - seed_lo   (lower 32 bits): base noise seed
/// - rate      ((seed>>32) & 0xFF): intensity strength (0..255; 0 treated as 1)
/// - tshift    ((seed>>40) & 0xFF): time scale; pos >> tshift drives evolution
/// - phase0    ((seed>>48) & 0xFF): phase offset for the cycle
/// - shift_amp ((seed>>56) & 0xFF): 0 disables; otherwise adds a wave-like SHIFT to stripe/phase
fn text40_field(seed: u64, pos: u64, raw: u8) -> u8 {
    let lane = (pos % 6) as u8;

    let mut stripe = ((pos >> 7) & 0xFF) as u8;
    let mut phase = ((pos >> 11) & 0xFF) as u8;

    let seed_lo = seed as u32;
    let rate = ((seed >> 32) & 0xFF) as u8;
    let tshift = ((seed >> 40) & 0xFF) as u8;
    let phase0 = ((seed >> 48) & 0xFF) as u8;
    let shift_amp = ((seed >> 56) & 0xFF) as u8;

    let sh = (tshift as u32).min(56);
    let t = ((pos >> sh) & 0xFFFF) as u16;
    let t8 = (t as u8).wrapping_add(phase0);

    let tri = {
        let x = t8 & 0x7F;
        if (t8 & 0x80) == 0 {
            x.wrapping_mul(2)
        } else {
            (127u8 - x).wrapping_mul(2)
        }
    };

    if shift_amp != 0 {
        let w = tri.wrapping_add(lane.wrapping_mul(31)).wrapping_add(phase0);
        let centered = (w as i16) - 128i16;
        let scaled = (centered * (shift_amp as i16)) / 256i16;
        stripe = stripe.wrapping_add(scaled as i8 as u8);
        phase = phase.wrapping_add(((scaled / 2) as i8) as u8);
    }

    let noise = (splitmix64((seed_lo as u64) ^ pos) as u8).wrapping_mul(13);

    let r = if rate == 0 { 1u8 } else { rate };
    let f = stripe
        .wrapping_add(phase)
        .wrapping_add(lane.wrapping_mul(17))
        .wrapping_add(noise)
        .wrapping_add(tri.wrapping_mul(r));

    let mixed = raw.wrapping_add(f);

    match lane {
        0 => text_from_weighted_alphabet(LANE0_ALPHA, LANE0_W, mixed),
        1 => text_from_weighted_alphabet(LANE1_ALPHA, LANE1_W, mixed),
        2 => text_from_weighted_alphabet(LANE2_ALPHA, LANE2_W, mixed),
        3 => text_from_weighted_alphabet(LANE3_ALPHA, LANE3_W, mixed),
        4 => text_from_weighted_alphabet(LANE4_ALPHA, LANE4_W, mixed),
        _ => text_from_weighted_alphabet(LANE5_ALPHA, LANE5_W, mixed),
    }
}

const TEXT40_ALPHABET: &[u8] = b" etaoinshrdlucmfwypvbgkjqxz\n.,'";
const TEXT40_WEIGHTS: &[u8] = &[
    58, 22, 16, 16, 14, 13, 12, 10, 9, 9, 8, 8, 6, 6, 6, 5, 4, 4, 4, 3, 3, 3, 2, 1, 1, 1, 1, 6, 2,
    2, 1,
];

const LANE0_ALPHA: &[u8] = b" \n.,'";
const LANE0_W: &[u8] = &[200, 40, 6, 6, 4];

const LANE1_ALPHA: &[u8] = b" aeiou";
const LANE1_W: &[u8] = &[64, 48, 56, 44, 28, 16];

const LANE2_ALPHA: &[u8] = b" nstrhl";
const LANE2_W: &[u8] = &[64, 44, 44, 42, 36, 32, 38];

const LANE3_ALPHA: &[u8] = b" dcmfwypvbgkjqxz";
const LANE3_W: &[u8] = &[64, 20, 20, 18, 18, 16, 16, 14, 12, 12, 12, 8, 4, 4, 4, 4];

const LANE4_ALPHA: &[u8] = b" \n.,'";
const LANE4_W: &[u8] = &[140, 44, 28, 28, 16];

const LANE5_ALPHA: &[u8] = b" etaoinshrdl";
const LANE5_W: &[u8] = &[96, 18, 14, 14, 12, 12, 10, 10, 10, 8, 6, 6];

const TEXT64_ALPHABET: &[u8] =
    b" etaoinshrdlucmfwypvbgkjqxzETAOINSHRDLUCMFWYPVBGKJQXZ\n.,;:'\"-?!0123456789";
