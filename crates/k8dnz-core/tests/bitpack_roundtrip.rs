// crates/k8dnz-core/tests/bitpack_roundtrip.rs

use k8dnz_core::signal::bitpack::{pack_symbols, unpack_symbols};

fn lcg_next(x: &mut u64) -> u64 {
    // deterministic, not crypto
    *x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
    *x
}

#[test]
fn bitpack_roundtrip_all_widths() {
    let mut seed: u64 = 0x1234_5678_9abc_def0;

    for bits in 1u8..=8u8 {
        let mask: u8 = ((1u16 << bits) - 1) as u8;

        for &n in &[0usize, 1, 2, 3, 7, 8, 9, 15, 16, 17, 31, 32, 33, 127, 128, 129] {
            let mut syms = Vec::with_capacity(n);
            for _ in 0..n {
                let r = (lcg_next(&mut seed) >> 56) as u8;
                syms.push(r & mask);
            }

            let packed = pack_symbols(bits, &syms).expect("pack ok");
            let out = unpack_symbols(bits, &packed, syms.len()).expect("unpack ok");
            assert_eq!(syms, out, "bits={} n={}", bits, n);
        }
    }
}

#[test]
fn bitpack_rejects_out_of_range_symbols() {
    let err = pack_symbols(2, &[0, 1, 2, 3, 4]).unwrap_err();
    let msg = format!("{err:?}");
    assert!(msg.contains("out of range"));
}

#[test]
fn bitpack_rejects_bad_bitwidth() {
    assert!(pack_symbols(0, &[0]).is_err());
    assert!(pack_symbols(9, &[0]).is_err());
    assert!(unpack_symbols(0, &[0], 1).is_err());
    assert!(unpack_symbols(9, &[0], 1).is_err());
}
