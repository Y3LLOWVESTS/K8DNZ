use k8dnz_core::{recipe::defaults::default_recipe, Engine};

fn hash8_first_32(mut e: Engine) -> [u8; 8] {
    let toks = e.run_emissions(32, 2_000_000);

    let mut hasher = blake3::Hasher::new();
    for t in toks {
        hasher.update(&[t.a, t.b]);
    }
    let h = hasher.finalize();
    let mut out = [0u8; 8];
    out.copy_from_slice(&h.as_bytes()[0..8]);
    out
}

#[test]
fn golden_first_32_tokens_stable_baseline_shift0() {
    let mut r = default_recipe();
    r.quant.shift = 0; // baseline (legacy) explicitly

    let e = Engine::new(r).unwrap();
    let got = hash8_first_32(e);

    // Locked on 2026-02-10 (baseline recipe + shift=0).
    const BASELINE_EXPECTED: [u8; 8] = [125, 12, 230, 47, 23, 87, 136, 99];

    assert_eq!(got, BASELINE_EXPECTED);
}

#[test]
fn golden_first_32_tokens_stable_default_tuned() {
    let r = default_recipe(); // now includes shift=7_141_012 by default
    let e = Engine::new(r).unwrap();
    let got = hash8_first_32(e);

    // Locked on 2026-02-10 (default recipe, shift=7_141_012).
    const TUNED_EXPECTED: [u8; 8] = [164, 199, 222, 110, 252, 37, 83, 1];

    assert_eq!(got, TUNED_EXPECTED);
}
