// crates/k8dnz-core/tests/genesis1_roundtrip.rs

use std::path::PathBuf;

use k8dnz_core::{Engine, Recipe};
use k8dnz_core::signal::token::PairToken;

fn workspace_root_from_core_manifest() -> PathBuf {
    // CARGO_MANIFEST_DIR points to crates/k8dnz-core
    // workspace root is two levels up.
    let here = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    here.parent()
        .and_then(|p| p.parent())
        .expect("expected crates/k8dnz-core to be two levels under workspace root")
        .to_path_buf()
}

/// Generate `n_bytes` of keystream using ONLY core primitives:
/// - run Engine emissions
/// - pack PairToken nibble pairs into bytes
fn keystream_bytes_core(engine: &mut Engine, n_bytes: usize, max_ticks: u64) -> Vec<u8> {
    // For N16: each emission => one packed byte: (a<<4) | b
    let toks: Vec<PairToken> = engine.run_emissions(n_bytes as u64, max_ticks);

    // Guard: if max_ticks is too small, we'd generate fewer bytes.
    assert_eq!(
        toks.len(),
        n_bytes,
        "keystream short: requested {} bytes but got {} (max_ticks too small?)",
        n_bytes,
        toks.len()
    );

    let mut out = Vec::with_capacity(n_bytes);
    for t in toks {
        let a = (t.a & 0x0F) as u8;
        let b = (t.b & 0x0F) as u8;
        out.push((a << 4) | b);
    }
    out
}

#[test]
fn genesis1_keystream_xor_roundtrip_is_lossless() {
    // Hard rule: Genesis1 is canonical sample until we intentionally move on.
    let root = workspace_root_from_core_manifest();
    let genesis1 = root.join("text").join("Genesis1.txt");
    let plain = std::fs::read(&genesis1)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", genesis1.display()));

    let recipe: Recipe = k8dnz_core::recipe::defaults::default_recipe();

    // 1) Recipe format encode/decode roundtrip (guards .k8r stability)
    let enc = k8dnz_core::recipe::format::encode(&recipe);
    let dec = k8dnz_core::recipe::format::decode(&enc).expect("recipe decode failed");

    // spot-check critical fields (we don't need to re-assert every nested wave field)
    assert_eq!(dec.version, recipe.version);
    assert_eq!(dec.seed, recipe.seed);
    assert_eq!(dec.free.phi_a0, recipe.free.phi_a0);
    assert_eq!(dec.free.phi_c0, recipe.free.phi_c0);
    assert_eq!(dec.free.v_a, recipe.free.v_a);
    assert_eq!(dec.free.v_c, recipe.free.v_c);
    assert_eq!(dec.free.epsilon, recipe.free.epsilon);
    assert_eq!(dec.lock.v_l, recipe.lock.v_l);
    assert_eq!(dec.lock.delta, recipe.lock.delta);
    assert_eq!(dec.lock.t_step, recipe.lock.t_step);
    assert_eq!(dec.field_clamp.min, recipe.field_clamp.min);
    assert_eq!(dec.field_clamp.max, recipe.field_clamp.max);
    assert_eq!(dec.quant.min, recipe.quant.min);
    assert_eq!(dec.quant.max, recipe.quant.max);
    assert_eq!(dec.quant.shift, recipe.quant.shift);
    assert_eq!(dec.field.waves.len(), recipe.field.waves.len());

    // 2) Generate keystream and do XOR roundtrip (core codec invariant)
    let mut e1 = Engine::new(recipe.clone()).expect("engine init failed");
    let key = keystream_bytes_core(&mut e1, plain.len(), 50_000_000);

    let mut cipher = plain.clone();
    for (c, k) in cipher.iter_mut().zip(key.iter()) {
        *c ^= *k;
    }

    // Decode: same recipe => same keystream => XOR back to plain.
    let mut e2 = Engine::new(recipe.clone()).expect("engine init failed (decode)");
    let key2 = keystream_bytes_core(&mut e2, cipher.len(), 50_000_000);

    let mut roundtrip = cipher.clone();
    for (p, k) in roundtrip.iter_mut().zip(key2.iter()) {
        *p ^= *k;
    }

    assert_eq!(roundtrip, plain, "Genesis1 roundtrip mismatch");

    // Sanity: consistent cadence budget for same length.
    assert_eq!(e1.stats.emissions, e2.stats.emissions);
    assert_eq!(e1.stats.ticks, e2.stats.ticks);
}
