use std::path::{Path, PathBuf};

use k8dnz_core::{recipe::defaults::default_recipe, Engine};

fn workspace_root_from_core_manifest() -> PathBuf {
    // CARGO_MANIFEST_DIR points to crates/k8dnz-core
    // workspace root is two levels up.
    let here = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    here.parent()
        .and_then(|p| p.parent())
        .expect("expected crates/k8dnz-core to be two levels under workspace root")
        .to_path_buf()
}

fn find_tuned_recipe_path(root: &Path) -> PathBuf {
    // Preferred locations (support both; fail with a helpful message if missing).
    let candidates = [
        root.join("tuned_7141012.k8r"),
        root.join("configs").join("tuned_7141012.k8r"),
    ];

    for p in candidates.iter() {
        if p.exists() {
            return p.to_path_buf();
        }
    }

    panic!(
        "tuned recipe file not found. Expected one of:\n  {}\n  {}\n\
         Tip: move/copy tuned_7141012.k8r into ./configs/ so it is checked in and stable for tests.",
        candidates[0].display(),
        candidates[1].display(),
    );
}

fn hash8_first_32_tokens(mut e: Engine) -> [u8; 8] {
    let toks = e.run_emissions(32, 2_000_000);

    let mut hasher = blake3::Hasher::new();
    for t in toks {
        // Match golden_stream.rs convention: hash [a,b] pairs
        hasher.update(&[t.a, t.b]);
    }

    let h = hasher.finalize();
    let mut out = [0u8; 8];
    out.copy_from_slice(&h.as_bytes()[0..8]);
    out
}

#[test]
fn tuned_k8r_loads_and_produces_tuned_stream() {
    // 1) Load tuned recipe from disk (persistence contract)
    let root = workspace_root_from_core_manifest();
    let path = find_tuned_recipe_path(&root);

    let bytes = std::fs::read(&path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));

    let recipe = k8dnz_core::recipe::format::decode(&bytes)
        .unwrap_or_else(|e| panic!("failed to decode {}: {e}", path.display()));

    // 2) Assert the tuning knob is present and correct
    assert_eq!(
        recipe.quant.shift, 7_141_012,
        "unexpected quant.shift in {}",
        path.display()
    );

    // 3) Optional sanity: loaded recipe should match default recipe shape
    // (We keep these BEFORE running the engine so we don't fight moves/borrows.)
    let def = default_recipe();
    assert_eq!(recipe.version, def.version);
    assert_eq!(recipe.seed, def.seed);
    assert_eq!(recipe.field.waves.len(), def.field.waves.len());

    // 4) Strong regression guard: loading this recipe reproduces the tuned stream
    // Engine::new consumes Recipe, so we pass a clone (tests are fine to clone).
    let got = hash8_first_32_tokens(Engine::new(recipe.clone()).unwrap());

    // Locked on 2026-02-10 (default recipe, shift=7_141_012).
    // This ensures the artifact recipe produces the same tuned stream.
    const TUNED_EXPECTED: [u8; 8] = [164, 199, 222, 110, 252, 37, 83, 1];
    assert_eq!(got, TUNED_EXPECTED);
}
