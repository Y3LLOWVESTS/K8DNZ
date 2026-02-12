use std::path::PathBuf;

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

#[test]
fn cadence_ticks_for_genesis1_is_stable_default_tuned() {
    // Canonical test vector constraint: Genesis1.txt only for now.
    let root = workspace_root_from_core_manifest();
    let genesis_path = root.join("text").join("Genesis1.txt");

    let bytes = std::fs::read(&genesis_path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", genesis_path.display()));

    let emissions = bytes.len() as u64;
    assert_eq!(emissions, 4201, "Genesis1.txt size changed; update this test intentionally");

    let r = default_recipe(); // default tuned
    let mut e = Engine::new(r).expect("engine init failed");

    // Big budget to avoid early stop; we WANT exact emission count.
    let toks = e.run_emissions(emissions, 100_000_000);
    assert_eq!(
        toks.len() as u64,
        emissions,
        "engine did not produce expected emissions"
    );

    assert_eq!(e.stats.emissions, emissions, "stats.emissions mismatch");
    assert_eq!(e.stats.alignments, emissions, "stats.alignments mismatch");

    // Locked from known-good CLI run:
    // encode Genesis1.txt with tuned recipe printed:
    // ticks=16335504 emissions=4201
    const EXPECTED_TICKS: u64 = 16_335_504;

    assert_eq!(
        e.stats.ticks, EXPECTED_TICKS,
        "Genesis1 cadence tick drift detected"
    );
}
