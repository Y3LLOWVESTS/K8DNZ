use k8dnz_core::{recipe::defaults::default_recipe, Engine};

/// Hard-locks the time evolution of the default tuned recipe by asserting
/// that producing exactly 256 emissions consumes an exact number of ticks.
///
/// Why this matters:
/// - If “time” (tick evolution) drifts even slightly, the emission stream changes.
/// - This test forces us to keep cadence stable across refactors.
#[test]
fn cadence_ticks_for_256_emissions_is_stable_default_tuned() {
    let r = default_recipe(); // default is tuned (includes quant.shift=7_141_012)
    let mut e = Engine::new(r).expect("engine init failed");

    // Big budget to avoid accidental early stop.
    let toks = e.run_emissions(256, 50_000_000);
    assert_eq!(toks.len(), 256, "engine did not produce 256 emissions");

    // These should be exact if the engine is consistent.
    assert_eq!(e.stats.emissions, 256, "stats.emissions mismatch");
    assert_eq!(e.stats.alignments, 256, "stats.alignments mismatch");

    let ticks = e.stats.ticks;

    // ---- LOCKED VALUE (captured via UPDATE_GOLDENS=1) ----
    const EXPECTED_TICKS: u64 = 993_399;
    // ------------------------------------------------------

    if std::env::var("UPDATE_GOLDENS").is_ok() {
        eprintln!(
            "LOCK THIS VALUE: const EXPECTED_TICKS: u64 = {};  (default tuned recipe, 256 emissions)",
            ticks
        );
        return;
    }

    assert_eq!(
        ticks, EXPECTED_TICKS,
        "cadence tick drift detected: 256 emissions no longer lands on the same tick count"
    );
}
