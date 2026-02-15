use k8dnz_core::fixed::turn32::Turn32;
use k8dnz_core::{recipe::defaults::default_recipe, Engine};

#[test]
fn free_orbit_speeds_differ() {
    let r = default_recipe();
    assert_ne!(r.free.v_a.0, r.free.v_c.0);
}

#[test]
fn lockstep_delta_is_half_by_default() {
    let r = default_recipe();
    assert_eq!(r.lock.delta.0, Turn32::HALF.0);
}

#[test]
fn emits_some_tokens() {
    let r = default_recipe();
    let mut e = Engine::new(r).unwrap();
    let toks = e.run_emissions(8, 2_000_000);
    assert_eq!(toks.len(), 8);
}
