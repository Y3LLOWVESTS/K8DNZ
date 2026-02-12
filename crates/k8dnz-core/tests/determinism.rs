use k8dnz_core::{Engine, recipe::defaults::default_recipe};

#[test]
fn deterministic_stream() {
    let r = default_recipe();
    let mut e1 = Engine::new(r.clone()).unwrap();
    let mut e2 = Engine::new(r).unwrap();

    let t1 = e1.run_emissions(256, 5_000_000);
    let t2 = e2.run_emissions(256, 5_000_000);

    assert_eq!(t1.len(), 256);
    assert_eq!(t1, t2);
}
