// crates/k8dnz-core/tests/orbexp_closed_form.rs

use k8dnz_core::orbexp::{compute_first_meet, simulate_first_meet, OrbParams};

#[test]
fn orbexp_closed_form_matches_simulation_small() {
    // Use a small modulus so simulation is fast and exhaustive-ish.
    let modn = 997u64;

    // A few deterministic step pairs.
    let cases = [
        (1u64, 2u64),
        (10u64, 10u64),
        (123u64, 456u64),
        (996u64, 1u64),
        (500u64, 750u64),
    ];

    for (step_a, step_c) in cases {
        let params = OrbParams {
            modn,
            step_a,
            step_c,
        };

        let r = compute_first_meet(params).unwrap();

        // Closed-form t is the period for returning to equality from t=0.
        // If d==0 => t_first_meet==0 is correct.
        if r.t_first_meet == 0 {
            assert_eq!(step_a % modn, step_c % modn);
            continue;
        }

        // For validation, look for first positive meet.
        // We simulate starting at t=0 equality; after 1..t should return at exactly t.
        let sim = simulate_first_meet(params, r.t_first_meet + 5)
            .unwrap()
            .unwrap();
        assert_eq!(sim, 0);

        // Check that after t_first_meet steps we are back in equality (positive meet).
        let mut a = 0u64;
        let mut c = 0u64;
        for _ in 0..r.t_first_meet {
            a = (a + (step_a % modn)) % modn;
            c = (c + (step_c % modn)) % modn;
        }
        assert_eq!(a, c);

        // And we should not meet earlier (for 1..t-1).
        let mut a2 = 0u64;
        let mut c2 = 0u64;
        for i in 1..r.t_first_meet {
            a2 = (a2 + (step_a % modn)) % modn;
            c2 = (c2 + (step_c % modn)) % modn;
            assert_ne!(a2, c2, "met early at i={i} step_a={step_a} step_c={step_c}");
        }
    }
}
