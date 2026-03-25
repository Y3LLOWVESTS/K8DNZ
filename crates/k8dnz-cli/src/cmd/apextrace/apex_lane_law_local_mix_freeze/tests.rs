use super::overrides::{override_path_bytes, select_override_subset_exact};
use super::types::OverrideCandidate;

#[test]
fn override_path_bytes_is_deterministic() {
    let rows = vec![
        OverrideCandidate {
            input: "a".into(),
            window_idx: 3,
            target_ordinal: 1,
            best_chunk_bytes: 96,
            default_payload_exact: 100,
            best_payload_exact: 90,
            gain_exact: 10,
        },
        OverrideCandidate {
            input: "a".into(),
            window_idx: 8,
            target_ordinal: 2,
            best_chunk_bytes: 64,
            default_payload_exact: 100,
            best_payload_exact: 95,
            gain_exact: 5,
        },
    ];
    assert_eq!(override_path_bytes(&rows), override_path_bytes(&rows));
    assert!(override_path_bytes(&rows) > 0);
}

#[test]
fn exact_subset_skips_small_gain_when_path_cost_eats_it() {
    let rows = vec![
        OverrideCandidate {
            input: "a".into(),
            window_idx: 1,
            target_ordinal: 0,
            best_chunk_bytes: 96,
            default_payload_exact: 100,
            best_payload_exact: 97,
            gain_exact: 3,
        },
        OverrideCandidate {
            input: "a".into(),
            window_idx: 1000,
            target_ordinal: 1,
            best_chunk_bytes: 128,
            default_payload_exact: 100,
            best_payload_exact: 99,
            gain_exact: 1,
        },
    ];
    let picked = select_override_subset_exact(&rows, &[0, 1]);
    assert!(picked.contains(&0));
    assert!(!picked.contains(&1));
}
