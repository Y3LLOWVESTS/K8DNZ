// crates/k8dnz-core/tests/case_lanes_roundtrip.rs

use k8dnz_core::repr::{
    case_lanes::{case_label, CaseLanes},
    text_norm,
};

#[test]
fn case_lane_split_reconstruct_roundtrips_letter_stream() {
    let input = b"Hello, WORLD!\nAbc xyz";
    let norm = text_norm::normalize_newlines(input);

    let lanes = CaseLanes::split(&norm);
    let out = lanes.reconstruct_letters().expect("reconstruct");

    let expected = norm
        .iter()
        .copied()
        .filter(|b| b.is_ascii_alphabetic())
        .collect::<Vec<_>>();

    assert_eq!(out, expected);
}

#[test]
fn case_lane_from_letter_bytes_roundtrips() {
    let input = b"HelloWORLDAbc";
    let lanes = CaseLanes::from_letter_bytes(input).expect("case lanes");
    let out = lanes.reconstruct_letters().expect("reconstruct");
    assert_eq!(out, input);
}

#[test]
fn case_lane_labels_are_stable() {
    assert_eq!(case_label(CaseLanes::CASE_LOWER), "lower");
    assert_eq!(case_label(CaseLanes::CASE_UPPER), "upper");
}