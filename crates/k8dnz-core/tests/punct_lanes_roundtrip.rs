// crates/k8dnz-core/tests/punct_kind_lanes_roundtrip.rs

use k8dnz_core::repr::{
    punct_kind_lanes::{classify_punct_kind_byte, punct_kind_label, PunctKindLanes},
    punct_lanes::PunctLanes,
    text_norm,
};

#[test]
fn punct_kind_split_reconstruct_roundtrips() {
    let input = b"Hello, world!\n\"A-B\" (test): yes?";
    let norm = text_norm::normalize_newlines(input);

    let punct = PunctLanes::split(&norm);
    let kinds = PunctKindLanes::from_punct_bytes(&punct.punct_lane).expect("kind lanes");

    let out = kinds.reconstruct_punct_bytes().expect("reconstruct");
    assert_eq!(out, punct.punct_lane);
}

#[test]
fn punct_kind_direct_split_matches_nested_punct_lane() {
    let input = b"(A) [B] {C} - \"D\"; E, F: G?!";
    let punct = PunctLanes::split(input);
    let kinds_a = PunctKindLanes::split(input);
    let kinds_b = PunctKindLanes::from_punct_bytes(&punct.punct_lane).expect("nested");

    assert_eq!(kinds_a, kinds_b);
    assert_eq!(kinds_a.punct_bytes, b"()[]{}-\"\";,:?!");
}

#[test]
fn punct_kind_classes_are_stable() {
    assert_eq!(classify_punct_kind_byte(b'.'), Some(PunctKindLanes::KIND_TERM));
    assert_eq!(classify_punct_kind_byte(b'!'), Some(PunctKindLanes::KIND_TERM));
    assert_eq!(classify_punct_kind_byte(b','), Some(PunctKindLanes::KIND_PAUSE));
    assert_eq!(classify_punct_kind_byte(b';'), Some(PunctKindLanes::KIND_PAUSE));
    assert_eq!(classify_punct_kind_byte(b'('), Some(PunctKindLanes::KIND_WRAP));
    assert_eq!(classify_punct_kind_byte(b'\''), Some(PunctKindLanes::KIND_WRAP));
    assert_eq!(classify_punct_kind_byte(b'A'), None);
    assert_eq!(punct_kind_label(PunctKindLanes::KIND_WRAP), "wrap");
}