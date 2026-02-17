// crates/k8dnz-core/tests/lane_ws_roundtrip.rs

use k8dnz_core::lane;
use k8dnz_core::repr::text_norm;

fn recipe_bytes_default() -> Vec<u8> {
    // Default recipe is available via Recipe::default(); encode to bytes.
    let r = k8dnz_core::Recipe::default();
    r.encode()
}

#[test]
fn ws_lane_split_reconstruct_roundtrips() {
    let input = b"ab cd\nef\n\n  xyz";
    let norm = text_norm::normalize_newlines(input);
    let lanes = k8dnz_core::repr::ws_lanes::WsLanes::split(&norm);
    let out = lanes.reconstruct().expect("reconstruct");
    assert_eq!(out, norm);
}

#[test]
fn k8l1_encode_decode_roundtrips_normalized() {
    let input = b"Hello\r\nworld\rtest\n\nA B";
    let recipe_bytes = recipe_bytes_default();

    let (artifact, _stats) = lane::encode_k8l1(input, &recipe_bytes, 20_000_000).expect("encode");
    let decoded = lane::decode_k8l1(&artifact).expect("decode");

    let norm = text_norm::normalize_newlines(input);
    assert_eq!(decoded, norm);
}

#[test]
fn k8l1_is_deterministic() {
    let input = b"abc def\nghi jkl\n";
    let recipe_bytes = recipe_bytes_default();

    let (a1, _s1) = lane::encode_k8l1(input, &recipe_bytes, 20_000_000).expect("encode1");
    let (a2, _s2) = lane::encode_k8l1(input, &recipe_bytes, 20_000_000).expect("encode2");
    assert_eq!(a1, a2);
}
