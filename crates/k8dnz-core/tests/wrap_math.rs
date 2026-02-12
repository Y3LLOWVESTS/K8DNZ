use k8dnz_core::fixed::{math::turn32_dist, turn32::Turn32};

#[test]
fn dist_wrap_works() {
    let a = Turn32(0xFFFF_F000);
    let b = Turn32(0x0000_1000);
    let d = turn32_dist(a, b);
    assert!(d.0 < 0x0000_5000, "expected small wrap distance, got {:08x}", d.0);
}
