use k8dnz_core::recipe::ark_key::{decode_ark1s, encode_ark1s};
use k8dnz_core::recipe::defaults::default_recipe;

#[test]
fn ark1s_roundtrip_matches_recipe_bytes_semantics() {
    let r1 = default_recipe();
    let s = encode_ark1s(&r1);
    let r2 = decode_ark1s(&s).unwrap();

    assert_eq!(r1.version, r2.version);
    assert_eq!(r1.seed, r2.seed);
    assert_eq!(r1.alphabet as u8, r2.alphabet as u8);
    assert_eq!(r1.reset_mode as u8, r2.reset_mode as u8);
    assert_eq!(r1.keystream_mix as u8, r2.keystream_mix as u8);
    assert_eq!(r1.payload_kind as u8, r2.payload_kind as u8);

    assert_eq!(r1.free.phi_a0.0, r2.free.phi_a0.0);
    assert_eq!(r1.free.phi_c0.0, r2.free.phi_c0.0);
    assert_eq!(r1.free.v_a.0, r2.free.v_a.0);
    assert_eq!(r1.free.v_c.0, r2.free.v_c.0);
    assert_eq!(r1.free.epsilon.0, r2.free.epsilon.0);

    assert_eq!(r1.lock.v_l.0, r2.lock.v_l.0);
    assert_eq!(r1.lock.delta.0, r2.lock.delta.0);
    assert_eq!(r1.lock.t_step, r2.lock.t_step);

    assert_eq!(r1.field_clamp.min, r2.field_clamp.min);
    assert_eq!(r1.field_clamp.max, r2.field_clamp.max);

    assert_eq!(r1.quant.min, r2.quant.min);
    assert_eq!(r1.quant.max, r2.quant.max);
    assert_eq!(r1.quant.shift, r2.quant.shift);

    assert_eq!(r1.field.waves.len(), r2.field.waves.len());
    for (a, b) in r1.field.waves.iter().zip(r2.field.waves.iter()) {
        assert_eq!(a.k_phi, b.k_phi);
        assert_eq!(a.k_t, b.k_t);
        assert_eq!(a.k_time, b.k_time);
        assert_eq!(a.phase, b.phase);
        assert_eq!(a.amp, b.amp);
    }
}
