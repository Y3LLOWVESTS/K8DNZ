use k8dnz_core::signal::timing_map::TimingMap;

#[test]
fn tm1_roundtrip_is_lossless_and_canonicalizes() {
    let tm = TimingMap::new(vec![10, 1, 5, 5, 2]).unwrap();
    assert_eq!(tm.indices, vec![1, 2, 5, 10]);

    let enc = tm.encode_tm1();
    let dec = TimingMap::decode_tm1(&enc).unwrap();
    assert_eq!(tm, dec);

    let enc2 = dec.encode_tm1();
    assert_eq!(enc, enc2);
}
