use k8dnz_apextrace::{bytes_to_quats, quats_to_bytes, ApexKey};

#[test]
fn apex_key_roundtrips() {
    let key = ApexKey::new_dibit_v1(17, 3, 42, 99).unwrap();
    let enc = key.encode().unwrap();
    let dec = ApexKey::decode(&enc).unwrap();
    assert_eq!(key, dec);
}

#[test]
fn dibit_roundtrip() {
    let src = b"ApexTrace";
    let q = bytes_to_quats(src).unwrap();
    let out = quats_to_bytes(&q).unwrap();
    assert_eq!(src.to_vec(), out);
}