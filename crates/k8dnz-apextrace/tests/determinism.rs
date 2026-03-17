use k8dnz_apextrace::{generate_bytes, generate_quats, ApexKey};

#[test]
fn same_key_same_quats_and_bytes() {
    let key = ApexKey::new_dibit_v1(
        64,
        2,
        0x1234_5678_9ABC_DEF0,
        0x0F0E_0D0C_0B0A_0908,
    )
    .unwrap();

    let q1 = generate_quats(&key).unwrap();
    let q2 = generate_quats(&key).unwrap();
    let b1 = generate_bytes(&key).unwrap();
    let b2 = generate_bytes(&key).unwrap();

    assert_eq!(q1, q2);
    assert_eq!(b1, b2);
    assert_eq!(q1.len(), 256);
    assert_eq!(b1.len(), 64);
}