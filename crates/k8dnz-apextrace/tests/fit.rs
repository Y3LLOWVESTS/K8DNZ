use k8dnz_apextrace::{analyze_key_against_bytes, generate_bytes, ApexKey};

#[test]
fn diagnostics_are_exact_for_self_generated_bytes() {
    let key = ApexKey::new_dibit_v1(4, 2, 0xABCD_EF01, 0x0123_4567).unwrap();
    let bytes = generate_bytes(&key).unwrap();
    let diag = analyze_key_against_bytes(&key, &bytes).unwrap();

    assert_eq!(diag.score.matches, diag.score.total);
    assert_eq!(diag.score.prefix, diag.score.total);
    assert_eq!(diag.score.hamming(), 0);
    assert_eq!(diag.byte_matches, key.byte_len);
    assert_eq!(diag.longest_run, diag.score.total);
    assert_eq!(diag.longest_run_start, 0);
    assert_eq!(diag.target_hist, diag.pred_hist);
}
