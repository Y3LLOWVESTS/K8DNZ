use std::path::PathBuf;

use k8dnz_cli::merkle::runner::FitProfile;
use k8dnz_cli::merkle::unzip::merkle_unzip_to_bytes;
use k8dnz_cli::merkle::zip::merkle_zip_bytes;

fn recipe_path() -> String {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop(); // crates/k8dnz-cli
    p.pop(); // crates
    p.push("configs");
    p.push("tuned_validated.k8r");
    p.to_string_lossy().to_string()
}

#[test]
fn arkm_roundtrip_small() {
    let recipe = recipe_path();
    let input = b"hello merkle-zip / merkle-unzip";

    let prof = FitProfile::default();
    let (root, _rep) = merkle_zip_bytes(&recipe, input, 16, &prof, 1).expect("zip");
    let (out, _urep) = merkle_unzip_to_bytes(&root).expect("unzip");

    assert_eq!(out, input);
}

#[test]
fn arkm_determinism_same_inputs_same_root() {
    let recipe = recipe_path();
    let input = b"determinism check";

    let prof = FitProfile::default();
    let (a, _) = merkle_zip_bytes(&recipe, input, 16, &prof, 1).expect("zip A");
    let (b, _) = merkle_zip_bytes(&recipe, input, 16, &prof, 1).expect("zip B");

    assert_eq!(a, b);
}
