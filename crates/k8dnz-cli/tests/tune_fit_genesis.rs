// crates/k8dnz-cli/tests/tune_fit_genesis.rs

use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn repo_path(rel: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join(rel)
}

fn tmp_path(name: &str, ext: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let pid = std::process::id();
    p.push(format!("k8dnz_{}_{}_{}.{}", name, pid, nanos, ext));
    p
}

fn run_ok(cmd: &mut Command) {
    let out = cmd.output().expect("spawn command");
    assert!(
        out.status.success(),
        "command failed: status={:?}\nstdout:\n{}\nstderr:\n{}",
        out.status.code(),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

fn entropy_bits_256(h: &[u64; 256], total: u64) -> f64 {
    if total == 0 {
        return 0.0;
    }
    let mut ent = 0.0;
    for &c in h.iter() {
        if c == 0 {
            continue;
        }
        let p = (c as f64) / (total as f64);
        ent -= p * p.log2();
    }
    ent
}

fn byte_health(bytes: &[u8]) -> (usize, f64) {
    let mut h = [0u64; 256];
    for &b in bytes {
        h[b as usize] += 1;
    }
    let distinct = h.iter().filter(|&&c| c > 0).count();
    let ent = entropy_bits_256(&h, bytes.len() as u64);
    (distinct, ent)
}

#[test]
fn tune_fit_is_deterministic_and_decodes_losslessly_for_genesis1() {
    let input = repo_path("text/Genesis1.txt");
    assert!(input.exists(), "missing {}", input.display());

    let recipe1 = tmp_path("tune_fit1", "k8r");
    let recipe2 = tmp_path("tune_fit2", "k8r");
    let ark1 = tmp_path("tune_fit1", "ark");
    let ark2 = tmp_path("tune_fit2", "ark");
    let decoded = tmp_path("tune_fit_decoded", "txt");

    // NOTE:
    // Genesis1.txt is 4201 bytes. The tune run must be allowed to emit >= 4201 bytes,
    // otherwise you'll get "keystream short".
    // Empirically your backend needs ~16.3M ticks for 4201 emissions, so we cap at 20M.
    let tune_args = [
        "tune",
        "--fit-in",
        input.to_str().unwrap(),
        "--rank-by-effective-zstd",
        "--zstd-level",
        "1",
        "--passes",
        "2",
        "--candidates",
        "5",
        "--per-emissions",
        "5000",
        "--per-max-ticks",
        "20000000",
    ];

    for (out_recipe, out_ark) in [(&recipe1, &ark1), (&recipe2, &ark2)] {
        let mut tune = Command::new(env!("CARGO_BIN_EXE_k8dnz-cli"));
        tune.args(tune_args);
        tune.args(["--out-recipe", out_recipe.to_str().unwrap()]);
        tune.args(["--out-ark", out_ark.to_str().unwrap()]);
        run_ok(&mut tune);
    }

    // Determinism: recipe bytes and ark bytes match across identical runs.
    let r1 = fs::read(&recipe1).expect("read recipe1");
    let r2 = fs::read(&recipe2).expect("read recipe2");
    assert_eq!(r1, r2, "tune output .k8r differs across identical runs");

    let a1 = fs::read(&ark1).expect("read ark1");
    let a2 = fs::read(&ark2).expect("read ark2");
    assert_eq!(a1, a2, "tune output .ark differs across identical runs");

    // Lossless decode: decoded bytes equal original.
    let mut dec = Command::new(env!("CARGO_BIN_EXE_k8dnz-cli"));
    dec.args([
        "decode",
        "--in",
        ark1.to_str().unwrap(),
        "--out",
        decoded.to_str().unwrap(),
    ]);
    run_ok(&mut dec);

    let orig = fs::read(&input).expect("read input");
    let got = fs::read(&decoded).expect("read decoded");
    assert_eq!(orig, got, "decoded bytes differ from original after tune fit");

    // NEW: Keystream health check
    // If the tuned recipe produces a dead stream (e.g., all 0x00), fail the test.
    let regen_out = tmp_path("tune_fit_keystream", "bin");
    let mut regen = Command::new(env!("CARGO_BIN_EXE_k8dnz-cli"));
    regen.args([
        "regen",
        "--recipe",
        recipe1.to_str().unwrap(),
        "--out",
        "bin",
        "--emissions",
        "4096",
        "--max-ticks",
        "20000000",
        "--output",
        regen_out.to_str().unwrap(),
    ]);
    run_ok(&mut regen);

    let ks = fs::read(&regen_out).expect("read regenerated keystream bytes");
    assert_eq!(ks.len(), 4096, "unexpected regen output length");
    let (distinct, ent) = byte_health(&ks);

    assert!(
        distinct > 2 && ent > 0.50,
        "dead/near-dead keystream: distinct={}/256 entropy_bits={:.4}",
        distinct,
        ent
    );

    // Cleanup (best-effort)
    let _ = fs::remove_file(&recipe1);
    let _ = fs::remove_file(&recipe2);
    let _ = fs::remove_file(&ark1);
    let _ = fs::remove_file(&ark2);
    let _ = fs::remove_file(&decoded);
    let _ = fs::remove_file(&regen_out);
}
