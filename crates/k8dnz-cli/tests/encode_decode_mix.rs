// crates/k8dnz-cli/tests/encode_decode_mix.rs

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
    p.push(format!("k8dnz_{}_{}_{}_{}.{}", name, pid, nanos, "tmp", ext));
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

fn distinct(bytes: &[u8]) -> usize {
    let mut seen = [false; 256];
    for &b in bytes {
        seen[b as usize] = true;
    }
    seen.into_iter().filter(|x| *x).count()
}

#[test]
fn encode_decode_roundtrip_with_splitmix64_is_lossless_and_deterministic() {
    let input = repo_path("text/Genesis1.txt");
    assert!(input.exists(), "missing {}", input.display());

    let ark1 = tmp_path("genesis1_mix1", "ark");
    let ark2 = tmp_path("genesis1_mix2", "ark");
    let decoded = tmp_path("genesis1_mix_decoded", "txt");

    let key_used = tmp_path("genesis1_mix_used", "bin");
    let key_raw = tmp_path("genesis1_mix_raw", "bin");

    for out_path in [&ark1, &ark2] {
        let mut enc = Command::new(env!("CARGO_BIN_EXE_k8dnz-cli"));
        enc.args([
            "encode",
            "--in",
            input.to_str().unwrap(),
            "--out",
            out_path.to_str().unwrap(),
            "--profile",
            "tuned",
            "--max-ticks",
            "50000000",
            "--keystream-mix",
            "splitmix64",
            "--payload",
            "cipher",
            "--dump-keystream",
            key_used.to_str().unwrap(),
            "--dump-raw-keystream",
            key_raw.to_str().unwrap(),
        ]);
        run_ok(&mut enc);
    }

    // Deterministic .ark bytes for same args
    let a = fs::read(&ark1).expect("read ark1");
    let b = fs::read(&ark2).expect("read ark2");
    assert_eq!(a, b, "mixed encode produced different .ark bytes on identical runs");

    // Decode roundtrip
    let mut dec = Command::new(env!("CARGO_BIN_EXE_k8dnz-cli"));
    dec.args([
        "decode",
        "--in",
        ark1.to_str().unwrap(),
        "--out",
        decoded.to_str().unwrap(),
        "--max-ticks",
        "50000000",
    ]);
    run_ok(&mut dec);

    let orig = fs::read(&input).expect("read input");
    let got = fs::read(&decoded).expect("read decoded");
    assert_eq!(orig, got, "decoded bytes differ from original (mix enabled)");

    // Directional distribution check: mixed distinct should not be worse than raw.
    let used = fs::read(&key_used).expect("read used key");
    let raw = fs::read(&key_raw).expect("read raw key");
    assert_eq!(used.len(), raw.len());
    assert!(
        distinct(&used) >= distinct(&raw),
        "expected mixed keystream distinct >= raw distinct"
    );

    // Cleanup (best effort)
    let _ = fs::remove_file(&ark1);
    let _ = fs::remove_file(&ark2);
    let _ = fs::remove_file(&decoded);
    let _ = fs::remove_file(&key_used);
    let _ = fs::remove_file(&key_raw);
}
