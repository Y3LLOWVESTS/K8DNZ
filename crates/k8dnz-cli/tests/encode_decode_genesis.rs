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

#[test]
fn genesis1_roundtrip_encode_decode_matches_bytes() {
    // Canonical sample per project rules.
    let input = repo_path("text/Genesis1.txt");
    assert!(
        input.exists(),
        "missing {} (expected canonical sample)",
        input.display()
    );

    let ark = tmp_path("genesis1", "ark");
    let decoded = tmp_path("genesis1_decoded", "txt");

    // Encode: default recipe (tuned profile) + explicit max-ticks.
    let mut enc = Command::new(env!("CARGO_BIN_EXE_k8dnz-cli"));
    enc.args([
        "encode",
        "--in",
        input.to_str().unwrap(),
        "--out",
        ark.to_str().unwrap(),
        "--profile",
        "tuned",
        "--max-ticks",
        "50000000",
    ]);
    run_ok(&mut enc);

    // Decode: uses embedded recipe inside .ark
    let mut dec = Command::new(env!("CARGO_BIN_EXE_k8dnz-cli"));
    dec.args([
        "decode",
        "--in",
        ark.to_str().unwrap(),
        "--out",
        decoded.to_str().unwrap(),
        "--max-ticks",
        "50000000",
    ]);
    run_ok(&mut dec);

    let a = fs::read(&input).expect("read input");
    let b = fs::read(&decoded).expect("read decoded");

    assert_eq!(a, b, "decoded bytes differ from original");

    // Cleanup (best effort)
    let _ = fs::remove_file(&ark);
    let _ = fs::remove_file(&decoded);
}

#[test]
fn genesis1_encode_is_deterministic_for_same_recipe_and_args() {
    let input = repo_path("text/Genesis1.txt");
    assert!(input.exists(), "missing {}", input.display());

    let ark1 = tmp_path("genesis1_det1", "ark");
    let ark2 = tmp_path("genesis1_det2", "ark");

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
        ]);
        run_ok(&mut enc);
    }

    let a = fs::read(&ark1).expect("read ark1");
    let b = fs::read(&ark2).expect("read ark2");

    assert_eq!(a, b, "encode produced different .ark bytes on identical runs");

    let _ = fs::remove_file(&ark1);
    let _ = fs::remove_file(&ark2);
}
