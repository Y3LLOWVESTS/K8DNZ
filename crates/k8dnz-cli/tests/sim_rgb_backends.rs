use std::process::Command;

fn run_sim(backend: &str) -> Vec<String> {
    let out = Command::new(env!("CARGO_BIN_EXE_k8dnz-cli"))
        .args([
            "sim",
            "--emissions",
            "50",
            "--mode",
            "rgbpair",
            "--fmt",
            "jsonl",
            "--rgb-from-field",
            "--rgb-backend",
            backend,
        ])
        .output()
        .expect("run k8dnz-cli sim");

    assert!(
        out.status.success(),
        "sim failed: status={:?}\nstdout:\n{}\nstderr:\n{}",
        out.status.code(),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    // stdout is JSONL tokens; stderr contains recipe_id / sim ok lines.
    let stdout = String::from_utf8(out.stdout).expect("utf8 stdout");
    stdout
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect()
}

#[test]
fn rgb_backend_repeatability() {
    let a1 = run_sim("dna");
    let a2 = run_sim("dna");
    assert_eq!(a1, a2, "dna backend output changed between identical runs");

    let c1 = run_sim("cone");
    let c2 = run_sim("cone");
    assert_eq!(c1, c2, "cone backend output changed between identical runs");
}

#[test]
fn rgb_backend_diverges_and_resonates() {
    let dna = run_sim("dna");
    let cone = run_sim("cone");

    assert_eq!(dna.len(), 50);
    assert_eq!(cone.len(), 50);

    let mut any_diff = false;
    let mut any_equal = false;
    let mut equals_at_mod3 = 0usize;
    let mut equals_off_mod3 = 0usize;

    for (i, (a, b)) in dna.iter().zip(cone.iter()).enumerate() {
        if a == b {
            any_equal = true;
            if i % 3 == 0 {
                equals_at_mod3 += 1;
            } else {
                equals_off_mod3 += 1;
            }
        } else {
            any_diff = true;
        }
    }

    assert!(any_diff, "expected dna and cone to differ at least once");
    assert!(any_equal, "expected dna and cone to match at least once (Option A resonance)");

    // Option A signature: matches should occur at i % 3 == 0.
    // We allow zero off-mod3 matches; if they appear later due to parameter changes,
    // we can relax this, but right now your behavior is exactly this pattern.
    assert!(
        equals_at_mod3 >= 10,
        "expected many resonance matches at i%3==0; got {}",
        equals_at_mod3
    );
    assert_eq!(
        equals_off_mod3, 0,
        "unexpected resonance matches off i%3==0; got {}",
        equals_off_mod3
    );
}
