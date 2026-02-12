// crates/k8dnz-cli/src/io/jsonl.rs

use anyhow::Context;
use k8dnz_core::signal::token::{PairToken, RgbPairToken};

/// Legacy/compat: write PairToken stream as JSONL to a file.
/// Format: {"a":N,"b":N}
pub fn write_tokens_file(path: &str, toks: &[PairToken]) -> anyhow::Result<()> {
    let mut s = String::new();
    for t in toks {
        s.push_str(&format!("{{\"a\":{},\"b\":{}}}\n", t.a, t.b));
    }
    std::fs::write(path, s).with_context(|| format!("write tokens jsonl: {path}"))?;
    Ok(())
}

/// Legacy/compat: write PairToken stream as JSONL to stdout.
/// Format: {"a":N,"b":N}
pub fn write_tokens_stdout(toks: &[PairToken]) -> anyhow::Result<()> {
    for t in toks {
        println!("{{\"a\":{},\"b\":{}}}", t.a, t.b);
    }
    Ok(())
}

/// New: write RGB pair stream as JSONL to a file.
/// Format: {"a":[r,g,b],"c":[r,g,b]}
pub fn write_rgbpairs_file(path: &str, toks: &[RgbPairToken]) -> anyhow::Result<()> {
    let mut s = String::new();
    for t in toks {
        s.push_str(&format!(
            "{{\"a\":[{},{},{}],\"c\":[{},{},{}]}}\n",
            t.a.r, t.a.g, t.a.b, t.c.r, t.c.g, t.c.b
        ));
    }
    std::fs::write(path, s).with_context(|| format!("write rgbpairs jsonl: {path}"))?;
    Ok(())
}

/// New: write RGB pair stream as JSONL to stdout.
/// Format: {"a":[r,g,b],"c":[r,g,b]}
pub fn write_rgbpairs_stdout(toks: &[RgbPairToken]) -> anyhow::Result<()> {
    for t in toks {
        println!(
            "{{\"a\":[{},{},{}],\"c\":[{},{},{}]}}",
            t.a.r, t.a.g, t.a.b, t.c.r, t.c.g, t.c.b
        );
    }
    Ok(())
}
