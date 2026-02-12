// crates/k8dnz-cli/src/io/bin.rs

use anyhow::Context;
use k8dnz_core::signal::token::{PairToken, RgbPairToken};

/// Legacy/compat: write PairToken stream as packed bytes to a file.
/// byte = (a<<4)|b
pub fn write_bytes_file(path: &str, toks: &[PairToken]) -> anyhow::Result<()> {
    let mut out = Vec::with_capacity(toks.len());
    for t in toks {
        out.push(t.pack_n16());
    }
    std::fs::write(path, out).with_context(|| format!("write bytes bin: {path}"))?;
    Ok(())
}

/// New: write RGB pair stream as packed bytes to a file.
/// 6 bytes per emission: A.rgb then C.rgb
pub fn write_rgbpairs_file(path: &str, toks: &[RgbPairToken]) -> anyhow::Result<()> {
    let mut out = Vec::with_capacity(toks.len() * 6);
    for t in toks {
        out.extend_from_slice(&t.to_bytes());
    }
    std::fs::write(path, out).with_context(|| format!("write rgbpairs bin: {path}"))?;
    Ok(())
}
