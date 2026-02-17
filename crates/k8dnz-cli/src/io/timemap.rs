// crates/k8dnz-cli/src/io/timemap.rs

use anyhow::{Context, Result};
use k8dnz_core::signal::timing_map::TimingMap;
use std::path::Path;

fn atomic_write(path: &str, bytes: &[u8], default_name: &str) -> Result<()> {
    let pathp = Path::new(path);

    if let Some(parent) = pathp.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create parent dirs for timemap {path}"))?;
        }
    }

    let tmp_path = {
        let fname = pathp
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or(default_name);
        let tmp_name = format!("{fname}.tmp");
        pathp.with_file_name(tmp_name)
    };

    std::fs::write(&tmp_path, bytes)
        .with_context(|| format!("write timemap temp {}", tmp_path.display()))?;

    std::fs::rename(&tmp_path, pathp).with_context(|| {
        format!(
            "rename timemap temp {} -> {}",
            tmp_path.display(),
            pathp.display()
        )
    })?;

    Ok(())
}
#[allow(dead_code)]
pub fn write_tm1(path: &str, tm: &TimingMap) -> Result<()> {
    let bytes = tm.encode_tm1();
    atomic_write(path, &bytes, "timemap.tm1")
}

pub fn write_timemap_auto(path: &str, tm: &TimingMap) -> Result<()> {
    let bytes = tm.encode_auto();
    atomic_write(path, &bytes, "timemap.tm")
}
#[allow(dead_code)]
pub fn read_tm1(path: &str) -> Result<TimingMap> {
    let bytes = std::fs::read(path).with_context(|| format!("read timemap {path}"))?;
    let tm = TimingMap::decode_tm1(&bytes)
        .map_err(|e| anyhow::anyhow!("{e}"))
        .with_context(|| format!("decode tm1 {path}"))?;
    Ok(tm)
}

pub fn read_timemap(path: &str) -> Result<TimingMap> {
    let bytes = std::fs::read(path).with_context(|| format!("read timemap {path}"))?;
    let tm = TimingMap::decode_auto(&bytes)
        .map_err(|e| anyhow::anyhow!("{e}"))
        .with_context(|| format!("decode timemap {path}"))?;
    Ok(tm)
}
