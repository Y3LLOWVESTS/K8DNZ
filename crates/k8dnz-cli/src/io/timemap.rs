use anyhow::Context;
use k8dnz_core::signal::timing_map::TimingMap;

pub fn write_tm1(path: &str, tm: &TimingMap) -> anyhow::Result<()> {
    let bytes = tm.encode_tm1();
    std::fs::write(path, &bytes).with_context(|| format!("write timemap {path}"))?;
    Ok(())
}

pub fn read_tm1(path: &str) -> anyhow::Result<TimingMap> {
    let bytes = std::fs::read(path).with_context(|| format!("read timemap {path}"))?;
    let tm = TimingMap::decode_tm1(&bytes).map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(tm)
}
