use k8dnz_core::recipe::{format, defaults};
use k8dnz_core::Recipe;

pub fn load_k8r(path: &str) -> anyhow::Result<Recipe> {
    let bytes = std::fs::read(path)?;
    Ok(format::decode(&bytes)?)
}

#[allow(dead_code)]
pub fn save_k8r(path: &str, r: &Recipe) -> anyhow::Result<()> {
    let bytes = format::encode(r);
    std::fs::write(path, bytes)?;
    Ok(())
}

#[allow(dead_code)]
pub fn save_default_k8r(path: &str) -> anyhow::Result<()> {
    let r = defaults::default_recipe();
    save_k8r(path, &r)
}
