// crates/k8dnz-cli/src/io/recipe_file.rs

use anyhow::{Context, Result};
use k8dnz_core::recipe::format as recipe_format;
use k8dnz_core::Recipe;

/// Load a .k8r recipe file and decode into a Recipe struct.
pub fn load_k8r(path: &str) -> Result<Recipe> {
    let bytes = std::fs::read(path).with_context(|| format!("read recipe {path}"))?;
    let recipe = recipe_format::decode(&bytes).with_context(|| format!("decode recipe {path}"))?;
    Ok(recipe)
}

/// Load raw recipe bytes from a .k8r file (used by encode2kb / lane_sweep).
pub fn load_k8r_bytes(path: &str) -> Result<Vec<u8>> {
    std::fs::read(path).with_context(|| format!("read recipe bytes {path}"))
}

/// Save a Recipe as a .k8r file.
pub fn save_k8r(path: &str, recipe: &Recipe) -> Result<()> {
    // recipe_format::encode returns Vec<u8> (not Result), so no `.context()` here.
    let bytes = recipe_format::encode(recipe);
    std::fs::write(path, bytes).with_context(|| format!("write recipe {path}"))?;
    Ok(())
}
