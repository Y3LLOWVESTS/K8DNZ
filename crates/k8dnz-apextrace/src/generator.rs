use crate::dibit::quats_to_bytes;
use crate::error::Result;
use crate::key::ApexKey;
use crate::law::{descend, emit_quat, root_state};

pub fn generate_quats(key: &ApexKey) -> Result<Vec<u8>> {
    key.validate()?;

    let root = root_state(key.root_quadrant, key.root_seed);
    let mut out = Vec::with_capacity(key.quat_len as usize);

    let mut leaf = 0u64;
    while leaf < key.quat_len {
        let state = descend(root, key.depth, leaf, key.recipe_seed);
        out.push(emit_quat(state));
        leaf += 1;
    }

    Ok(out)
}

pub fn generate_bytes(key: &ApexKey) -> Result<Vec<u8>> {
    let quats = generate_quats(key)?;
    quats_to_bytes(&quats)
}