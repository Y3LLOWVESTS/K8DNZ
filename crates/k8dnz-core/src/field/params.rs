use crate::recipe::recipe::{FieldClampParams, FieldParams};

#[derive(Clone, Copy, Debug)]
pub struct FieldEvalCfg {
    /// Clamp final sample into this inclusive range.
    /// Must be wide enough that typical wave sums don't saturate.
    pub clamp_min: i64,
    pub clamp_max: i64,
}

impl Default for FieldEvalCfg {
    fn default() -> Self {
        // Back-compat default.
        Self {
            clamp_min: -100_000_000,
            clamp_max: 100_000_000,
        }
    }
}

impl From<FieldClampParams> for FieldEvalCfg {
    fn from(c: FieldClampParams) -> Self {
        Self {
            clamp_min: c.min,
            clamp_max: c.max,
        }
    }
}

#[derive(Clone, Debug)]
pub struct FieldModel {
    pub params: FieldParams,
    pub cfg: FieldEvalCfg,
}

impl FieldModel {
    pub fn new(params: FieldParams, cfg: FieldEvalCfg) -> Self {
        Self { params, cfg }
    }
}
