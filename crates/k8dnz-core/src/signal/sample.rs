/// Contract: field samples are clamped to a known range before quantization.
/// We keep this as i64 to avoid overflow during accumulation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FieldSample(pub i64);
