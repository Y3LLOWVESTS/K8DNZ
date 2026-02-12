#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Unit32(pub u32);

impl Unit32 {
    pub const MIN: Unit32 = Unit32(0);
    pub const MAX: Unit32 = Unit32(u32::MAX);

    /// Build a Unit32 from a rational fraction in [0,1]: num/den.
    /// Unit32 is a saturating [0..=u32::MAX] representation of [0..=1].
    #[inline]
    pub fn from_frac(num: u64, den: u64) -> Unit32 {
        assert!(den != 0, "denominator must be non-zero");
        // scale into [0..=u32::MAX]
        let v = ((num * (u32::MAX as u64)) / den) as u32;
        Unit32(v)
    }

    #[inline]
    pub fn saturating_add(self, delta: u32) -> Unit32 {
        Unit32(self.0.saturating_add(delta))
    }

    #[inline]
    pub fn is_max(self) -> bool {
        self.0 == u32::MAX
    }
}
