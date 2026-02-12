#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Turn32(pub u32);

impl Turn32 {
    pub const ZERO: Turn32 = Turn32(0);
    pub const HALF: Turn32 = Turn32(0x8000_0000);

    /// Build a Turn32 from a rational fraction of a turn: num/den turns.
    /// Example: from_frac(1, 2) == HALF.
    #[inline]
    pub fn from_frac(num: u64, den: u64) -> Turn32 {
        assert!(den != 0, "denominator must be non-zero");
        // 1 turn == 2^32
        let v = ((num << 32) / den) as u32;
        Turn32(v)
    }

    #[inline]
    pub fn wrapping_add(self, other: Turn32) -> Turn32 {
        Turn32(self.0.wrapping_add(other.0))
    }

    #[inline]
    pub fn wrapping_sub(self, other: Turn32) -> Turn32 {
        Turn32(self.0.wrapping_sub(other.0))
    }
}
