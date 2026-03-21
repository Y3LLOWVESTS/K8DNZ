// crates/k8dnz-core/src/repr/case_lanes.rs
//
// Deterministic case-lane factorization.
//
// This is a nested lane over ASCII letters only.
// It is meant to sit on top of the normalized text stream.
//
// Symbols:
//   0 = lower
//   1 = upper
//
// Exactness is preserved because we keep the lowercase/base letters in
// `lower_letter_bytes` while `case_lane` carries the case bits.

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CaseLanes {
    pub letter_len: usize,
    pub lower_letter_bytes: Vec<u8>,
    pub case_lane: Vec<u8>,
}

impl CaseLanes {
    pub const CASE_LOWER: u8 = 0;
    pub const CASE_UPPER: u8 = 1;

    pub fn split(bytes: &[u8]) -> Self {
        let mut lower_letter_bytes = Vec::new();
        let mut case_lane = Vec::new();

        for &b in bytes {
            if b.is_ascii_alphabetic() {
                lower_letter_bytes.push(b.to_ascii_lowercase());
                if b.is_ascii_uppercase() {
                    case_lane.push(Self::CASE_UPPER);
                } else {
                    case_lane.push(Self::CASE_LOWER);
                }
            }
        }

        Self {
            letter_len: lower_letter_bytes.len(),
            lower_letter_bytes,
            case_lane,
        }
    }

    pub fn from_letter_bytes(letter_bytes: &[u8]) -> Option<Self> {
        let mut lower_letter_bytes = Vec::with_capacity(letter_bytes.len());
        let mut case_lane = Vec::with_capacity(letter_bytes.len());

        for &b in letter_bytes {
            if !b.is_ascii_alphabetic() {
                return None;
            }
            lower_letter_bytes.push(b.to_ascii_lowercase());
            if b.is_ascii_uppercase() {
                case_lane.push(Self::CASE_UPPER);
            } else {
                case_lane.push(Self::CASE_LOWER);
            }
        }

        Some(Self {
            letter_len: lower_letter_bytes.len(),
            lower_letter_bytes,
            case_lane,
        })
    }

    pub fn reconstruct_letters(&self) -> Option<Vec<u8>> {
        if self.lower_letter_bytes.len() != self.letter_len || self.case_lane.len() != self.letter_len {
            return None;
        }

        let mut out = Vec::with_capacity(self.letter_len);

        for (&b, &case) in self.lower_letter_bytes.iter().zip(self.case_lane.iter()) {
            if !b.is_ascii_lowercase() {
                return None;
            }

            let out_b = match case {
                Self::CASE_LOWER => b,
                Self::CASE_UPPER => b.to_ascii_uppercase(),
                _ => return None,
            };
            out.push(out_b);
        }

        Some(out)
    }
}

#[inline]
pub fn case_label(v: u8) -> &'static str {
    match v {
        CaseLanes::CASE_LOWER => "lower",
        CaseLanes::CASE_UPPER => "upper",
        _ => "invalid",
    }
}