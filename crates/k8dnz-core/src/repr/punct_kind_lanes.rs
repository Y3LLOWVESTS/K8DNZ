// crates/k8dnz-core/src/repr/punct_kind_lanes.rs
//
// Deterministic punctuation-subtype lane factorization.
//
// This is a nested lane that operates on punctuation-only positions.
// It is meant to sit on top of repr::punct_lanes::PunctLanes::punct_lane.
//
// Symbols:
//   0 = TERM  (. ! ?)
//   1 = PAUSE (, ; :)
//   2 = WRAP  (all other ASCII punctuation)
//
// Exactness is preserved because we keep the verbatim punctuation bytes in
// punct_bytes while kind_lane provides the small-alphabet subtype surface.

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PunctKindLanes {
    pub punct_len: usize,
    pub punct_bytes: Vec<u8>,
    pub kind_lane: Vec<u8>,
}

impl PunctKindLanes {
    pub const KIND_TERM: u8 = 0;
    pub const KIND_PAUSE: u8 = 1;
    pub const KIND_WRAP: u8 = 2;

    pub fn split(bytes: &[u8]) -> Self {
        let mut punct_bytes = Vec::new();
        let mut kind_lane = Vec::new();

        for &b in bytes {
            if let Some(kind) = classify_punct_kind_byte(b) {
                punct_bytes.push(b);
                kind_lane.push(kind);
            }
        }

        Self {
            punct_len: punct_bytes.len(),
            punct_bytes,
            kind_lane,
        }
    }

    pub fn from_punct_bytes(punct_bytes: &[u8]) -> Option<Self> {
        let mut kind_lane = Vec::with_capacity(punct_bytes.len());
        for &b in punct_bytes {
            kind_lane.push(classify_punct_kind_byte(b)?);
        }

        Some(Self {
            punct_len: punct_bytes.len(),
            punct_bytes: punct_bytes.to_vec(),
            kind_lane,
        })
    }

    pub fn reconstruct_punct_bytes(&self) -> Option<Vec<u8>> {
        if self.punct_bytes.len() != self.punct_len || self.kind_lane.len() != self.punct_len {
            return None;
        }

        for (&b, &kind) in self.punct_bytes.iter().zip(self.kind_lane.iter()) {
            if classify_punct_kind_byte(b) != Some(kind) {
                return None;
            }
        }

        Some(self.punct_bytes.clone())
    }
}

#[inline]
pub fn classify_punct_kind_byte(b: u8) -> Option<u8> {
    match b {
        b'.' | b'!' | b'?' => Some(PunctKindLanes::KIND_TERM),
        b',' | b';' | b':' => Some(PunctKindLanes::KIND_PAUSE),
        _ if b.is_ascii_punctuation() => Some(PunctKindLanes::KIND_WRAP),
        _ => None,
    }
}

#[inline]
pub fn punct_kind_label(v: u8) -> &'static str {
    match v {
        PunctKindLanes::KIND_TERM => "term",
        PunctKindLanes::KIND_PAUSE => "pause",
        PunctKindLanes::KIND_WRAP => "wrap",
        _ => "invalid",
    }
}