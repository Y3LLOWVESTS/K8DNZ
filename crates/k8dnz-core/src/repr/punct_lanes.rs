// crates/k8dnz-core/src/repr/punct_lanes.rs
//
// Deterministic punctuation lane factorization.
//
// We encode the normalized byte stream as:
// - class_lane: length = total_len, symbols in {0,1,2,3}
//     0 = OTHER (not ASCII punctuation)
//     1 = TERM  (. ! ?)
//     2 = PAUSE (, ; :)
//     3 = WRAP  (all other ASCII punctuation)
// - other_lane: raw bytes for all OTHER positions, in order
// - punct_lane: raw punctuation bytes for all punctuation positions, in order
//
// Reconstruction is exact because punctuation bytes are preserved verbatim in
// punct_lane, while class_lane gives us the alignment surface to predict.

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PunctLanes {
    pub total_len: usize,
    pub class_lane: Vec<u8>,
    pub other_lane: Vec<u8>,
    pub punct_lane: Vec<u8>,
}

impl PunctLanes {
    pub const CLASS_OTHER: u8 = 0;
    pub const CLASS_TERM: u8 = 1;
    pub const CLASS_PAUSE: u8 = 2;
    pub const CLASS_WRAP: u8 = 3;

    pub fn split(bytes: &[u8]) -> Self {
        let mut class_lane = Vec::with_capacity(bytes.len());
        let mut other_lane = Vec::with_capacity(bytes.len());
        let mut punct_lane = Vec::with_capacity(bytes.len());

        for &b in bytes {
            if let Some(class) = classify_punct_byte(b) {
                class_lane.push(class);
                punct_lane.push(b);
            } else {
                class_lane.push(Self::CLASS_OTHER);
                other_lane.push(b);
            }
        }

        Self {
            total_len: bytes.len(),
            class_lane,
            other_lane,
            punct_lane,
        }
    }

    pub fn reconstruct(&self) -> Option<Vec<u8>> {
        if self.class_lane.len() != self.total_len {
            return None;
        }

        let mut out = Vec::with_capacity(self.total_len);
        let mut other_i = 0usize;
        let mut punct_i = 0usize;

        for &class in &self.class_lane {
            match class {
                Self::CLASS_OTHER => {
                    if other_i >= self.other_lane.len() {
                        return None;
                    }
                    out.push(self.other_lane[other_i]);
                    other_i += 1;
                }
                Self::CLASS_TERM | Self::CLASS_PAUSE | Self::CLASS_WRAP => {
                    if punct_i >= self.punct_lane.len() {
                        return None;
                    }
                    out.push(self.punct_lane[punct_i]);
                    punct_i += 1;
                }
                _ => return None,
            }
        }

        if other_i != self.other_lane.len() || punct_i != self.punct_lane.len() {
            return None;
        }

        Some(out)
    }
}

#[inline]
pub fn classify_punct_byte(b: u8) -> Option<u8> {
    match b {
        b'.' | b'!' | b'?' => Some(PunctLanes::CLASS_TERM),
        b',' | b';' | b':' => Some(PunctLanes::CLASS_PAUSE),
        _ if b.is_ascii_punctuation() => Some(PunctLanes::CLASS_WRAP),
        _ => None,
    }
}

#[inline]
pub fn punct_class_label(v: u8) -> &'static str {
    match v {
        PunctLanes::CLASS_OTHER => "other",
        PunctLanes::CLASS_TERM => "term",
        PunctLanes::CLASS_PAUSE => "pause",
        PunctLanes::CLASS_WRAP => "wrap",
        _ => "invalid",
    }
}