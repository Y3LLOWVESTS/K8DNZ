// crates/k8dnz-core/src/repr/ws_lanes.rs
//
// MVP lane factorization: Whitespace-vs-Other.
//
// We encode the original byte stream as:
// - class_lane: length = total_len, symbols in {0,1,2}
//     0 = OTHER
//     1 = SPACE (0x20)
//     2 = NEWLINE (0x0A)
// - other_lane: bytes for all OTHER positions, in order.
//
// Reconstruction is exact:
// iterate class_lane; if SPACE/NEWLINE emit literal; else pop next from other_lane.

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WsLanes {
    pub total_len: usize,
    pub class_lane: Vec<u8>, // values 0..=2
    pub other_lane: Vec<u8>, // raw bytes for OTHER positions
}

impl WsLanes {
    pub const CLASS_OTHER: u8 = 0;
    pub const CLASS_SPACE: u8 = 1;
    pub const CLASS_NL: u8 = 2;

    pub fn split(bytes: &[u8]) -> Self {
        let mut class_lane = Vec::with_capacity(bytes.len());
        let mut other_lane = Vec::with_capacity(bytes.len());

        for &b in bytes {
            match b {
                b' ' => class_lane.push(Self::CLASS_SPACE),
                b'\n' => class_lane.push(Self::CLASS_NL),
                _ => {
                    class_lane.push(Self::CLASS_OTHER);
                    other_lane.push(b);
                }
            }
        }

        Self {
            total_len: bytes.len(),
            class_lane,
            other_lane,
        }
    }

    pub fn reconstruct(&self) -> Option<Vec<u8>> {
        if self.class_lane.len() != self.total_len {
            return None;
        }

        let mut out = Vec::with_capacity(self.total_len);
        let mut j = 0usize;

        for &c in &self.class_lane {
            match c {
                Self::CLASS_SPACE => out.push(b' '),
                Self::CLASS_NL => out.push(b'\n'),
                Self::CLASS_OTHER => {
                    if j >= self.other_lane.len() {
                        return None;
                    }
                    out.push(self.other_lane[j]);
                    j += 1;
                }
                _ => return None,
            }
        }

        if j != self.other_lane.len() {
            return None;
        }

        Some(out)
    }
}
