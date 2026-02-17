// crates/k8dnz-core/src/repr/text_norm.rs
//
// MVP text normalization (byte-level, corpus-free).
// Goal: stabilize line endings so lane splitting is deterministic across platforms.
//
// Rules (MVP):
// - Convert CRLF and CR to LF.
// - Leave all other bytes unchanged.

pub fn normalize_newlines(input: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(input.len());
    let mut i = 0usize;
    while i < input.len() {
        let b = input[i];
        if b == b'\r' {
            if i + 1 < input.len() && input[i + 1] == b'\n' {
                // CRLF -> LF
                out.push(b'\n');
                i += 2;
                continue;
            } else {
                // CR -> LF
                out.push(b'\n');
                i += 1;
                continue;
            }
        }
        out.push(b);
        i += 1;
    }
    out
}
