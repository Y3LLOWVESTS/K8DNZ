// crates/k8dnz-core/src/lane/mod.rs
//
// Lane codec (K8L1 container, upgraded internal lane model):
// - normalize newlines
// - class lane: {OTHER, SPACE, NEWLINE} length = total_len
// - for OTHER positions only, we factorize into sublanes:
//     kind_lane: {LETTER, DIGIT, PUNCT, RAW} length = other_count
//     case_lane: {LOWER, UPPER} length = n_letters
//     letter_lane: 0..25 for a..z length = n_letters
//     digit_lane: 0..9 length = n_digits
//     punct_lane: 0..(PUNCT_ALPH.len-1) length = n_punct
//     raw_lane: raw bytes length = n_raw
//
// Prediction:
// - lanes consume a shared emission cursor from Engine (Ω schedule)
// - map pack_byte into lane symbol using BUCKETING (range partition), not modulo
// - store sparse mismatches as PatchList per lane
//
// Container K8L1 stays unchanged:
//   magic: 4 bytes "K8L1"
//   version: u8 (1)
//   total_len: varint
//   other_len: varint
//   max_ticks: varint
//   recipe_len: varint, recipe bytes
//   class_patch_len: varint, class_patch_bytes
//   other_patch_len: varint, other_patch_bytes
//
// IMPORTANT: other_patch_bytes is a mux container holding multiple PatchList blobs:
//   varint n
//   repeated n times: varint id, varint len, len bytes
// ids must match k8dnz-cli demux_other_patches() constants.
//
// Public API contract (matches k8dnz-cli expectations):
//   encode_k8l1(input, recipe_bytes, max_ticks) -> (artifact_bytes, stats)
//   decode_k8l1(bytes) -> decoded bytes

use crate::error::{K8Error, Result};
use crate::recipe::format as recipe_format;
use crate::repr::text_norm;
use crate::symbol::patch::PatchList;
use crate::symbol::varint;
use crate::{Engine, Recipe};

pub const MAGIC_K8L1: [u8; 4] = *b"K8L1";
pub const K8L1_VERSION: u8 = 1;

// -------------------- punctuation alphabet (fixed, corpus-free) --------------------

const PUNCT_ALPH: &[u8] = b".,;:?!'\"()-";

// -------------------- helpers --------------------

#[inline]
fn bucket_u8(b: u8, k: u8) -> u8 {
    debug_assert!(k > 0);
    ((b as u16 * k as u16) >> 8) as u8
}

// -------------------- V2 lane model (internal only) --------------------

#[derive(Clone, Debug, PartialEq, Eq)]
struct TextLanesV2 {
    total_len: usize,
    class_lane: Vec<u8>,   // 0..=2
    kind_lane: Vec<u8>,    // 0..=3, only for OTHER positions
    case_lane: Vec<u8>,    // 0..=1, only for letters
    letter_lane: Vec<u8>,  // 0..=25, only for letters
    digit_lane: Vec<u8>,   // 0..=9, only for digits
    punct_lane: Vec<u8>,   // 0..=PUNCT_ALPH.len-1, only for punct
    raw_lane: Vec<u8>,     // raw bytes, only for kind=RAW
}

impl TextLanesV2 {
    const CLASS_OTHER: u8 = 0;
    const CLASS_SPACE: u8 = 1;
    const CLASS_NL: u8 = 2;

    const KIND_LETTER: u8 = 0;
    const KIND_DIGIT: u8 = 1;
    const KIND_PUNCT: u8 = 2;
    const KIND_RAW: u8 = 3;

    const CASE_LOWER: u8 = 0;
    const CASE_UPPER: u8 = 1;

    fn split(norm: &[u8]) -> Result<Self> {
        let mut class_lane = Vec::with_capacity(norm.len());
        let mut kind_lane = Vec::new();
        let mut case_lane = Vec::new();
        let mut letter_lane = Vec::new();
        let mut digit_lane = Vec::new();
        let mut punct_lane = Vec::new();
        let mut raw_lane = Vec::new();

        for &b in norm {
            match b {
                b' ' => class_lane.push(Self::CLASS_SPACE),
                b'\n' => class_lane.push(Self::CLASS_NL),
                _ => {
                    class_lane.push(Self::CLASS_OTHER);

                    if b.is_ascii_alphabetic() {
                        kind_lane.push(Self::KIND_LETTER);
                        if b.is_ascii_uppercase() {
                            case_lane.push(Self::CASE_UPPER);
                            letter_lane.push((b.to_ascii_lowercase() - b'a') as u8);
                        } else {
                            case_lane.push(Self::CASE_LOWER);
                            letter_lane.push((b - b'a') as u8);
                        }
                    } else if b.is_ascii_digit() {
                        kind_lane.push(Self::KIND_DIGIT);
                        digit_lane.push((b - b'0') as u8);
                    } else if let Some(ix) = PUNCT_ALPH.iter().position(|&p| p == b) {
                        kind_lane.push(Self::KIND_PUNCT);
                        punct_lane.push(ix as u8);
                    } else {
                        kind_lane.push(Self::KIND_RAW);
                        raw_lane.push(b);
                    }
                }
            }
        }

        Ok(Self {
            total_len: norm.len(),
            class_lane,
            kind_lane,
            case_lane,
            letter_lane,
            digit_lane,
            punct_lane,
            raw_lane,
        })
    }

    fn unsplit(self) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(self.total_len);

        let mut k_ix = 0usize;
        let mut l_ix = 0usize;
        let mut d_ix = 0usize;
        let mut p_ix = 0usize;
        let mut r_ix = 0usize;

        for &cl in &self.class_lane {
            match cl {
                Self::CLASS_SPACE => out.push(b' '),
                Self::CLASS_NL => out.push(b'\n'),
                Self::CLASS_OTHER => {
                    if k_ix >= self.kind_lane.len() {
                        return Err(K8Error::Validation("unsplit: kind_lane too short".to_string()));
                    }
                    let k = self.kind_lane[k_ix];
                    k_ix += 1;

                    match k {
                        Self::KIND_LETTER => {
                            if l_ix >= self.letter_lane.len() || l_ix >= self.case_lane.len() {
                                return Err(K8Error::Validation(
                                    "unsplit: letter/case lanes too short".to_string(),
                                ));
                            }
                            let base = self.letter_lane[l_ix];
                            let case = self.case_lane[l_ix];
                            l_ix += 1;

                            let mut b = b'a' + base;
                            if case == Self::CASE_UPPER {
                                b = b.to_ascii_uppercase();
                            }
                            out.push(b);
                        }
                        Self::KIND_DIGIT => {
                            if d_ix >= self.digit_lane.len() {
                                return Err(K8Error::Validation("unsplit: digit_lane too short".to_string()));
                            }
                            let v = self.digit_lane[d_ix];
                            d_ix += 1;
                            out.push(b'0' + v);
                        }
                        Self::KIND_PUNCT => {
                            if p_ix >= self.punct_lane.len() {
                                return Err(K8Error::Validation("unsplit: punct_lane too short".to_string()));
                            }
                            let ix = self.punct_lane[p_ix] as usize;
                            p_ix += 1;
                            let b = *PUNCT_ALPH
                                .get(ix)
                                .ok_or_else(|| K8Error::Validation("unsplit: punct index OOB".to_string()))?;
                            out.push(b);
                        }
                        Self::KIND_RAW => {
                            if r_ix >= self.raw_lane.len() {
                                return Err(K8Error::Validation("unsplit: raw_lane too short".to_string()));
                            }
                            let b = self.raw_lane[r_ix];
                            r_ix += 1;
                            out.push(b);
                        }
                        _ => return Err(K8Error::Validation("unsplit: bad kind".to_string())),
                    }
                }
                _ => return Err(K8Error::Validation("unsplit: bad class".to_string())),
            }
        }

        Ok(out)
    }
}

// -------------------- other_patch mux container (must match k8dnz-cli) --------------------

// ids (must match k8dnz-cli demux_other_patches)
const PATCH_KIND: u64 = 1;
const PATCH_CASE: u64 = 2;
const PATCH_LETTER: u64 = 3;
const PATCH_DIGIT: u64 = 4;
const PATCH_PUNCT: u64 = 5;
const PATCH_RAW: u64 = 6;

fn mux_other_patches(
    kind: &[u8],
    caseb: &[u8],
    letter: &[u8],
    digit: &[u8],
    punct: &[u8],
    raw: &[u8],
) -> Vec<u8> {
    // Always emit all 6 in fixed order (simple + deterministic).
    // CLI ignores unknown ids; missing ids are treated as empty patchlists there.
    let mut out = Vec::new();
    varint::put_u64(6, &mut out);

    fn put(out: &mut Vec<u8>, id: u64, bytes: &[u8]) {
        varint::put_u64(id, out);
        varint::put_u64(bytes.len() as u64, out);
        out.extend_from_slice(bytes);
    }

    put(&mut out, PATCH_KIND, kind);
    put(&mut out, PATCH_CASE, caseb);
    put(&mut out, PATCH_LETTER, letter);
    put(&mut out, PATCH_DIGIT, digit);
    put(&mut out, PATCH_PUNCT, punct);
    put(&mut out, PATCH_RAW, raw);
    out
}

fn demux_other_patches(bytes: &[u8]) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>)> {
    let mut i = 0usize;
    let n = varint::get_u64(bytes, &mut i)? as usize;

    let mut kind = Vec::new();
    let mut caseb = Vec::new();
    let mut letter = Vec::new();
    let mut digit = Vec::new();
    let mut punct = Vec::new();
    let mut raw = Vec::new();

    for _ in 0..n {
        let id = varint::get_u64(bytes, &mut i)?;
        let len = varint::get_u64(bytes, &mut i)? as usize;
        if i + len > bytes.len() {
            return Err(K8Error::Validation("k8l1: other_patch mux len oob".to_string()));
        }
        let chunk = bytes[i..i + len].to_vec();
        i += len;

        match id {
            PATCH_KIND => kind = chunk,
            PATCH_CASE => caseb = chunk,
            PATCH_LETTER => letter = chunk,
            PATCH_DIGIT => digit = chunk,
            PATCH_PUNCT => punct = chunk,
            PATCH_RAW => raw = chunk,
            _ => {
                // ignore unknown ids for forward compatibility
            }
        }
    }

    if i != bytes.len() {
        return Err(K8Error::Validation("k8l1: other_patch mux trailing bytes".to_string()));
    }

    Ok((kind, caseb, letter, digit, punct, raw))
}

// -------------------- predictor stream (Engine emissions) --------------------

// Ω / emission schedule fix:
//
// We do NOT reinitialize Engine per lane.
// Lanes consume a shared emission cursor in deterministic order:
// class → kind → case → letter → digit → punct → raw.
fn gen_pred_stream(eng: &mut Engine, symbols: u64, max_ticks: u64) -> Result<Vec<u8>> {
    let toks = eng.run_emissions(symbols, max_ticks);
    if toks.len() != symbols as usize {
        return Err(K8Error::Validation(format!(
            "engine: insufficient emissions (need {symbols}, got {}) within max_ticks={max_ticks}",
            toks.len()
        )));
    }
    let mut out = Vec::with_capacity(symbols as usize);
    for t in toks {
        out.push(t.pack_byte());
    }
    Ok(out)
}

// -------------------- K8L1 container struct (internal plumbing) --------------------

#[derive(Clone, Debug)]
struct K8L1Artifact {
    total_len: usize,
    other_len: usize,
    max_ticks: u64,
    recipe_bytes: Vec<u8>,
    class_patch_bytes: Vec<u8>,
    other_patch_bytes: Vec<u8>,
}

impl K8L1Artifact {
    fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&MAGIC_K8L1);
        out.push(K8L1_VERSION);

        varint::put_u64(self.total_len as u64, &mut out);
        varint::put_u64(self.other_len as u64, &mut out);
        varint::put_u64(self.max_ticks, &mut out);

        varint::put_u64(self.recipe_bytes.len() as u64, &mut out);
        out.extend_from_slice(&self.recipe_bytes);

        varint::put_u64(self.class_patch_bytes.len() as u64, &mut out);
        out.extend_from_slice(&self.class_patch_bytes);

        varint::put_u64(self.other_patch_bytes.len() as u64, &mut out);
        out.extend_from_slice(&self.other_patch_bytes);

        out
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 5 {
            return Err(K8Error::Validation("K8L1 too short".to_string()));
        }
        if &bytes[..4] != &MAGIC_K8L1 {
            return Err(K8Error::Validation("K8L1 bad magic".to_string()));
        }
        let ver = bytes[4];
        if ver != K8L1_VERSION {
            return Err(K8Error::Validation(format!("K8L1 bad version {ver}")));
        }

        let mut i = 5usize;

        let total_len = varint::get_u64(bytes, &mut i)? as usize;
        let other_len = varint::get_u64(bytes, &mut i)? as usize;
        let max_ticks = varint::get_u64(bytes, &mut i)?;

        let rlen = varint::get_u64(bytes, &mut i)? as usize;
        if bytes.len() < i + rlen {
            return Err(K8Error::Validation("K8L1 recipe OOB".to_string()));
        }
        let recipe_bytes = bytes[i..i + rlen].to_vec();
        i += rlen;

        let clen = varint::get_u64(bytes, &mut i)? as usize;
        if bytes.len() < i + clen {
            return Err(K8Error::Validation("K8L1 class patch OOB".to_string()));
        }
        let class_patch_bytes = bytes[i..i + clen].to_vec();
        i += clen;

        let olen = varint::get_u64(bytes, &mut i)? as usize;
        if bytes.len() < i + olen {
            return Err(K8Error::Validation("K8L1 other patch OOB".to_string()));
        }
        let other_patch_bytes = bytes[i..i + olen].to_vec();
        i += olen;

        if i != bytes.len() {
            return Err(K8Error::Validation("K8L1 trailing bytes".to_string()));
        }

        Ok(Self {
            total_len,
            other_len,
            max_ticks,
            recipe_bytes,
            class_patch_bytes,
            other_patch_bytes,
        })
    }
}

// -------------------- encode / decode (public API) --------------------

#[derive(Clone, Debug, Default)]
pub struct LaneEncodeStats {
    pub total_len: usize,
    pub other_len: usize,
    pub n_letters: usize,
    pub n_digits: usize,
    pub n_punct: usize,
    pub n_raw: usize,
    pub emissions_needed: usize,
    pub class_mismatches: usize,
    pub other_mismatches: usize,
    pub kind_mismatches: usize,
    pub case_mismatches: usize,
    pub letter_mismatches: usize,
    pub digit_mismatches: usize,
    pub punct_mismatches: usize,
    pub raw_mismatches: usize,
    pub artifact_bytes: usize,
}

pub fn encode_k8l1(input: &[u8], recipe_bytes: &[u8], max_ticks: u64) -> Result<(Vec<u8>, LaneEncodeStats)> {
    let norm = text_norm::normalize_newlines(input);
    let lanes = TextLanesV2::split(&norm)?;

    let total_len_u = lanes.total_len as u64;
    let other_len_u = lanes.kind_lane.len() as u64;
    let n_letters_u = lanes.letter_lane.len() as u64;
    let n_digits_u = lanes.digit_lane.len() as u64;
    let n_punct_u = lanes.punct_lane.len() as u64;
    let n_raw_u = lanes.raw_lane.len() as u64;

    let recipe = recipe_from_bytes(recipe_bytes)?;
    let mut eng = Engine::new(recipe.clone())?;

    // class
    let pred_class_raw = gen_pred_stream(&mut eng, total_len_u, max_ticks)?;
    let pred_class: Vec<u8> = pred_class_raw.iter().map(|&b| bucket_u8(b, 3)).collect();
    let class_patch = PatchList::from_pred_actual(&pred_class, &lanes.class_lane)?;
    let class_patch_bytes = class_patch.encode();

    // kind
    let pred_kind_raw = gen_pred_stream(&mut eng, other_len_u, max_ticks)?;
    let pred_kind: Vec<u8> = pred_kind_raw.iter().map(|&b| bucket_u8(b, 4)).collect();
    let kind_patch = PatchList::from_pred_actual(&pred_kind, &lanes.kind_lane)?;
    let kind_bytes = kind_patch.encode();

    // case
    let pred_case_raw = gen_pred_stream(&mut eng, n_letters_u, max_ticks)?;
    let pred_case: Vec<u8> = pred_case_raw.iter().map(|&b| bucket_u8(b, 2)).collect();
    let case_patch = PatchList::from_pred_actual(&pred_case, &lanes.case_lane)?;
    let case_bytes = case_patch.encode();

    // letter
    let pred_letter_raw = gen_pred_stream(&mut eng, n_letters_u, max_ticks)?;
    let pred_letter: Vec<u8> = pred_letter_raw.iter().map(|&b| bucket_u8(b, 26)).collect();
    let letter_patch = PatchList::from_pred_actual(&pred_letter, &lanes.letter_lane)?;
    let letter_bytes = letter_patch.encode();

    // digit
    let pred_digit_raw = gen_pred_stream(&mut eng, n_digits_u, max_ticks)?;
    let pred_digit: Vec<u8> = pred_digit_raw.iter().map(|&b| bucket_u8(b, 10)).collect();
    let digit_patch = PatchList::from_pred_actual(&pred_digit, &lanes.digit_lane)?;
    let digit_bytes = digit_patch.encode();

    // punct
    let pred_punct_raw = gen_pred_stream(&mut eng, n_punct_u, max_ticks)?;
    let pred_punct: Vec<u8> = pred_punct_raw
        .iter()
        .map(|&b| bucket_u8(b, PUNCT_ALPH.len() as u8))
        .collect();
    let punct_patch = PatchList::from_pred_actual(&pred_punct, &lanes.punct_lane)?;
    let punct_bytes = punct_patch.encode();

    // raw
    let pred_raw = gen_pred_stream(&mut eng, n_raw_u, max_ticks)?;
    let raw_patch = PatchList::from_pred_actual(&pred_raw, &lanes.raw_lane)?;
    let raw_bytes = raw_patch.encode();

    // MUST be mux format (matches CLI demux_other_patches)
    let other_patch_bytes = mux_other_patches(&kind_bytes, &case_bytes, &letter_bytes, &digit_bytes, &punct_bytes, &raw_bytes);

    let recipe_bytes_owned = recipe_to_bytes(&recipe)?;

    let art = K8L1Artifact {
        total_len: lanes.total_len,
        other_len: lanes.kind_lane.len(),
        max_ticks,
        recipe_bytes: recipe_bytes_owned,
        class_patch_bytes,
        other_patch_bytes,
    };

    let artifact_bytes = art.to_bytes();
    let artifact_len = artifact_bytes.len();

    let class_mismatches = class_patch.entries.len();
    let kind_mismatches = kind_patch.entries.len();
    let case_mismatches = case_patch.entries.len();
    let letter_mismatches = letter_patch.entries.len();
    let digit_mismatches = digit_patch.entries.len();
    let punct_mismatches = punct_patch.entries.len();
    let raw_mismatches = raw_patch.entries.len();

    let other_mismatches = kind_mismatches
        + case_mismatches
        + letter_mismatches
        + digit_mismatches
        + punct_mismatches
        + raw_mismatches;

    let emissions_needed =
        (total_len_u + other_len_u + n_letters_u + n_letters_u + n_digits_u + n_punct_u + n_raw_u) as usize;

    let stats = LaneEncodeStats {
        total_len: lanes.total_len,
        other_len: lanes.kind_lane.len(),
        n_letters: lanes.letter_lane.len(),
        n_digits: lanes.digit_lane.len(),
        n_punct: lanes.punct_lane.len(),
        n_raw: lanes.raw_lane.len(),
        emissions_needed,
        class_mismatches,
        other_mismatches,
        kind_mismatches,
        case_mismatches,
        letter_mismatches,
        digit_mismatches,
        punct_mismatches,
        raw_mismatches,
        artifact_bytes: artifact_len,
    };

    Ok((artifact_bytes, stats))
}

pub fn decode_k8l1(bytes: &[u8]) -> Result<Vec<u8>> {
    let art = K8L1Artifact::from_bytes(bytes)?;
    let recipe = recipe_from_bytes(&art.recipe_bytes)?;
    let mut eng = Engine::new(recipe.clone())?;

    let total_len_u = art.total_len as u64;
    let other_len_u = art.other_len as u64;

    // class
    let pred_class_raw = gen_pred_stream(&mut eng, total_len_u, art.max_ticks)?;
    let mut pred_class: Vec<u8> = pred_class_raw.iter().map(|&b| bucket_u8(b, 3)).collect();
    let class_patch = PatchList::decode(&art.class_patch_bytes)?;
    class_patch.apply_to_pred(&mut pred_class)?;

    // other_patch mux -> patch blobs
    let (kind_b, case_b, letter_b, digit_b, punct_b, raw_b) = demux_other_patches(&art.other_patch_bytes)?;

    // kind (needed to derive downstream lane lengths)
    let pred_kind_raw = gen_pred_stream(&mut eng, other_len_u, art.max_ticks)?;
    let mut pred_kind: Vec<u8> = pred_kind_raw.iter().map(|&b| bucket_u8(b, 4)).collect();
    let kind_patch = if kind_b.is_empty() { PatchList::new() } else { PatchList::decode(&kind_b)? };
    kind_patch.apply_to_pred(&mut pred_kind)?;

    // Determine lane counts from patched kind lane
    let mut n_letters = 0usize;
    let mut n_digits = 0usize;
    let mut n_punct = 0usize;
    let mut n_raw = 0usize;

    for &k in &pred_kind {
        match k {
            TextLanesV2::KIND_LETTER => n_letters += 1,
            TextLanesV2::KIND_DIGIT => n_digits += 1,
            TextLanesV2::KIND_PUNCT => n_punct += 1,
            TextLanesV2::KIND_RAW => n_raw += 1,
            _ => return Err(K8Error::Validation("decode: bad kind".to_string())),
        }
    }

    // case
    let pred_case_raw = gen_pred_stream(&mut eng, n_letters as u64, art.max_ticks)?;
    let mut pred_case: Vec<u8> = pred_case_raw.iter().map(|&b| bucket_u8(b, 2)).collect();
    let case_patch = if case_b.is_empty() { PatchList::new() } else { PatchList::decode(&case_b)? };
    case_patch.apply_to_pred(&mut pred_case)?;

    // letter
    let pred_letter_raw = gen_pred_stream(&mut eng, n_letters as u64, art.max_ticks)?;
    let mut pred_letter: Vec<u8> = pred_letter_raw.iter().map(|&b| bucket_u8(b, 26)).collect();
    let letter_patch = if letter_b.is_empty() { PatchList::new() } else { PatchList::decode(&letter_b)? };
    letter_patch.apply_to_pred(&mut pred_letter)?;

    // digit
    let pred_digit_raw = gen_pred_stream(&mut eng, n_digits as u64, art.max_ticks)?;
    let mut pred_digit: Vec<u8> = pred_digit_raw.iter().map(|&b| bucket_u8(b, 10)).collect();
    let digit_patch = if digit_b.is_empty() { PatchList::new() } else { PatchList::decode(&digit_b)? };
    digit_patch.apply_to_pred(&mut pred_digit)?;

    // punct
    let pred_punct_raw = gen_pred_stream(&mut eng, n_punct as u64, art.max_ticks)?;
    let mut pred_punct: Vec<u8> = pred_punct_raw
        .iter()
        .map(|&b| bucket_u8(b, PUNCT_ALPH.len() as u8))
        .collect();
    let punct_patch = if punct_b.is_empty() { PatchList::new() } else { PatchList::decode(&punct_b)? };
    punct_patch.apply_to_pred(&mut pred_punct)?;

    // raw
    let mut pred_raw = gen_pred_stream(&mut eng, n_raw as u64, art.max_ticks)?;
    let raw_patch = if raw_b.is_empty() { PatchList::new() } else { PatchList::decode(&raw_b)? };
    raw_patch.apply_to_pred(&mut pred_raw)?;

    let lanes = TextLanesV2 {
        total_len: art.total_len,
        class_lane: pred_class,
        kind_lane: pred_kind,
        case_lane: pred_case,
        letter_lane: pred_letter,
        digit_lane: pred_digit,
        punct_lane: pred_punct,
        raw_lane: pred_raw,
    };

    Ok(lanes.unsplit()?)
}

// -------------------- recipe format helpers --------------------

fn recipe_from_bytes(b: &[u8]) -> Result<Recipe> {
    recipe_format::decode(b)
}

fn recipe_to_bytes(r: &Recipe) -> Result<Vec<u8>> {
    Ok(recipe_format::encode(r))
}
