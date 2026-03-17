// crates/k8dnz-cli/src/cmd/encode2kb.rs

use clap::Args;

use crate::cmd::omega::parse_omega_spec;
use crate::io::recipe_file;
use k8dnz_core::lane;
use k8dnz_core::symbol::patch::PatchList;
use k8dnz_core::symbol::varint;

#[derive(Args)]
pub struct Encode2kbArgs {
    #[arg(long)]
    pub recipe: String,

    #[arg(long = "in")]
    pub r#in: String,

    #[arg(long)]
    pub out: String,

    #[arg(long, default_value_t = 20_000_000)]
    pub max_ticks: u64,

    #[arg(long, default_value_t = true)]
    pub auto_ticks: bool,

    #[arg(long, default_value_t = 12)]
    pub auto_tries: u32,

    #[arg(long, default_value_t = 2)]
    pub auto_mul: u64,

    #[arg(long, default_value_t = 2_000_000_000)]
    pub auto_max_ticks: u64,

    #[arg(long, default_value_t = 2048)]
    pub budget_bytes: usize,

    #[arg(long, default_value_t = true)]
    pub verify: bool,

    /// Optional Ω schedule/program.
    ///
    /// V1: "letter:skip=251,stride=1;kind:skip=113,stride=1"
    /// V2: "letter:seg2=323:1|900:1"
    #[arg(long)]
    pub omega: Option<String>,
}

const MAGIC_K8L1: &[u8; 4] = b"K8L1";
const K8L1_VERSION_MIN: u8 = 1;
const K8L1_VERSION_MAX: u8 = 3;
const K8L1_VERSION_V2: u8 = 2;

const PATCH_KIND: u64 = 1;
const PATCH_CASE: u64 = 2;
const PATCH_LETTER: u64 = 3;
const PATCH_DIGIT: u64 = 4;
const PATCH_PUNCT: u64 = 5;
const PATCH_RAW: u64 = 6;

#[derive(Clone, Debug, Default)]
struct PatchBreakdown {
    // entry counts
    kind: usize,
    caseb: usize,
    letter: usize,
    digit: usize,
    punct: usize,
    raw: usize,

    // payload byte lengths (just the patch blob bytes for each lane)
    kind_bytes: usize,
    caseb_bytes: usize,
    letter_bytes: usize,
    digit_bytes: usize,
    punct_bytes: usize,
    raw_bytes: usize,

    // mux overhead inside other_patch (varints: n, ids, lens)
    other_mux_overhead_bytes: usize,
}

#[derive(Clone, Debug)]
struct K8L1View {
    ver: u8,
    #[allow(dead_code)]
    total_len: usize,
    #[allow(dead_code)]
    other_len: usize,
    #[allow(dead_code)]
    max_ticks: u64,
    #[allow(dead_code)]
    recipe_bytes: Vec<u8>,
    #[allow(dead_code)]
    omega_len: usize,
    #[allow(dead_code)]
    class_patch: Vec<u8>,
    other_patch: Vec<u8>,
    #[allow(dead_code)]
    trailing_len: usize,

    // These help compute header/varint overhead precisely
    // (bytes from start through the end of other_patch, excluding trailing extensions)
    consumed_len: usize,

    // Track raw lengths for summary
    recipe_len: usize,
    class_patch_len: usize,
    other_patch_len: usize,
}

fn decode_k8l1_view(bytes: &[u8]) -> anyhow::Result<K8L1View> {
    let mut i = 0usize;

    if bytes.len() < 5 {
        anyhow::bail!("k8l1: too short");
    }
    if &bytes[0..4] != MAGIC_K8L1 {
        anyhow::bail!("k8l1: bad magic");
    }
    i += 4;

    let ver = bytes[i];
    i += 1;
    if !(K8L1_VERSION_MIN..=K8L1_VERSION_MAX).contains(&ver) {
        anyhow::bail!("k8l1: unsupported version {}", ver);
    }

    let total_len = varint::get_u64(bytes, &mut i)? as usize;
    let other_len = varint::get_u64(bytes, &mut i)? as usize;
    let max_ticks = varint::get_u64(bytes, &mut i)?;

    let recipe_len = varint::get_u64(bytes, &mut i)? as usize;
    if i + recipe_len > bytes.len() {
        anyhow::bail!("k8l1: recipe oob");
    }
    let recipe_bytes = bytes[i..i + recipe_len].to_vec();
    i += recipe_len;

    let mut omega_len: usize = 0;
    if ver >= K8L1_VERSION_V2 {
        omega_len = varint::get_u64(bytes, &mut i)? as usize;
        if i + omega_len > bytes.len() {
            anyhow::bail!("k8l1: omega oob");
        }
        i += omega_len;
    }

    let class_patch_len = varint::get_u64(bytes, &mut i)? as usize;
    if i + class_patch_len > bytes.len() {
        anyhow::bail!("k8l1: class_patch oob");
    }
    let class_patch = bytes[i..i + class_patch_len].to_vec();
    i += class_patch_len;

    let other_patch_len = varint::get_u64(bytes, &mut i)? as usize;
    if i + other_patch_len > bytes.len() {
        anyhow::bail!("k8l1: other_patch oob");
    }
    let other_patch = bytes[i..i + other_patch_len].to_vec();
    i += other_patch_len;

    let consumed_len = i;
    let trailing_len = bytes.len().saturating_sub(i);

    Ok(K8L1View {
        ver,
        total_len,
        other_len,
        max_ticks,
        recipe_bytes,
        omega_len,
        class_patch,
        other_patch,
        trailing_len,
        consumed_len,
        recipe_len,
        class_patch_len,
        other_patch_len,
    })
}

fn patch_count(patch_bytes: &[u8]) -> anyhow::Result<usize> {
    if patch_bytes.is_empty() {
        return Ok(0);
    }
    let p = PatchList::decode(patch_bytes).map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(p.entries.len())
}

fn demux_other_patches(
    other_bytes: &[u8],
) -> anyhow::Result<(Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, usize)> {
    let mut i = 0usize;

    // n lanes
    let n = varint::get_u64(other_bytes, &mut i)? as usize;

    let mut kind: Option<Vec<u8>> = None;
    let mut caseb: Option<Vec<u8>> = None;
    let mut letter: Option<Vec<u8>> = None;
    let mut digit: Option<Vec<u8>> = None;
    let mut punct: Option<Vec<u8>> = None;
    let mut raw: Option<Vec<u8>> = None;

    // Track payload total to compute mux overhead
    let mut payload_total = 0usize;

    for _ in 0..n {
        let id = varint::get_u64(other_bytes, &mut i)?;
        let len = varint::get_u64(other_bytes, &mut i)? as usize;
        if i + len > other_bytes.len() {
            anyhow::bail!("k8l1: other_patch mux oob (id={}, len={})", id, len);
        }
        let payload = other_bytes[i..i + len].to_vec();
        i += len;

        payload_total = payload_total.saturating_add(payload.len());

        match id {
            PATCH_KIND => kind = Some(payload),
            PATCH_CASE => caseb = Some(payload),
            PATCH_LETTER => letter = Some(payload),
            PATCH_DIGIT => digit = Some(payload),
            PATCH_PUNCT => punct = Some(payload),
            PATCH_RAW => raw = Some(payload),
            _ => {}
        }
    }

    let kind = kind.ok_or_else(|| anyhow::anyhow!("k8l1: other_patch mux missing kind"))?;
    let caseb = caseb.ok_or_else(|| anyhow::anyhow!("k8l1: other_patch mux missing case"))?;
    let letter = letter.ok_or_else(|| anyhow::anyhow!("k8l1: other_patch mux missing letter"))?;
    let digit = digit.ok_or_else(|| anyhow::anyhow!("k8l1: other_patch mux missing digit"))?;
    let punct = punct.ok_or_else(|| anyhow::anyhow!("k8l1: other_patch mux missing punct"))?;
    let raw = raw.ok_or_else(|| anyhow::anyhow!("k8l1: other_patch mux missing raw"))?;

    // Everything in other_bytes that's not payload is mux overhead:
    // [varint n] + N*(varint id + varint len) + (payload bytes)
    // So overhead = total_len - payload_total.
    let mux_overhead = other_bytes.len().saturating_sub(payload_total);

    Ok((kind, caseb, letter, digit, punct, raw, mux_overhead))
}

fn inspect_breakdown(artifact: &[u8]) -> anyhow::Result<(K8L1View, PatchBreakdown)> {
    let v = decode_k8l1_view(artifact)?;
    let (k, c, l, d, p, r, mux_overhead) = demux_other_patches(&v.other_patch)?;
    let bd = PatchBreakdown {
        kind: patch_count(&k)?,
        caseb: patch_count(&c)?,
        letter: patch_count(&l)?,
        digit: patch_count(&d)?,
        punct: patch_count(&p)?,
        raw: patch_count(&r)?,
        kind_bytes: k.len(),
        caseb_bytes: c.len(),
        letter_bytes: l.len(),
        digit_bytes: d.len(),
        punct_bytes: p.len(),
        raw_bytes: r.len(),
        other_mux_overhead_bytes: mux_overhead,
    };
    Ok((v, bd))
}

fn is_insufficient_emissions_err(e: &anyhow::Error) -> bool {
    let s = format!("{e}");
    s.contains("insufficient emissions") && s.contains("max_ticks=")
}

pub fn run(args: Encode2kbArgs) -> anyhow::Result<()> {
    let recipe_bytes = recipe_file::load_k8r_bytes(&args.recipe)?;
    let input = std::fs::read(&args.r#in)?;

    let omega_prog = match &args.omega {
        Some(s) => parse_omega_spec(s)?,
        None => k8dnz_core::lane::OmegaProgram::default(),
    };

    let mut ticks = args.max_ticks;
    let mut tries_done: u32 = 0;

    let (artifact, stats) = loop {
        tries_done += 1;
        let res = lane::encode_k8l1_with_omega_prog(&input, &recipe_bytes, ticks, omega_prog.clone())
            .map_err(|e| anyhow::anyhow!("{e}"));

        match res {
            Ok(ok) => break ok,
            Err(e) => {
                if !args.auto_ticks || !is_insufficient_emissions_err(&e) {
                    return Err(e);
                }
                if tries_done >= args.auto_tries {
                    return Err(anyhow::anyhow!(
                        "encode2kb: exceeded auto_tries={} (last max_ticks={})",
                        args.auto_tries,
                        ticks
                    ));
                }
                if ticks >= args.auto_max_ticks {
                    return Err(anyhow::anyhow!(
                        "encode2kb: hit auto_max_ticks={} without emissions (last max_ticks={})",
                        args.auto_max_ticks,
                        ticks
                    ));
                }

                let next = ticks
                    .saturating_mul(args.auto_mul)
                    .min(args.auto_max_ticks)
                    .max(ticks + 1);
                eprintln!(
                    "note: insufficient emissions at max_ticks={} -> retrying with max_ticks={} (attempt {}/{})",
                    ticks,
                    next,
                    tries_done + 1,
                    args.auto_tries
                );
                ticks = next;
            }
        }
    };

    // Write artifact
    std::fs::write(&args.out, &artifact)?;

    // Inspect breakdown
    let (view, bd) = inspect_breakdown(&artifact)?;

    if view.trailing_len > 0 {
        eprintln!("note: K8L1 has extension tail bytes={}", view.trailing_len);
    }

    if view.ver >= K8L1_VERSION_V2 {
        eprintln!("note: K8L1 v{} omega bytes={}", view.ver, view.omega_len);
    }

    // Compute top-level byte breakdown.
    //
    // total = artifact.len()
    // major blocks:
    //   recipe bytes (payload)
    //   omega bytes (payload)
    //   class_patch bytes (payload)
    //   other_patch bytes (payload; includes mux overhead inside)
    //
    // also compute "header/varint overhead" = bytes consumed in container up to other_patch
    //   minus those payload lengths
    //
    // Note: consumed_len excludes trailing extensions (if any).
    let total_bytes = artifact.len();
    let recipe_payload = view.recipe_len;
    let omega_payload = view.omega_len;
    let class_payload = view.class_patch_len;
    let other_payload = view.other_patch_len;

    let payload_sum = recipe_payload
        .saturating_add(omega_payload)
        .saturating_add(class_payload)
        .saturating_add(other_payload);

    let header_overhead = view
        .consumed_len
        .saturating_sub(payload_sum);

    // Per-lane other patch payload sum
    let other_lane_payload_sum = bd
        .kind_bytes
        .saturating_add(bd.caseb_bytes)
        .saturating_add(bd.letter_bytes)
        .saturating_add(bd.digit_bytes)
        .saturating_add(bd.punct_bytes)
        .saturating_add(bd.raw_bytes);

    // Sanity: other_patch_len = other_lane_payload_sum + other_mux_overhead
    // (if this doesn't hold, we still print what we computed).
    // Print byte accounting line (always).
    println!(
        "BYTES total={} recipe={} omega={} class_patch={} other_patch={} header_overhead={} | other_payload_sum={} mux_overhead={} | lane_bytes kind={} case={} letter={} digit={} punct={} raw={}",
        total_bytes,
        recipe_payload,
        omega_payload,
        class_payload,
        other_payload,
        header_overhead,
        other_lane_payload_sum,
        bd.other_mux_overhead_bytes,
        bd.kind_bytes,
        bd.caseb_bytes,
        bd.letter_bytes,
        bd.digit_bytes,
        bd.punct_bytes,
        bd.raw_bytes
    );

    // If over budget, print mismatch + lane mismatch breakdown and bail (same behavior as before).
    if artifact.len() > args.budget_bytes {
        eprintln!(
            "encode2kb breakdown: class_mismatches={} other_mismatches={} | kind={} case={} letter={} digit={} punct={} raw={}",
            stats.class_mismatches,
            stats.other_mismatches,
            bd.kind,
            bd.caseb,
            bd.letter,
            bd.digit,
            bd.punct,
            bd.raw
        );

        anyhow::bail!(
            "OVER BUDGET: artifact_bytes={} budget_bytes={} (class_mismatches={} other_mismatches={} total_len={} other_len={} emissions_needed={} | kind={} case={} letter={} digit={} punct={} raw={})",
            artifact.len(),
            args.budget_bytes,
            stats.class_mismatches,
            stats.other_mismatches,
            stats.total_len,
            stats.other_len,
            stats.emissions_needed,
            bd.kind,
            bd.caseb,
            bd.letter,
            bd.digit,
            bd.punct,
            bd.raw
        );
    }

    if args.verify {
        let decoded = lane::decode_k8l1(&artifact).map_err(|e| anyhow::anyhow!("{e}"))?;
        let norm = k8dnz_core::repr::text_norm::normalize_newlines(&input);
        if decoded != norm {
            anyhow::bail!(
                "VERIFY FAILED: decoded != normalized input (decoded_len={}, norm_len={})",
                decoded.len(),
                norm.len()
            );
        }
    }

    println!(
        "ok encode2kb: out={} artifact_bytes={} class_mismatches={} other_mismatches={} total_len={} other_len={} emissions_needed={} kind_mismatches={} case_mismatches={} letter_mismatches={} digit_mismatches={} punct_mismatches={} raw_mismatches={}",
        args.out,
        stats.artifact_bytes,
        stats.class_mismatches,
        stats.other_mismatches,
        stats.total_len,
        stats.other_len,
        stats.emissions_needed,
        bd.kind,
        bd.caseb,
        bd.letter,
        bd.digit,
        bd.punct,
        bd.raw
    );

    Ok(())
}
