// crates/k8dnz-cli/src/cmd/encode2kb.rs

use clap::Args;

use crate::io::recipe_file;
use k8dnz_core::lane;
use k8dnz_core::symbol::patch::PatchList;
use k8dnz_core::symbol::varint;

#[derive(Args)]
pub struct Encode2kbArgs {
    /// Recipe path (.k8r)
    #[arg(long)]
    pub recipe: String,

    /// Input file path
    #[arg(long = "in")]
    pub r#in: String,

    /// Output artifact path (K8L1)
    #[arg(long)]
    pub out: String,

    /// Max ticks guard for cadence emissions
    #[arg(long, default_value_t = 20_000_000)]
    pub max_ticks: u64,

    /// Hard budget gate (bytes). If exceeded, returns error.
    #[arg(long, default_value_t = 2048)]
    pub budget_bytes: usize,

    /// Verify by decoding immediately (recommended)
    #[arg(long, default_value_t = true)]
    pub verify: bool,
}

// -------------------- internal K8L1 inspection helpers --------------------

const MAGIC_K8L1: &[u8; 4] = b"K8L1";
const K8L1_VERSION: u8 = 1;

// ids (must match k8dnz-core lane/mod.rs mux ids)
const PATCH_KIND: u64 = 1;
const PATCH_CASE: u64 = 2;
const PATCH_LETTER: u64 = 3;
const PATCH_DIGIT: u64 = 4;
const PATCH_PUNCT: u64 = 5;
const PATCH_RAW: u64 = 6;

#[derive(Clone, Debug, Default)]
struct PatchBreakdown {
    kind: usize,
    caseb: usize,
    letter: usize,
    digit: usize,
    punct: usize,
    raw: usize,
}

#[derive(Clone, Debug)]
struct K8L1View {
    total_len: usize,
    other_len: usize,
    max_ticks: u64,
    recipe_bytes: Vec<u8>,
    class_patch: Vec<u8>,
    other_patch: Vec<u8>,
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
    if ver != K8L1_VERSION {
        anyhow::bail!("k8l1: unsupported version {}", ver);
    }

    let total_len = varint::get_u64(bytes, &mut i)? as usize;
    let other_len = varint::get_u64(bytes, &mut i)? as usize;
    let max_ticks = varint::get_u64(bytes, &mut i)?;

    let recipe_len = varint::get_u64(bytes, &mut i)? as usize;
    if i + recipe_len > bytes.len() {
        anyhow::bail!("k8l1: recipe len oob");
    }
    let recipe_bytes = bytes[i..i + recipe_len].to_vec();
    i += recipe_len;

    let class_patch_len = varint::get_u64(bytes, &mut i)? as usize;
    if i + class_patch_len > bytes.len() {
        anyhow::bail!("k8l1: class patch len oob");
    }
    let class_patch = bytes[i..i + class_patch_len].to_vec();
    i += class_patch_len;

    let other_patch_len = varint::get_u64(bytes, &mut i)? as usize;
    if i + other_patch_len > bytes.len() {
        anyhow::bail!("k8l1: other patch len oob");
    }
    let other_patch = bytes[i..i + other_patch_len].to_vec();
    i += other_patch_len;

    if i != bytes.len() {
        anyhow::bail!("k8l1: trailing bytes");
    }

    Ok(K8L1View {
        total_len,
        other_len,
        max_ticks,
        recipe_bytes,
        class_patch,
        other_patch,
    })
}

fn demux_other_patches(bytes: &[u8]) -> anyhow::Result<(Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>)> {
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
            anyhow::bail!("k8l1: other_patch mux len oob");
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
        anyhow::bail!("k8l1: other_patch mux trailing bytes");
    }

    Ok((kind, caseb, letter, digit, punct, raw))
}

fn patch_count(bytes: &[u8]) -> anyhow::Result<usize> {
    if bytes.is_empty() {
        // if mux is missing an id, treat as empty patchlist
        return Ok(0);
    }
    let pl = PatchList::decode(bytes).map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(pl.entries.len())
}

fn inspect_breakdown(artifact: &[u8]) -> anyhow::Result<(K8L1View, PatchBreakdown)> {
    let v = decode_k8l1_view(artifact)?;
    let (k, c, l, d, p, r) = demux_other_patches(&v.other_patch)?;
    let bd = PatchBreakdown {
        kind: patch_count(&k)?,
        caseb: patch_count(&c)?,
        letter: patch_count(&l)?,
        digit: patch_count(&d)?,
        punct: patch_count(&p)?,
        raw: patch_count(&r)?,
    };
    Ok((v, bd))
}

// -------------------- main command --------------------

pub fn run(args: Encode2kbArgs) -> anyhow::Result<()> {
    let recipe_bytes = recipe_file::load_k8r_bytes(&args.recipe)?;
    let input = std::fs::read(&args.r#in)?;

    let (artifact, stats) = lane::encode_k8l1(&input, &recipe_bytes, args.max_ticks)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    // Always compute breakdown (helps debugging even on over-budget)
    let (_view, bd) = inspect_breakdown(&artifact)?;

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

    std::fs::write(&args.out, &artifact)?;

    if args.verify {
        let decoded = lane::decode_k8l1(&artifact).map_err(|e| anyhow::anyhow!("{e}"))?;
        // Normalize input the same way encode does (newline normalization),
        // because K8L1 encodes the normalized stream.
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
