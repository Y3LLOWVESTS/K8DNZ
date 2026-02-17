// crates/k8dnz-cli/src/cmd/lane_sweep.rs

use clap::Args;

use crate::io::recipe_file;
use k8dnz_core::lane;
use k8dnz_core::symbol::patch::PatchList;
use k8dnz_core::symbol::varint;

// ids (must match k8dnz-core lane/mod.rs mux ids)
const PATCH_KIND: u64 = 1;
const PATCH_CASE: u64 = 2;
const PATCH_LETTER: u64 = 3;
const PATCH_DIGIT: u64 = 4;
const PATCH_PUNCT: u64 = 5;
const PATCH_RAW: u64 = 6;

// K8L1 container constants
const MAGIC_K8L1: &[u8; 4] = b"K8L1";
const K8L1_VERSION: u8 = 1;

#[derive(Args)]
pub struct LaneSweepArgs {
    /// Recipe path (.k8r)
    #[arg(long)]
    pub recipe: String,

    /// Input file path
    #[arg(long = "in")]
    pub r#in: String,

    /// Comma-separated sizes in bytes (e.g. 256,512,1024,2048,4096)
    #[arg(long, default_value = "256,512,1024,2048,4096")]
    pub sizes: String,

    /// Max ticks guard for cadence emissions
    #[arg(long, default_value_t = 20_000_000)]
    pub max_ticks: u64,
}

fn parse_sizes(s: &str) -> anyhow::Result<Vec<usize>> {
    let mut out = Vec::new();
    for part in s.split(',') {
        let t = part.trim();
        if t.is_empty() {
            continue;
        }
        out.push(t.parse::<usize>()?);
    }
    if out.is_empty() {
        anyhow::bail!("sizes list is empty");
    }
    Ok(out)
}

#[derive(Clone, Debug)]
struct K8L1View {
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

    // total_len, other_len, max_ticks
    let _total_len = varint::get_u64(bytes, &mut i)?;
    let _other_len = varint::get_u64(bytes, &mut i)?;
    let _max_ticks = varint::get_u64(bytes, &mut i)?;

    // recipe
    let recipe_len = varint::get_u64(bytes, &mut i)? as usize;
    if i + recipe_len > bytes.len() {
        anyhow::bail!("k8l1: recipe len oob");
    }
    i += recipe_len;

    // class_patch
    let class_patch_len = varint::get_u64(bytes, &mut i)? as usize;
    if i + class_patch_len > bytes.len() {
        anyhow::bail!("k8l1: class patch len oob");
    }
    let class_patch = bytes[i..i + class_patch_len].to_vec();
    i += class_patch_len;

    // other_patch
    let other_patch_len = varint::get_u64(bytes, &mut i)? as usize;
    if i + other_patch_len > bytes.len() {
        anyhow::bail!("k8l1: other patch len oob");
    }
    let other_patch = bytes[i..i + other_patch_len].to_vec();
    i += other_patch_len;

    if i != bytes.len() {
        anyhow::bail!("k8l1: trailing bytes");
    }

    Ok(K8L1View { class_patch, other_patch })
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
            _ => {}
        }
    }

    if i != bytes.len() {
        anyhow::bail!("k8l1: other_patch mux trailing bytes");
    }

    Ok((kind, caseb, letter, digit, punct, raw))
}

fn patch_count(bytes: &[u8]) -> anyhow::Result<usize> {
    if bytes.is_empty() {
        return Ok(0);
    }
    let pl = PatchList::decode(bytes).map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(pl.entries.len())
}

pub fn run(args: LaneSweepArgs) -> anyhow::Result<()> {
    let recipe_bytes = recipe_file::load_k8r_bytes(&args.recipe)?;
    let input = std::fs::read(&args.r#in)?;
    let sizes = parse_sizes(&args.sizes)?;

    println!("size,artifact_bytes,class_mismatches,other_mismatches,kind_mismatches,case_mismatches,letter_mismatches,digit_mismatches,punct_mismatches,raw_mismatches,total_len,other_len,emissions_needed");

    for sz in sizes {
        let take = sz.min(input.len());
        let slice = &input[..take];

        let (artifact, stats) = lane::encode_k8l1(slice, &recipe_bytes, args.max_ticks)
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        let v = decode_k8l1_view(&artifact)?;
        let class_m = patch_count(&v.class_patch)?;
        let (k, c, l, d, p, r) = demux_other_patches(&v.other_patch)?;

        let kind_m = patch_count(&k)?;
        let case_m = patch_count(&c)?;
        let letter_m = patch_count(&l)?;
        let digit_m = patch_count(&d)?;
        let punct_m = patch_count(&p)?;
        let raw_m = patch_count(&r)?;

        println!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{}",
            take,
            artifact.len(),
            class_m,
            stats.other_mismatches,
            kind_m,
            case_m,
            letter_m,
            digit_m,
            punct_m,
            raw_m,
            stats.total_len,
            stats.other_len,
            stats.emissions_needed
        );
    }

    Ok(())
}
