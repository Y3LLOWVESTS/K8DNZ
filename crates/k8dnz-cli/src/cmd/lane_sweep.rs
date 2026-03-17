// crates/k8dnz-cli/src/cmd/lane_sweep.rs

use clap::Args;

use crate::io::recipe_file;
use k8dnz_core::lane;
use k8dnz_core::symbol::patch::PatchList;
use k8dnz_core::symbol::varint;

const PATCH_KIND: u64 = 1;
const PATCH_CASE: u64 = 2;
const PATCH_LETTER: u64 = 3;
const PATCH_DIGIT: u64 = 4;
const PATCH_PUNCT: u64 = 5;
const PATCH_RAW: u64 = 6;

const MAGIC_K8L1: &[u8; 4] = b"K8L1";
const K8L1_VERSION_MIN: u8 = 1;
const K8L1_VERSION_MAX: u8 = 2;
const K8L1_VERSION_V2: u8 = 2;

#[derive(Args)]
pub struct LaneSweepArgs {
    #[arg(long)]
    pub recipe: String,

    #[arg(long = "in")]
    pub r#in: String,

    #[arg(long, default_value = "256,512,1024,2048,4096")]
    pub sizes: String,

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
    #[allow(dead_code)]
    omega_len: usize,
    #[allow(dead_code)]
    trailing_len: usize,
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

    let _total_len = varint::get_u64(bytes, &mut i)? as usize;
    let _other_len = varint::get_u64(bytes, &mut i)? as usize;
    let _max_ticks = varint::get_u64(bytes, &mut i)?;

    let recipe_len = varint::get_u64(bytes, &mut i)? as usize;
    if i + recipe_len > bytes.len() {
        anyhow::bail!("k8l1: recipe oob");
    }
    i += recipe_len;

    let mut omega_len: usize = 0;
    if ver == K8L1_VERSION_V2 {
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

    let trailing_len = bytes.len().saturating_sub(i);

    Ok(K8L1View {
        class_patch,
        other_patch,
        omega_len,
        trailing_len,
    })
}

fn patch_count(patch_bytes: &[u8]) -> anyhow::Result<usize> {
    let p = PatchList::decode(patch_bytes).map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(p.entries.len())
}

fn demux_other_patches(other_bytes: &[u8]) -> anyhow::Result<(Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>)> {
    let mut i = 0usize;

    let n = varint::get_u64(other_bytes, &mut i)? as usize;

    let mut kind: Option<Vec<u8>> = None;
    let mut caseb: Option<Vec<u8>> = None;
    let mut letter: Option<Vec<u8>> = None;
    let mut digit: Option<Vec<u8>> = None;
    let mut punct: Option<Vec<u8>> = None;
    let mut raw: Option<Vec<u8>> = None;

    for _ in 0..n {
        let id = varint::get_u64(other_bytes, &mut i)?;
        let len = varint::get_u64(other_bytes, &mut i)? as usize;
        if i + len > other_bytes.len() {
            anyhow::bail!("k8l1: other_patch mux oob (id={}, len={})", id, len);
        }
        let payload = other_bytes[i..i + len].to_vec();
        i += len;

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

    Ok((kind, caseb, letter, digit, punct, raw))
}

pub fn run(args: LaneSweepArgs) -> anyhow::Result<()> {
    let recipe_bytes = recipe_file::load_k8r_bytes(&args.recipe)?;
    let input = std::fs::read(&args.r#in)?;
    let sizes = parse_sizes(&args.sizes)?;

    println!("size,artifact_bytes,class_mismatches,other_mismatches,kind_mismatches,case_mismatches,letter_mismatches,digit_mismatches,punct_mismatches,raw_mismatches,total_len,other_len,emissions_needed");

    for sz in sizes {
        let take = sz.min(input.len());
        let slice = &input[..take];

        let (artifact, stats) =
            lane::encode_k8l1(slice, &recipe_bytes, args.max_ticks).map_err(|e| anyhow::anyhow!("{e}"))?;

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
            kind_m + case_m + letter_m + digit_m + punct_m + raw_m,
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
