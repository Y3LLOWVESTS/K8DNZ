// crates/k8dnz-cli/src/cmd/lane_sweep.rs

use anyhow::{anyhow, Context, Result};
use clap::Args;

use crate::cmd::omega::{omega_to_spec, parse_omega_spec};
use crate::io::recipe_file;
use k8dnz_core::lane;
use k8dnz_core::symbol::patch::PatchList;
use k8dnz_core::symbol::varint;

const MAGIC_K8L1: &[u8; 4] = b"K8L1";
const K8L1_VERSION_MIN: u8 = 1;
const K8L1_VERSION_MAX: u8 = 3;

const PATCH_KIND: u64 = 1;
const PATCH_CASE: u64 = 2;
const PATCH_LETTER: u64 = 3;
const PATCH_DIGIT: u64 = 4;
const PATCH_PUNCT: u64 = 5;
const PATCH_RAW: u64 = 6;

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

    #[arg(long, default_value_t = true)]
    pub auto_ticks: bool,

    #[arg(long, default_value_t = 2)]
    pub auto_mul: u64,

    #[arg(long, default_value_t = 2_000_000_000)]
    pub auto_max_ticks: u64,

    #[arg(long, default_value_t = 3)]
    pub zstd_level: i32,

    #[arg(long)]
    pub omega: Option<String>,

    #[arg(long)]
    pub out_csv: Option<String>,
}

#[derive(Clone, Debug)]
struct K8L1View {
    recipe_len: usize,
    omega_len: usize,
    class_patch_len: usize,
    other_patch_len: usize,
    #[allow(dead_code)]
    class_patch: Vec<u8>,
    other_patch: Vec<u8>,
    consumed_len: usize,
}

#[derive(Clone, Debug, Default)]
struct PatchBreakdown {
    kind: usize,
    caseb: usize,
    letter: usize,
    digit: usize,
    punct: usize,
    raw: usize,

    kind_bytes: usize,
    caseb_bytes: usize,
    letter_bytes: usize,
    digit_bytes: usize,
    punct_bytes: usize,
    raw_bytes: usize,

    other_mux_overhead_bytes: usize,
}

#[derive(Clone, Debug)]
struct SweepRow {
    size: usize,
    omega_spec: String,
    max_ticks_used: u64,
    input_bytes: usize,
    plain_zstd_bytes: usize,
    artifact_bytes: usize,
    delta_vs_plain_zstd: i64,
    recipe_bytes: usize,
    omega_bytes: usize,
    class_patch_bytes: usize,
    other_patch_bytes: usize,
    header_overhead: usize,
    class_mismatches: usize,
    other_mismatches: usize,
    kind_mismatches: usize,
    case_mismatches: usize,
    letter_mismatches: usize,
    digit_mismatches: usize,
    punct_mismatches: usize,
    raw_mismatches: usize,
    emissions_needed: u64,
}

pub fn run(args: LaneSweepArgs) -> Result<()> {
    let input = std::fs::read(&args.r#in).with_context(|| format!("read {}", args.r#in))?;
    let recipe_bytes = recipe_file::load_k8r_bytes(&args.recipe)
        .with_context(|| format!("load recipe {}", args.recipe))?;
    let omega = match args.omega.as_deref() {
        Some(spec) => parse_omega_spec(spec)?,
        None => k8dnz_core::lane::OmegaProgram::default(),
    };
    let omega_spec = omega_to_spec(&omega);

    let sizes = parse_sizes(&args.sizes)?;
    let mut rows = Vec::new();

    for &size in &sizes {
        let take = size.min(input.len());
        let slice = &input[..take];

        let (artifact, stats, max_ticks_used) = encode_with_retries(
            slice,
            &recipe_bytes,
            args.max_ticks,
            args.auto_ticks,
            args.auto_mul,
            args.auto_max_ticks,
            omega.clone(),
        )?;

        let view = decode_k8l1_view(&artifact)?;
        let bd = decode_patch_breakdown(&view.other_patch).unwrap_or_default();

        let plain_zstd_bytes = zstd_bytes(slice, args.zstd_level)?;
        let artifact_bytes = artifact.len();
        let delta_vs_plain_zstd = artifact_bytes as i64 - plain_zstd_bytes as i64;

        let payload_sum = view
            .recipe_len
            .saturating_add(view.omega_len)
            .saturating_add(view.class_patch_len)
            .saturating_add(view.other_patch_len);
        let header_overhead = view.consumed_len.saturating_sub(payload_sum);

        rows.push(SweepRow {
            size: take,
            omega_spec: omega_spec.clone(),
            max_ticks_used,
            input_bytes: take,
            plain_zstd_bytes,
            artifact_bytes,
            delta_vs_plain_zstd,
            recipe_bytes: view.recipe_len,
            omega_bytes: view.omega_len,
            class_patch_bytes: view.class_patch_len,
            other_patch_bytes: view.other_patch_len,
            header_overhead,
            class_mismatches: stats.class_mismatches,
            other_mismatches: stats.other_mismatches,
            kind_mismatches: bd.kind,
            case_mismatches: bd.caseb,
            letter_mismatches: bd.letter,
            digit_mismatches: bd.digit,
            punct_mismatches: bd.punct,
            raw_mismatches: bd.raw,
            emissions_needed: stats.emissions_needed as u64,
        });
    }

    eprintln!("lane_sweep omega={}", omega_spec);
    print_rows(&rows);

    if let Some(path) = args.out_csv.as_deref() {
        std::fs::write(path, rows_to_csv(&rows).as_bytes())
            .with_context(|| format!("write {}", path))?;
        eprintln!("lane_sweep ok: out_csv={}", path);
    }

    Ok(())
}

fn parse_sizes(s: &str) -> Result<Vec<usize>> {
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

fn encode_with_retries(
    input: &[u8],
    recipe_bytes: &[u8],
    base_max_ticks: u64,
    auto_ticks: bool,
    mul: u64,
    cap: u64,
    omega: k8dnz_core::lane::OmegaProgram,
) -> Result<(Vec<u8>, lane::LaneEncodeStats, u64)> {
    let mut max_ticks = base_max_ticks.max(1);
    loop {
        match lane::encode_k8l1_with_omega_prog(input, recipe_bytes, max_ticks, omega.clone()) {
            Ok((artifact, st)) => return Ok((artifact, st, max_ticks)),
            Err(e) => {
                let s = e.to_string();
                let is_insufficient = s.contains("insufficient emissions")
                    || s.contains("need 1, got 0")
                    || s.contains("within max_ticks");

                if auto_ticks && is_insufficient && max_ticks < cap {
                    let next = max_ticks.saturating_mul(mul).min(cap);
                    if next == max_ticks {
                        return Err(anyhow!("{e}"));
                    }
                    max_ticks = next;
                    continue;
                }

                return Err(anyhow!("{e}"));
            }
        }
    }
}

fn decode_k8l1_view(bytes: &[u8]) -> Result<K8L1View> {
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

    let mut omega_len = 0usize;
    if ver >= 2 {
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

    Ok(K8L1View {
        recipe_len,
        omega_len,
        class_patch_len,
        other_patch_len,
        class_patch,
        other_patch,
        consumed_len: i,
    })
}

fn decode_patch_breakdown(other_patch: &[u8]) -> Result<PatchBreakdown> {
    let mut out = PatchBreakdown::default();
    let mut i = 0usize;

    if other_patch.is_empty() {
        return Ok(out);
    }

    let patch_count = varint::get_u64(other_patch, &mut i)? as usize;
    let overhead_start = i;

    for _ in 0..patch_count {
        let patch_id = varint::get_u64(other_patch, &mut i)?;
        let patch_len = varint::get_u64(other_patch, &mut i)? as usize;

        if i + patch_len > other_patch.len() {
            anyhow::bail!("other_patch child oob");
        }

        let child = &other_patch[i..i + patch_len];
        i += patch_len;

        let decoded = PatchList::decode(child).map_err(|e| anyhow!("{e}"))?;
        let entries = decoded.entries.len();

        match patch_id {
            PATCH_KIND => {
                out.kind = entries;
                out.kind_bytes = patch_len;
            }
            PATCH_CASE => {
                out.caseb = entries;
                out.caseb_bytes = patch_len;
            }
            PATCH_LETTER => {
                out.letter = entries;
                out.letter_bytes = patch_len;
            }
            PATCH_DIGIT => {
                out.digit = entries;
                out.digit_bytes = patch_len;
            }
            PATCH_PUNCT => {
                out.punct = entries;
                out.punct_bytes = patch_len;
            }
            PATCH_RAW => {
                out.raw = entries;
                out.raw_bytes = patch_len;
            }
            _ => {}
        }
    }

    out.other_mux_overhead_bytes = i.saturating_sub(
        overhead_start
            .saturating_add(out.kind_bytes)
            .saturating_add(out.caseb_bytes)
            .saturating_add(out.letter_bytes)
            .saturating_add(out.digit_bytes)
            .saturating_add(out.punct_bytes)
            .saturating_add(out.raw_bytes),
    );

    Ok(out)
}

fn zstd_bytes(input: &[u8], level: i32) -> Result<usize> {
    let compressed = zstd::stream::encode_all(std::io::Cursor::new(input), level)
        .context("zstd encode plain input")?;
    Ok(compressed.len())
}

fn print_rows(rows: &[SweepRow]) {
    println!(
        "size,input_bytes,plain_zstd_bytes,artifact_bytes,delta_vs_plain_zstd,recipe_bytes,omega_bytes,class_patch_bytes,other_patch_bytes,header_overhead,max_ticks_used,emissions_needed,class_mismatches,other_mismatches,kind_mismatches,case_mismatches,letter_mismatches,digit_mismatches,punct_mismatches,raw_mismatches,omega_spec"
    );
    for row in rows {
        println!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            row.size,
            row.input_bytes,
            row.plain_zstd_bytes,
            row.artifact_bytes,
            row.delta_vs_plain_zstd,
            row.recipe_bytes,
            row.omega_bytes,
            row.class_patch_bytes,
            row.other_patch_bytes,
            row.header_overhead,
            row.max_ticks_used,
            row.emissions_needed,
            row.class_mismatches,
            row.other_mismatches,
            row.kind_mismatches,
            row.case_mismatches,
            row.letter_mismatches,
            row.digit_mismatches,
            row.punct_mismatches,
            row.raw_mismatches,
            csv_escape(&row.omega_spec)
        );
    }

    if let Some(best_total) = rows.iter().min_by_key(|row| row.artifact_bytes) {
        eprintln!(
            "lane_sweep best_artifact: size={} artifact_bytes={} plain_zstd_bytes={} delta_vs_plain_zstd={} class_mismatches={} other_mismatches={} max_ticks_used={} omega={}",
            best_total.size,
            best_total.artifact_bytes,
            best_total.plain_zstd_bytes,
            best_total.delta_vs_plain_zstd,
            best_total.class_mismatches,
            best_total.other_mismatches,
            best_total.max_ticks_used,
            best_total.omega_spec,
        );
    }

    if let Some(best_delta) = rows.iter().min_by_key(|row| row.delta_vs_plain_zstd) {
        eprintln!(
            "lane_sweep best_delta_vs_zstd: size={} artifact_bytes={} plain_zstd_bytes={} delta_vs_plain_zstd={} class_mismatches={} other_mismatches={} max_ticks_used={} omega={}",
            best_delta.size,
            best_delta.artifact_bytes,
            best_delta.plain_zstd_bytes,
            best_delta.delta_vs_plain_zstd,
            best_delta.class_mismatches,
            best_delta.other_mismatches,
            best_delta.max_ticks_used,
            best_delta.omega_spec,
        );
    }
}

fn rows_to_csv(rows: &[SweepRow]) -> String {
    let mut out = String::from(
        "size,input_bytes,plain_zstd_bytes,artifact_bytes,delta_vs_plain_zstd,recipe_bytes,omega_bytes,class_patch_bytes,other_patch_bytes,header_overhead,max_ticks_used,emissions_needed,class_mismatches,other_mismatches,kind_mismatches,case_mismatches,letter_mismatches,digit_mismatches,punct_mismatches,raw_mismatches,omega_spec\n",
    );

    for row in rows {
        out.push_str(&format!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n",
            row.size,
            row.input_bytes,
            row.plain_zstd_bytes,
            row.artifact_bytes,
            row.delta_vs_plain_zstd,
            row.recipe_bytes,
            row.omega_bytes,
            row.class_patch_bytes,
            row.other_patch_bytes,
            row.header_overhead,
            row.max_ticks_used,
            row.emissions_needed,
            row.class_mismatches,
            row.other_mismatches,
            row.kind_mismatches,
            row.case_mismatches,
            row.letter_mismatches,
            row.digit_mismatches,
            row.punct_mismatches,
            row.raw_mismatches,
            csv_escape(&row.omega_spec)
        ));
    }

    out
}

fn csv_escape(s: &str) -> String {
    let escaped = s.replace('"', "\"\"");
    format!("\"{}\"", escaped)
}
