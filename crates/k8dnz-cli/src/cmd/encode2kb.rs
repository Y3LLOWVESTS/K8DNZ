// crates/k8dnz-cli/src/cmd/encode2kb.rs

use anyhow::{anyhow, bail, Context, Result};
use clap::Args;

use crate::cmd::omega::{omega_to_spec, parse_omega_spec};
use crate::io::recipe_file;
use k8dnz_apextrace::{generate_bytes, ApexKey, SearchCfg};
use k8dnz_core::lane;
use k8dnz_core::repr::{text_norm, ws_lanes::WsLanes};
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

    #[arg(long, default_value_t = 3)]
    pub zstd_level: i32,

    /// Optional Ω schedule/program.
    ///
    /// V1: "letter:skip=251,stride=1;kind:skip=113,stride=1"
    /// V2: "letter:seg2=323:1|900:1"
    #[arg(long)]
    pub omega: Option<String>,

    /// Optional ApexTrace comparator on the whitespace/class lane.
    ///
    /// This does NOT change the encoded artifact. It only reports whether a
    /// single global ApexTrace key would beat the current class patch bytes.
    #[arg(long, default_value_t = false)]
    pub apex_class_report: bool,

    #[arg(long, default_value_t = 0)]
    pub apex_seed_from: u64,

    #[arg(long, default_value_t = 512)]
    pub apex_seed_count: u64,

    #[arg(long, default_value_t = 1)]
    pub apex_seed_step: u64,

    #[arg(long, default_value_t = 1)]
    pub apex_recipe_seed: u64,
}

const MAGIC_K8L1: &[u8; 4] = b"K8L1";
const K8L1_VERSION_MIN: u8 = 1;
const K8L1_VERSION_MAX: u8 = 3;

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

    kind_bytes: usize,
    caseb_bytes: usize,
    letter_bytes: usize,
    digit_bytes: usize,
    punct_bytes: usize,
    raw_bytes: usize,

    other_mux_overhead_bytes: usize,
}

#[derive(Clone, Debug)]
struct K8L1View {
    ver: u8,
    total_len: usize,
    other_len: usize,
    max_ticks: u64,
    recipe_bytes: Vec<u8>,
    omega_len: usize,
    class_patch: Vec<u8>,
    other_patch: Vec<u8>,
    trailing_len: usize,
    consumed_len: usize,
    recipe_len: usize,
    class_patch_len: usize,
    other_patch_len: usize,
}

#[derive(Clone, Debug, Default)]
struct ApexClassReport {
    key_bytes_exact: usize,
    patch_entries: usize,
    patch_bytes: usize,
    total_payload_exact: usize,
    matches: u64,
    total: u64,
    match_pct: f64,
    root_quadrant: u8,
    root_seed: u64,
    recipe_seed: u64,
    delta_patch_vs_class_patch: i64,
    delta_total_vs_class_patch: i64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ApexWsScore {
    matches: u64,
    prefix: u64,
    total: u64,
    longest_run: u64,
}

impl ApexWsScore {
    fn better_than(&self, other: &Self) -> bool {
        (self.matches, self.longest_run, self.prefix) > (other.matches, other.longest_run, other.prefix)
    }
}

pub fn run(args: Encode2kbArgs) -> Result<()> {
    let input = std::fs::read(&args.r#in).with_context(|| format!("read {}", args.r#in))?;
    let recipe_bytes = recipe_file::load_k8r_bytes(&args.recipe)
        .with_context(|| format!("load recipe {}", args.recipe))?;

    let omega = match args.omega.as_deref() {
        Some(spec) => parse_omega_spec(spec)?,
        None => k8dnz_core::lane::OmegaProgram::default(),
    };
    let omega_spec = omega_to_spec(&omega);

    let (artifact, stats, max_ticks_used) = encode_with_retries(
        &input,
        &recipe_bytes,
        args.max_ticks,
        args.auto_ticks,
        args.auto_tries,
        args.auto_mul,
        args.auto_max_ticks,
        omega,
    )?;

    std::fs::write(&args.out, &artifact).with_context(|| format!("write {}", args.out))?;

    let view = decode_k8l1_view(&artifact)?;
    let bd = decode_patch_breakdown(&view.other_patch).unwrap_or_default();

    let total_bytes = artifact.len();
    let plain_zstd_bytes = zstd_bytes(&input, args.zstd_level)?;
    let delta_vs_plain_zstd = total_bytes as i64 - plain_zstd_bytes as i64;

    let recipe_payload = view.recipe_len;
    let omega_payload = view.omega_len;
    let class_payload = view.class_patch_len;
    let other_payload = view.other_patch_len;
    let payload_sum = recipe_payload
        .saturating_add(omega_payload)
        .saturating_add(class_payload)
        .saturating_add(other_payload);
    let header_overhead = view.consumed_len.saturating_sub(payload_sum);

    let other_lane_payload_sum = bd
        .kind_bytes
        .saturating_add(bd.caseb_bytes)
        .saturating_add(bd.letter_bytes)
        .saturating_add(bd.digit_bytes)
        .saturating_add(bd.punct_bytes)
        .saturating_add(bd.raw_bytes);

    let apex = if args.apex_class_report {
        Some(run_apex_class_report(
            &input,
            &view.class_patch,
            args.apex_seed_from,
            args.apex_seed_count,
            args.apex_seed_step,
            args.apex_recipe_seed,
        )?)
    } else {
        None
    };

    println!("OMEGA spec={}", omega_spec);

    println!(
        "SCOREBOARD input_bytes={} plain_zstd_bytes={} artifact_bytes={} delta_vs_plain_zstd={} recipe_bytes={} omega_bytes={} class_patch_bytes={} other_patch_bytes={} header_overhead={} max_ticks_used={} emissions_needed={}",
        input.len(),
        plain_zstd_bytes,
        total_bytes,
        delta_vs_plain_zstd,
        recipe_payload,
        omega_payload,
        class_payload,
        other_payload,
        header_overhead,
        max_ticks_used,
        stats.emissions_needed
    );

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

    println!(
        "MISMATCHES class={} other={} | kind={} case={} letter={} digit={} punct={} raw={}",
        stats.class_mismatches,
        stats.other_mismatches,
        bd.kind,
        bd.caseb,
        bd.letter,
        bd.digit,
        bd.punct,
        bd.raw
    );

    if let Some(apex) = &apex {
        println!(
            "APEX_CLASS key_bytes_exact={} patch_entries={} patch_bytes={} total_payload_exact={} matches={} total={} match_pct={:.6} root_quadrant={} root_seed=0x{:016X} recipe_seed=0x{:016X} delta_patch_vs_class_patch={} delta_total_vs_class_patch={}",
            apex.key_bytes_exact,
            apex.patch_entries,
            apex.patch_bytes,
            apex.total_payload_exact,
            apex.matches,
            apex.total,
            apex.match_pct,
            apex.root_quadrant,
            apex.root_seed,
            apex.recipe_seed,
            apex.delta_patch_vs_class_patch,
            apex.delta_total_vs_class_patch
        );
    }

    if total_bytes > args.budget_bytes {
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

        bail!(
            "OVER BUDGET: artifact_bytes={} budget_bytes={} plain_zstd_bytes={} delta_vs_plain_zstd={} omega={} (class_mismatches={} other_mismatches={} total_len={} other_len={} emissions_needed={} | kind={} case={} letter={} digit={} punct={} raw={})",
            total_bytes,
            args.budget_bytes,
            plain_zstd_bytes,
            delta_vs_plain_zstd,
            omega_spec,
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
        let decoded = lane::decode_k8l1(&artifact).map_err(|e| anyhow!("{e}"))?;
        let norm = text_norm::normalize_newlines(&input);
        if decoded != norm {
            bail!(
                "VERIFY FAILED: decoded != normalized input (decoded_len={}, norm_len={})",
                decoded.len(),
                norm.len()
            );
        }
    }

    println!(
        "ok encode2kb: out={} artifact_bytes={} plain_zstd_bytes={} delta_vs_plain_zstd={} omega={} class_mismatches={} other_mismatches={} total_len={} other_len={} emissions_needed={} kind_mismatches={} case_mismatches={} letter_mismatches={} digit_mismatches={} punct_mismatches={} raw_mismatches={}",
        args.out,
        stats.artifact_bytes,
        plain_zstd_bytes,
        delta_vs_plain_zstd,
        omega_spec,
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

fn encode_with_retries(
    input: &[u8],
    recipe_bytes: &[u8],
    base_max_ticks: u64,
    auto_ticks: bool,
    auto_tries: u32,
    mul: u64,
    cap: u64,
    omega: k8dnz_core::lane::OmegaProgram,
) -> Result<(Vec<u8>, lane::LaneEncodeStats, u64)> {
    let mut max_ticks = base_max_ticks.max(1);
    let mut tries = 0u32;

    loop {
        match lane::encode_k8l1_with_omega_prog(input, recipe_bytes, max_ticks, omega.clone()) {
            Ok((artifact, stats)) => return Ok((artifact, stats, max_ticks)),
            Err(e) => {
                let s = e.to_string();
                let is_insufficient = s.contains("insufficient emissions")
                    || s.contains("need 1, got 0")
                    || s.contains("within max_ticks");

                if auto_ticks && is_insufficient && tries < auto_tries && max_ticks < cap {
                    let next = max_ticks.saturating_mul(mul).min(cap);
                    if next == max_ticks {
                        return Err(anyhow!("{e}"));
                    }
                    eprintln!(
                        "encode2kb retry: max_ticks={} failed with insufficient emissions; retrying with max_ticks={}",
                        max_ticks,
                        next
                    );
                    max_ticks = next;
                    tries = tries.saturating_add(1);
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
        bail!("k8l1: too short");
    }
    if &bytes[0..4] != MAGIC_K8L1 {
        bail!("k8l1: bad magic");
    }
    i += 4;

    let ver = bytes[i];
    i += 1;
    if !(K8L1_VERSION_MIN..=K8L1_VERSION_MAX).contains(&ver) {
        bail!("k8l1: unsupported version {}", ver);
    }

    let total_len = varint::get_u64(bytes, &mut i)? as usize;
    let other_len = varint::get_u64(bytes, &mut i)? as usize;
    let max_ticks = varint::get_u64(bytes, &mut i)?;

    let recipe_len = varint::get_u64(bytes, &mut i)? as usize;
    if i + recipe_len > bytes.len() {
        bail!("k8l1: recipe oob");
    }
    let recipe_bytes = bytes[i..i + recipe_len].to_vec();
    i += recipe_len;

    let mut omega_len = 0usize;
    if ver >= 2 {
        omega_len = varint::get_u64(bytes, &mut i)? as usize;
        if i + omega_len > bytes.len() {
            bail!("k8l1: omega oob");
        }
        i += omega_len;
    }

    let class_patch_len = varint::get_u64(bytes, &mut i)? as usize;
    if i + class_patch_len > bytes.len() {
        bail!("k8l1: class_patch oob");
    }
    let class_patch = bytes[i..i + class_patch_len].to_vec();
    i += class_patch_len;

    let other_patch_len = varint::get_u64(bytes, &mut i)? as usize;
    if i + other_patch_len > bytes.len() {
        bail!("k8l1: other_patch oob");
    }
    let other_patch = bytes[i..i + other_patch_len].to_vec();
    i += other_patch_len;

    Ok(K8L1View {
        ver,
        total_len,
        other_len,
        max_ticks,
        recipe_bytes,
        omega_len,
        class_patch,
        other_patch,
        trailing_len: bytes.len().saturating_sub(i),
        consumed_len: i,
        recipe_len,
        class_patch_len,
        other_patch_len,
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
            bail!("other_patch child oob");
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

    out.other_mux_overhead_bytes = i
        .saturating_sub(
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

fn run_apex_class_report(
    input: &[u8],
    class_patch_bytes: &[u8],
    seed_from: u64,
    seed_count: u64,
    seed_step: u64,
    recipe_seed: u64,
) -> Result<ApexClassReport> {
    let norm = text_norm::normalize_newlines(input);
    let ws = WsLanes::split(&norm);

    let _class_patch = PatchList::decode(class_patch_bytes).map_err(|e| anyhow!("{e}"))?;
    let class_patch_len = class_patch_bytes.len();

    if seed_step == 0 {
        bail!("apex class report: seed_step must be >= 1");
    }

    let cfg = SearchCfg {
        seed_from,
        seed_count,
        seed_step,
        recipe_seed,
    };

    let mut best_key: Option<ApexKey> = None;
    let mut best_pred = Vec::new();
    let mut best_score = ApexWsScore::default();

    for quadrant in 0u8..=3 {
        let mut i = 0u64;
        while i < cfg.seed_count {
            let seed = cfg.seed_from.saturating_add(i.saturating_mul(cfg.seed_step));
            let key = ApexKey::new_dibit_v1(ws.class_lane.len() as u64, quadrant, seed, cfg.recipe_seed)?;
            let pred_bytes = generate_bytes(&key).map_err(|e| anyhow!("{e}"))?;
            let predicted: Vec<u8> = pred_bytes.into_iter().map(bucket_u8_to_3).collect();
            let score = score_ws_symbols(&ws.class_lane, &predicted)?;

            if best_key.is_none() || score.better_than(&best_score) {
                best_score = score;
                best_pred = predicted;
                best_key = Some(key);
            }

            i = i.saturating_add(1);
        }
    }

    let key = best_key.ok_or_else(|| anyhow!("apex class report: no candidates"))?;
    let patch = PatchList::from_pred_actual(&best_pred, &ws.class_lane).map_err(|e| anyhow!("{e}"))?;
    let patch_bytes = patch.encode();
    let total_payload_exact = 48usize.saturating_add(patch_bytes.len());

    Ok(ApexClassReport {
        key_bytes_exact: 48,
        patch_entries: patch.entries.len(),
        patch_bytes: patch_bytes.len(),
        total_payload_exact,
        matches: best_score.matches,
        total: best_score.total,
        match_pct: pct(best_score.matches, best_score.total),
        root_quadrant: key.root_quadrant,
        root_seed: key.root_seed,
        recipe_seed: key.recipe_seed,
        delta_patch_vs_class_patch: patch_bytes.len() as i64 - class_patch_len as i64,
        delta_total_vs_class_patch: total_payload_exact as i64 - class_patch_len as i64,
    })
}

fn score_ws_symbols(target: &[u8], predicted: &[u8]) -> Result<ApexWsScore> {
    if target.len() != predicted.len() {
        bail!(
            "apex class report: target len {} != predicted len {}",
            target.len(),
            predicted.len()
        );
    }

    let mut matches = 0u64;
    let mut prefix = 0u64;
    let mut still_prefix = true;
    let mut current_run = 0u64;
    let mut longest_run = 0u64;

    for (&t, &p) in target.iter().zip(predicted.iter()) {
        if t > 2 || p > 2 {
            bail!("apex class report: invalid class symbol");
        }

        if t == p {
            matches = matches.saturating_add(1);
            if still_prefix {
                prefix = prefix.saturating_add(1);
            }
            current_run = current_run.saturating_add(1);
            if current_run > longest_run {
                longest_run = current_run;
            }
        } else {
            still_prefix = false;
            current_run = 0;
        }
    }

    Ok(ApexWsScore {
        matches,
        prefix,
        total: target.len() as u64,
        longest_run,
    })
}

#[inline]
fn bucket_u8_to_3(b: u8) -> u8 {
    ((b as u16 * 3u16) >> 8) as u8
}

fn pct(matches: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        matches as f64 * 100.0 / total as f64
    }
}
