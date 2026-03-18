// crates/k8dnz-cli/src/cmd/omega_sweep.rs

use std::io::Write;

use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, ValueEnum};

use crate::cmd::omega::{omega_to_spec, omega_with_lane, parse_omega_spec, LaneName};
use crate::io::recipe_file;
use k8dnz_core::lane;

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum SweepLane {
    Class,
    Kind,
    Case,
    Letter,
    Digit,
    Punct,
    Raw,
}

impl From<SweepLane> for LaneName {
    fn from(v: SweepLane) -> Self {
        match v {
            SweepLane::Class => LaneName::Class,
            SweepLane::Kind => LaneName::Kind,
            SweepLane::Case => LaneName::Case,
            SweepLane::Letter => LaneName::Letter,
            SweepLane::Digit => LaneName::Digit,
            SweepLane::Punct => LaneName::Punct,
            SweepLane::Raw => LaneName::Raw,
        }
    }
}

#[derive(Args)]
pub struct OmegaSweepArgs {
    #[arg(long)]
    pub recipe: String,

    #[arg(long = "in")]
    pub r#in: String,

    #[arg(long)]
    pub lane: SweepLane,

    #[arg(long, default_value_t = 0)]
    pub skip_from: u64,

    #[arg(long, default_value_t = 2048)]
    pub skip_to: u64,

    #[arg(long, default_value_t = 17)]
    pub skip_step: u64,

    #[arg(long, default_value_t = 1)]
    pub stride: u64,

    /// Optional comma-separated stride list, e.g. "1,2,3,5".
    /// When present, this overrides --stride and sweeps all listed strides.
    #[arg(long)]
    pub stride_list: Option<String>,

    /// Optional baseline omega spec; the sweep overrides only the chosen lane.
    #[arg(long)]
    pub omega: Option<String>,

    /// Base max ticks for each attempt (auto-ticks may bump this).
    #[arg(long, default_value_t = 20_000_000)]
    pub max_ticks: u64,

    #[arg(long, default_value_t = true)]
    pub auto_ticks: bool,

    #[arg(long, default_value_t = 2)]
    pub auto_ticks_mul: u64,

    #[arg(long, default_value_t = 80_000_000)]
    pub auto_ticks_cap: u64,

    /// Verify decode for each successful row (slower).
    #[arg(long, default_value_t = false)]
    pub verify: bool,

    /// Continue sweep even if a particular candidate fails.
    #[arg(long, default_value_t = true)]
    pub fail_soft: bool,

    /// Print a BEST line whenever we find a new best artifact_bytes.
    #[arg(long, default_value_t = true)]
    pub print_best: bool,

    /// Reference zstd level for the truth surface.
    #[arg(long, default_value_t = 3)]
    pub zstd_level: i32,

    /// Optional budget. If > 0, rows at or below budget are tagged UNDER_BUDGET.
    #[arg(long, default_value_t = 0)]
    pub budget_bytes: usize,

    /// Optional CSV output file (still prints header/rows to stdout).
    #[arg(long)]
    pub out_csv: Option<String>,

    /// Optional path to write the best resulting omega spec.
    #[arg(long)]
    pub best_omega_out: Option<String>,
}

#[derive(Clone, Debug)]
struct RowStats {
    artifact_bytes: usize,
    class_mismatches: usize,
    other_mismatches: usize,
    kind_mismatches: usize,
    case_mismatches: usize,
    letter_mismatches: usize,
    digit_mismatches: usize,
    punct_mismatches: usize,
    raw_mismatches: usize,
    total_len: usize,
    other_len: usize,
    emissions_needed: usize,
}

#[derive(Clone, Debug)]
struct SweepRow {
    lane: String,
    skip: u64,
    stride: u64,
    artifact_bytes: usize,
    plain_zstd_bytes: usize,
    delta_vs_plain_zstd: i64,
    class_mismatches: usize,
    other_mismatches: usize,
    kind_mismatches: usize,
    case_mismatches: usize,
    letter_mismatches: usize,
    digit_mismatches: usize,
    punct_mismatches: usize,
    raw_mismatches: usize,
    total_len: usize,
    other_len: usize,
    emissions_needed: usize,
    max_ticks_used: u64,
    status: String,
    omega_spec: String,
}

pub fn run(args: OmegaSweepArgs) -> Result<()> {
    if args.skip_step == 0 {
        bail!("skip-step must be >= 1");
    }
    if args.skip_from > args.skip_to {
        bail!("skip-from must be <= skip-to");
    }
    if args.auto_ticks_mul < 2 {
        bail!("auto-ticks-mul must be >= 2");
    }
    if args.auto_ticks_cap < args.max_ticks {
        bail!("auto-ticks-cap must be >= max-ticks");
    }

    let recipe_bytes = recipe_file::load_k8r_bytes(&args.recipe)
        .with_context(|| format!("load recipe {}", args.recipe))?;
    let input = std::fs::read(&args.r#in).with_context(|| format!("read {}", args.r#in))?;
    let plain_zstd_bytes = zstd::stream::encode_all(std::io::Cursor::new(&input), args.zstd_level)
        .context("zstd encode omega-sweep input")?
        .len();

    let base = match &args.omega {
        Some(s) => parse_omega_spec(s)?,
        None => k8dnz_core::lane::OmegaProgram::default(),
    };
    let base_spec = omega_to_spec(&base);
    let lane_name: LaneName = args.lane.into();
    let strides = parse_stride_list(args.stride_list.as_deref(), args.stride)?;

    let mut out_fh = match &args.out_csv {
        Some(p) => Some(std::io::BufWriter::new(std::fs::File::create(p)?)),
        None => None,
    };

    let header = "lane,skip,stride,artifact_bytes,plain_zstd_bytes,delta_vs_plain_zstd,class_m,other_m,kind_m,case_m,letter_m,digit_m,punct_m,raw_m,total_len,other_len,emissions_needed,max_ticks_used,status,omega_spec\n";
    print!("{}", header);
    if let Some(fh) = out_fh.as_mut() {
        fh.write_all(header.as_bytes())?;
        fh.flush()?;
    }

    eprintln!(
        "omega_sweep start: lane={} plain_zstd_bytes={} base_omega={}",
        lane_name.as_str(),
        plain_zstd_bytes,
        base_spec
    );

    let mut best: Option<SweepRow> = None;

    for &stride in &strides {
        let mut skip = args.skip_from;
        while skip <= args.skip_to {
            let omega = omega_with_lane(&base, lane_name, skip, stride)?;
            let omega_spec = omega_to_spec(&omega);

            match encode_with_retries(
                &input,
                &recipe_bytes,
                args.max_ticks,
                args.auto_ticks,
                args.auto_ticks_mul,
                args.auto_ticks_cap,
                omega,
            ) {
                Ok((artifact, st, ticks_used)) => {
                    if args.verify {
                        let decoded = lane::decode_k8l1(&artifact).map_err(|e| anyhow!("{e}"))?;
                        let norm = k8dnz_core::repr::text_norm::normalize_newlines(&input);
                        if decoded != norm {
                            let row = SweepRow {
                                lane: lane_name.as_str().to_string(),
                                skip,
                                stride,
                                artifact_bytes: st.artifact_bytes,
                                plain_zstd_bytes,
                                delta_vs_plain_zstd: st.artifact_bytes as i64 - plain_zstd_bytes as i64,
                                class_mismatches: st.class_mismatches,
                                other_mismatches: st.other_mismatches,
                                kind_mismatches: st.kind_mismatches,
                                case_mismatches: st.case_mismatches,
                                letter_mismatches: st.letter_mismatches,
                                digit_mismatches: st.digit_mismatches,
                                punct_mismatches: st.punct_mismatches,
                                raw_mismatches: st.raw_mismatches,
                                total_len: st.total_len,
                                other_len: st.other_len,
                                emissions_needed: st.emissions_needed,
                                max_ticks_used: ticks_used,
                                status: "VERIFY_FAIL".to_string(),
                                omega_spec,
                            };
                            emit_row(&row, out_fh.as_mut())?;
                            if !args.fail_soft {
                                bail!("verify failed at skip={} stride={}", skip, stride);
                            }
                            skip = advance_skip(skip, args.skip_step)?;
                            continue;
                        }
                    }

                    let mut status = String::from("OK");
                    if args.budget_bytes > 0 && st.artifact_bytes <= args.budget_bytes {
                        status = String::from("UNDER_BUDGET");
                    }

                    let row = SweepRow {
                        lane: lane_name.as_str().to_string(),
                        skip,
                        stride,
                        artifact_bytes: st.artifact_bytes,
                        plain_zstd_bytes,
                        delta_vs_plain_zstd: st.artifact_bytes as i64 - plain_zstd_bytes as i64,
                        class_mismatches: st.class_mismatches,
                        other_mismatches: st.other_mismatches,
                        kind_mismatches: st.kind_mismatches,
                        case_mismatches: st.case_mismatches,
                        letter_mismatches: st.letter_mismatches,
                        digit_mismatches: st.digit_mismatches,
                        punct_mismatches: st.punct_mismatches,
                        raw_mismatches: st.raw_mismatches,
                        total_len: st.total_len,
                        other_len: st.other_len,
                        emissions_needed: st.emissions_needed,
                        max_ticks_used: ticks_used,
                        status,
                        omega_spec,
                    };
                    emit_row(&row, out_fh.as_mut())?;

                    let is_better = match &best {
                        None => true,
                        Some(cur) => better_row(&row, cur),
                    };
                    if is_better {
                        best = Some(row.clone());
                        if args.print_best {
                            eprintln!(
                                "BEST lane={} skip={} stride={} artifact_bytes={} delta_vs_plain_zstd={} class_m={} other_m={} max_ticks_used={} omega={}",
                                row.lane,
                                row.skip,
                                row.stride,
                                row.artifact_bytes,
                                row.delta_vs_plain_zstd,
                                row.class_mismatches,
                                row.other_mismatches,
                                row.max_ticks_used,
                                row.omega_spec
                            );
                        }
                    }
                }
                Err(e) => {
                    let row = SweepRow {
                        lane: lane_name.as_str().to_string(),
                        skip,
                        stride,
                        artifact_bytes: 0,
                        plain_zstd_bytes,
                        delta_vs_plain_zstd: 0,
                        class_mismatches: 0,
                        other_mismatches: 0,
                        kind_mismatches: 0,
                        case_mismatches: 0,
                        letter_mismatches: 0,
                        digit_mismatches: 0,
                        punct_mismatches: 0,
                        raw_mismatches: 0,
                        total_len: input.len(),
                        other_len: 0,
                        emissions_needed: 0,
                        max_ticks_used: 0,
                        status: format!("ERROR:{}", e.to_string().replace(',', ";")),
                        omega_spec,
                    };
                    emit_row(&row, out_fh.as_mut())?;
                    if !args.fail_soft {
                        return Err(e);
                    }
                }
            }

            skip = advance_skip(skip, args.skip_step)?;
        }
    }

    if let Some(fh) = out_fh.as_mut() {
        fh.flush()?;
    }

    if let Some(best) = &best {
        eprintln!(
            "best_row=lane={} skip={} stride={} artifact_bytes={} plain_zstd_bytes={} delta_vs_plain_zstd={} class_m={} other_m={} kind_m={} case_m={} letter_m={} digit_m={} punct_m={} raw_m={} total_len={} other_len={} emissions_needed={} max_ticks_used={} status={} omega={}",
            best.lane,
            best.skip,
            best.stride,
            best.artifact_bytes,
            best.plain_zstd_bytes,
            best.delta_vs_plain_zstd,
            best.class_mismatches,
            best.other_mismatches,
            best.kind_mismatches,
            best.case_mismatches,
            best.letter_mismatches,
            best.digit_mismatches,
            best.punct_mismatches,
            best.raw_mismatches,
            best.total_len,
            best.other_len,
            best.emissions_needed,
            best.max_ticks_used,
            best.status,
            best.omega_spec,
        );

        if let Some(path) = args.best_omega_out.as_deref() {
            std::fs::write(path, best.omega_spec.as_bytes())
                .with_context(|| format!("write {}", path))?;
            eprintln!("best_omega_written={}", path);
        }
    }

    Ok(())
}

fn emit_row(row: &SweepRow, out_fh: Option<&mut std::io::BufWriter<std::fs::File>>) -> Result<()> {
    let line = format!(
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n",
        row.lane,
        row.skip,
        row.stride,
        row.artifact_bytes,
        row.plain_zstd_bytes,
        row.delta_vs_plain_zstd,
        row.class_mismatches,
        row.other_mismatches,
        row.kind_mismatches,
        row.case_mismatches,
        row.letter_mismatches,
        row.digit_mismatches,
        row.punct_mismatches,
        row.raw_mismatches,
        row.total_len,
        row.other_len,
        row.emissions_needed,
        row.max_ticks_used,
        csv_escape(&row.status),
        csv_escape(&row.omega_spec),
    );
    print!("{}", line);
    if let Some(fh) = out_fh {
        fh.write_all(line.as_bytes())?;
    }
    Ok(())
}

fn better_row(a: &SweepRow, b: &SweepRow) -> bool {
    (
        a.artifact_bytes,
        a.delta_vs_plain_zstd,
        a.other_mismatches,
        a.class_mismatches,
        a.max_ticks_used,
        a.skip,
        a.stride,
    ) < (
        b.artifact_bytes,
        b.delta_vs_plain_zstd,
        b.other_mismatches,
        b.class_mismatches,
        b.max_ticks_used,
        b.skip,
        b.stride,
    )
}

fn parse_stride_list(spec: Option<&str>, fallback_stride: u64) -> Result<Vec<u64>> {
    match spec {
        Some(s) => {
            let mut out = Vec::new();
            for part in s.split(',') {
                let t = part.trim();
                if t.is_empty() {
                    continue;
                }
                let v = t.parse::<u64>()?;
                if v == 0 {
                    bail!("stride-list values must be >= 1");
                }
                out.push(v);
            }
            if out.is_empty() {
                bail!("stride-list is empty");
            }
            out.sort_unstable();
            out.dedup();
            Ok(out)
        }
        None => {
            if fallback_stride == 0 {
                bail!("stride must be >= 1");
            }
            Ok(vec![fallback_stride])
        }
    }
}

fn advance_skip(skip: u64, step: u64) -> Result<u64> {
    let next = skip.saturating_add(step);
    if next == u64::MAX {
        bail!("omega-sweep overflowed skip range");
    }
    Ok(next)
}

fn encode_with_retries(
    input: &[u8],
    recipe_bytes: &[u8],
    base_max_ticks: u64,
    auto_ticks: bool,
    mul: u64,
    cap: u64,
    omega: k8dnz_core::lane::OmegaProgram,
) -> Result<(Vec<u8>, RowStats, u64)> {
    let mut max_ticks = base_max_ticks.max(1);

    loop {
        match lane::encode_k8l1_with_omega_prog(input, recipe_bytes, max_ticks, omega.clone()) {
            Ok((artifact, st)) => {
                let row = RowStats {
                    artifact_bytes: st.artifact_bytes,
                    class_mismatches: st.class_mismatches,
                    other_mismatches: st.other_mismatches,
                    kind_mismatches: st.kind_mismatches,
                    case_mismatches: st.case_mismatches,
                    letter_mismatches: st.letter_mismatches,
                    digit_mismatches: st.digit_mismatches,
                    punct_mismatches: st.punct_mismatches,
                    raw_mismatches: st.raw_mismatches,
                    total_len: st.total_len,
                    other_len: st.other_len,
                    emissions_needed: st.emissions_needed,
                };
                return Ok((artifact, row, max_ticks));
            }
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

fn csv_escape(s: &str) -> String {
    let escaped = s.replace('"', "\"\"");
    format!("\"{}\"", escaped)
}
