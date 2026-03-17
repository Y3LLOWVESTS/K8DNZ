// crates/k8dnz-cli/src/cmd/omega_sweep.rs

use clap::{Args, ValueEnum};

use crate::cmd::omega::{omega_with_lane, parse_omega_spec, LaneName};
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

    /// Optional baseline omega spec; the sweep overrides only the chosen lane.
    #[arg(long)]
    pub omega: Option<String>,

    /// Base max ticks for each attempt (auto-ticks may bump this).
    #[arg(long, default_value_t = 20_000_000)]
    pub max_ticks: u64,

    /// Verify decode for each successful row (slower).
    #[arg(long, default_value_t = false)]
    pub verify: bool,

    /// Print a BEST line whenever we find a new best artifact_bytes.
    #[arg(long, default_value_t = true)]
    pub print_best: bool,

    /// Continue sweep even if a particular skip fails (recommended).
    #[arg(long, default_value_t = true)]
    pub fail_soft: bool,

    /// If we fail due to insufficient emissions, auto-increase max_ticks and retry.
    #[arg(long, default_value_t = true)]
    pub auto_ticks: bool,

    /// Multiply ticks by this factor on each auto retry.
    #[arg(long, default_value_t = 2)]
    pub auto_ticks_mul: u64,

    /// Maximum ticks cap when auto-ticks is enabled.
    #[arg(long, default_value_t = 80_000_000)]
    pub auto_ticks_cap: u64,

    /// Optional CSV output file (still prints header/rows to stdout).
    #[arg(long)]
    pub out_csv: Option<String>,
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

pub fn run(args: OmegaSweepArgs) -> anyhow::Result<()> {
    if args.skip_step == 0 {
        anyhow::bail!("skip-step must be >= 1");
    }
    if args.stride == 0 {
        anyhow::bail!("stride must be >= 1");
    }
    if args.skip_from > args.skip_to {
        anyhow::bail!("skip-from must be <= skip-to");
    }
    if args.auto_ticks_mul < 2 {
        anyhow::bail!("auto-ticks-mul must be >= 2");
    }
    if args.auto_ticks_cap < args.max_ticks {
        anyhow::bail!("auto-ticks-cap must be >= max-ticks");
    }

    let recipe_bytes = recipe_file::load_k8r_bytes(&args.recipe)?;
    let input = std::fs::read(&args.r#in)?;

    // IMPORTANT: your current omega parser returns OmegaProgram.
    let base: k8dnz_core::lane::OmegaProgram = match &args.omega {
        Some(s) => parse_omega_spec(s)?,
        None => k8dnz_core::lane::OmegaProgram::default(),
    };

    let lane_name: LaneName = args.lane.into();

    let mut out_fh = match &args.out_csv {
        Some(p) => Some(std::io::BufWriter::new(std::fs::File::create(p)?)),
        None => None,
    };

    let header = "skip,artifact_bytes,class_m,other_m,kind_m,case_m,letter_m,digit_m,punct_m,raw_m,total_len,other_len,emissions_needed,max_ticks_used,status\n";
    print!("{}", header);
    if let Some(fh) = out_fh.as_mut() {
        use std::io::Write;
        fh.write_all(header.as_bytes())?;
        fh.flush()?;
    }

    let mut best_artifact: Option<usize> = None;
    let mut best_line: Option<String> = None;

    let mut skip = args.skip_from;
    while skip <= args.skip_to {
        // omega_with_lane now returns OmegaProgram in your tree.
        let omega: k8dnz_core::lane::OmegaProgram = omega_with_lane(&base, lane_name, skip, args.stride)?;

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
                    let decoded = lane::decode_k8l1(&artifact).map_err(|e| anyhow::anyhow!("{e}"))?;
                    let norm = k8dnz_core::repr::text_norm::normalize_newlines(&input);
                    if decoded != norm {
                        let line = format!(
                            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},VERIFY_FAIL\n",
                            skip,
                            st.artifact_bytes,
                            st.class_mismatches,
                            st.other_mismatches,
                            st.kind_mismatches,
                            st.case_mismatches,
                            st.letter_mismatches,
                            st.digit_mismatches,
                            st.punct_mismatches,
                            st.raw_mismatches,
                            st.total_len,
                            st.other_len,
                            st.emissions_needed,
                            ticks_used
                        );
                        print!("{}", line);
                        if let Some(fh) = out_fh.as_mut() {
                            use std::io::Write;
                            fh.write_all(line.as_bytes())?;
                        }

                        if !args.fail_soft {
                            anyhow::bail!("verify failed at skip={}", skip);
                        }

                        skip = skip.saturating_add(args.skip_step);
                        continue;
                    }
                }

                let line = format!(
                    "{},{},{},{},{},{},{},{},{},{},{},{},{},{},OK\n",
                    skip,
                    st.artifact_bytes,
                    st.class_mismatches,
                    st.other_mismatches,
                    st.kind_mismatches,
                    st.case_mismatches,
                    st.letter_mismatches,
                    st.digit_mismatches,
                    st.punct_mismatches,
                    st.raw_mismatches,
                    st.total_len,
                    st.other_len,
                    st.emissions_needed,
                    ticks_used
                );

                print!("{}", line);
                if let Some(fh) = out_fh.as_mut() {
                    use std::io::Write;
                    fh.write_all(line.as_bytes())?;
                }

                let cur = st.artifact_bytes;
                let is_better = best_artifact.map(|b| cur < b).unwrap_or(true);
                if is_better {
                    best_artifact = Some(cur);
                    best_line = Some(line.trim_end().to_string());
                    if args.print_best {
                        if let Some(bl) = &best_line {
                            println!("BEST,{}", bl);
                        }
                    }
                }
            }
            Err(e) => {
                let msg = e.to_string().replace(',', ";");
                let line = format!("{},,,,,,,,,,,,,ERROR:{}\n", skip, msg);
                print!("{}", line);
                if let Some(fh) = out_fh.as_mut() {
                    use std::io::Write;
                    fh.write_all(line.as_bytes())?;
                }

                if !args.fail_soft {
                    return Err(e);
                }
            }
        }

        skip = skip.saturating_add(args.skip_step);
        if skip == u64::MAX {
            break;
        }
    }

    if let Some(fh) = out_fh.as_mut() {
        use std::io::Write;
        fh.flush()?;
    }

    if let Some(bl) = best_line {
        eprintln!("best_row={}", bl);
    }

    Ok(())
}

fn encode_with_retries(
    input: &[u8],
    recipe_bytes: &[u8],
    base_max_ticks: u64,
    auto_ticks: bool,
    mul: u64,
    cap: u64,
    omega: k8dnz_core::lane::OmegaProgram,
) -> anyhow::Result<(Vec<u8>, RowStats, u64)> {
    let mut max_ticks = base_max_ticks;

    loop {
        // IMPORTANT: OmegaProgram -> use the *_omega_prog API.
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
                        return Err(anyhow::anyhow!("{e}"));
                    }
                    max_ticks = next;
                    continue;
                }

                return Err(anyhow::anyhow!("{e}"));
            }
        }
    }
}
