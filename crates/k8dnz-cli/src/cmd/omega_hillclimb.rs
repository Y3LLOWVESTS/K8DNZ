// crates/k8dnz-cli/src/cmd/omega_hillclimb.rs

use clap::{Args, ValueEnum};

use crate::cmd::omega::{omega_with_lane_single, parse_omega_spec, LaneName};
use crate::io::recipe_file;
use k8dnz_core::lane;

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum HillLane {
    Class,
    Kind,
    Case,
    Letter,
    Digit,
    Punct,
    Raw,
}

impl From<HillLane> for LaneName {
    fn from(v: HillLane) -> Self {
        match v {
            HillLane::Class => LaneName::Class,
            HillLane::Kind => LaneName::Kind,
            HillLane::Case => LaneName::Case,
            HillLane::Letter => LaneName::Letter,
            HillLane::Digit => LaneName::Digit,
            HillLane::Punct => LaneName::Punct,
            HillLane::Raw => LaneName::Raw,
        }
    }
}

#[derive(Args)]
pub struct OmegaHillclimbArgs {
    #[arg(long)]
    pub recipe: String,

    #[arg(long = "in")]
    pub r#in: String,

    #[arg(long, value_delimiter = ',', default_value = "letter,kind,class")]
    pub lanes: Vec<HillLane>,

    #[arg(long, default_value_t = 0)]
    pub skip_from: u64,

    #[arg(long, default_value_t = 16384)]
    pub skip_to: u64,

    #[arg(long, default_value_t = 257)]
    pub skip_step: u64,

    #[arg(long, default_value_t = 1)]
    pub stride: u64,

    #[arg(long)]
    pub omega: Option<String>,

    #[arg(long, default_value_t = 20_000_000)]
    pub max_ticks: u64,

    #[arg(long, default_value_t = true)]
    pub auto_ticks: bool,

    #[arg(long, default_value_t = 2)]
    pub auto_ticks_mul: u64,

    #[arg(long, default_value_t = 160_000_000)]
    pub auto_ticks_cap: u64,

    /// Optional: cheaper ticks for each trial during hillclimb (final omega still printed)
    #[arg(long)]
    pub trial_max_ticks: Option<u64>,

    /// Optional: cheaper auto-ticks cap for each trial during hillclimb
    #[arg(long)]
    pub trial_auto_ticks_cap: Option<u64>,

    /// Print progress every N trials per lane
    #[arg(long, default_value_t = 8)]
    pub progress_every: u64,

    #[arg(long, default_value_t = false)]
    pub verify: bool,

    #[arg(long, default_value_t = 2)]
    pub rounds: u32,
}

fn encode_with_retries(
    input: &[u8],
    recipe_bytes: &[u8],
    base_max_ticks: u64,
    auto_ticks: bool,
    mul: u64,
    cap: u64,
    omega: k8dnz_core::lane::OmegaProgram,
) -> anyhow::Result<(lane::LaneEncodeStats, u64)> {
    let mut max_ticks = base_max_ticks;
    loop {
        match lane::encode_k8l1_with_omega_prog(input, recipe_bytes, max_ticks, omega.clone()) {
            Ok((_artifact, st)) => return Ok((st, max_ticks)),
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

fn omega_to_spec(o: &k8dnz_core::lane::OmegaProgram) -> String {
    fn lane_seg(name: &str, lp: &k8dnz_core::lane::LaneOmegaProg) -> Option<String> {
        if lp.segs.is_empty() {
            return None;
        }
        if lp.segs.len() == 1 {
            let s = lp.segs[0];
            if s.skip == 0 && s.stride == 1 {
                return None;
            }
            return Some(format!("{name}:skip={},stride={}", s.skip, s.stride));
        }
        if lp.segs.len() == 2 {
            let a = lp.segs[0];
            let b = lp.segs[1];
            return Some(format!("{name}:seg2={}:{}|{}:{}", a.skip, a.stride, b.skip, b.stride));
        }
        let mut parts = Vec::new();
        for (ix, s) in lp.segs.iter().copied().enumerate() {
            parts.push(format!("{}:{}:{}", ix, s.skip, s.stride));
        }
        Some(format!("{name}:segs={}", parts.join("|")))
    }

    let mut segs = Vec::new();
    if let Some(s) = lane_seg("class", &o.class) {
        segs.push(s);
    }
    if let Some(s) = lane_seg("kind", &o.kind) {
        segs.push(s);
    }
    if let Some(s) = lane_seg("case", &o.caseb) {
        segs.push(s);
    }
    if let Some(s) = lane_seg("letter", &o.letter) {
        segs.push(s);
    }
    if let Some(s) = lane_seg("digit", &o.digit) {
        segs.push(s);
    }
    if let Some(s) = lane_seg("punct", &o.punct) {
        segs.push(s);
    }
    if let Some(s) = lane_seg("raw", &o.raw) {
        segs.push(s);
    }
    segs.join(";")
}

fn count_trials(skip_from: u64, skip_to: u64, step: u64) -> u64 {
    if step == 0 || skip_from > skip_to {
        return 0;
    }
    let span = skip_to - skip_from;
    (span / step) + 1
}

pub fn run(args: OmegaHillclimbArgs) -> anyhow::Result<()> {
    if args.skip_step == 0 {
        anyhow::bail!("skip-step must be >= 1");
    }
    if args.skip_from > args.skip_to {
        anyhow::bail!("skip-from must be <= skip-to");
    }
    if args.stride == 0 {
        anyhow::bail!("stride must be >= 1");
    }
    if args.auto_ticks_mul < 2 {
        anyhow::bail!("auto-ticks-mul must be >= 2");
    }
    if args.auto_ticks_cap < args.max_ticks {
        anyhow::bail!("auto-ticks-cap must be >= max-ticks");
    }
    if args.rounds == 0 {
        anyhow::bail!("rounds must be >= 1");
    }
    if args.progress_every == 0 {
        anyhow::bail!("progress-every must be >= 1");
    }

    let recipe_bytes = recipe_file::load_k8r_bytes(&args.recipe)?;
    let input = std::fs::read(&args.r#in)?;

    let mut omega = match &args.omega {
        Some(s) => parse_omega_spec(s)?,
        None => k8dnz_core::lane::OmegaProgram::default(),
    };

    let trials_total = count_trials(args.skip_from, args.skip_to, args.skip_step);

    let trial_max_ticks = args.trial_max_ticks.unwrap_or(args.max_ticks);
    let trial_auto_cap = args
        .trial_auto_ticks_cap
        .unwrap_or(args.auto_ticks_cap)
        .min(args.auto_ticks_cap);

    eprintln!("start_omega={}", omega_to_spec(&omega));
    eprintln!(
        "trials_per_lane={} trial_max_ticks={} trial_auto_ticks_cap={}",
        trials_total, trial_max_ticks, trial_auto_cap
    );

    for round in 0..args.rounds {
        eprintln!("round={}", round + 1);

        for lane_v in &args.lanes {
            let lane_name: LaneName = (*lane_v).into();

            let mut best_skip: Option<u64> = None;
            let mut best_bytes: Option<usize> = None;
            let mut best_ticks: u64 = trial_max_ticks;

            let mut ok_count: u64 = 0;
            let mut fail_count: u64 = 0;

            eprintln!("lane_start={:?} omega={}", lane_v, omega_to_spec(&omega));

            let mut skip = args.skip_from;
            let mut tix: u64 = 0;

            while skip <= args.skip_to {
                tix += 1;

                if tix == 1 || (tix % args.progress_every == 0) {
                    eprintln!(
                        "lane={:?} trial={}/{} skip={} ok={} fail={} best_bytes={}",
                        lane_v,
                        tix,
                        trials_total,
                        skip,
                        ok_count,
                        fail_count,
                        best_bytes.map(|v| v.to_string()).unwrap_or_else(|| "NA".to_string())
                    );
                }

                let cand = omega_with_lane_single(&omega, lane_name, skip, args.stride)?;

                match encode_with_retries(
                    &input,
                    &recipe_bytes,
                    trial_max_ticks,
                    args.auto_ticks,
                    args.auto_ticks_mul,
                    trial_auto_cap,
                    cand,
                ) {
                    Ok((st, ticks_used)) => {
                        ok_count += 1;
                        let cur = st.artifact_bytes;

                        let improve = best_bytes.map(|b| cur < b).unwrap_or(true);
                        if improve {
                            best_bytes = Some(cur);
                            best_skip = Some(skip);
                            best_ticks = ticks_used;
                        }
                    }
                    Err(_) => {
                        fail_count += 1;
                    }
                }

                skip = skip.saturating_add(args.skip_step);
                if skip == u64::MAX {
                    break;
                }
            }

            if let (Some(s), Some(b)) = (best_skip, best_bytes) {
                omega = omega_with_lane_single(&omega, lane_name, s, args.stride)?;
                eprintln!(
                    "lane_done={:?} best_skip={} best_bytes={} ticks_used={} ok={} fail={} omega={}",
                    lane_v,
                    s,
                    b,
                    best_ticks,
                    ok_count,
                    fail_count,
                    omega_to_spec(&omega)
                );
            } else {
                eprintln!(
                    "lane_done={:?} no_valid_candidate ok={} fail={} omega_unchanged={}",
                    lane_v,
                    ok_count,
                    fail_count,
                    omega_to_spec(&omega)
                );
            }
        }
    }

    eprintln!("final_omega={}", omega_to_spec(&omega));

    if args.verify {
        let (artifact, _st) = lane::encode_k8l1_with_omega_prog(&input, &recipe_bytes, args.max_ticks, omega.clone())
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let decoded = lane::decode_k8l1(&artifact).map_err(|e| anyhow::anyhow!("{e}"))?;
        let norm = k8dnz_core::repr::text_norm::normalize_newlines(&input);
        if decoded != norm {
            anyhow::bail!("verify failed: final_omega did not roundtrip");
        }
        eprintln!("verify_ok=1");
    }

    Ok(())
}
