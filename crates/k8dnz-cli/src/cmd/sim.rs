// crates/k8dnz-cli/src/cmd/sim.rs

use clap::{Args, ValueEnum};
use k8dnz_core::dynamics::engine::{EmissionField, FieldRangeStats};
use k8dnz_core::recipe::recipe::RgbRecipe;
use k8dnz_core::signal::rgb_emit::emit_rgbpair_from_fields;
use k8dnz_core::signal::token::{PairToken, Rgb, RgbPairToken};
use k8dnz_core::{Engine, Recipe};

use crate::io::{bin, jsonl, recipe_file};

use std::time::Instant;

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum SimOutFmt {
    /// JSON lines: {"a":N,"b":N} OR {"a":[r,g,b],"c":[r,g,b]}
    Jsonl,
    /// Packed bytes (pair): byte = (a<<4) | b
    /// Packed bytes (rgbpair): 6 bytes per emission: A.rgb then C.rgb
    Bin,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum SimMode {
    /// Emit PairToken stream (N=16); legacy/proven path.
    Pair,
    /// Emit RGB pair stream (6 bytes/emission).
    Rgbpair,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum Profile {
    /// Uses the tuned default cadence labeling shift (current winner).
    Tuned,
    /// Baseline distribution (shift=0).
    Baseline,
}

fn profile_shift(p: Profile) -> i64 {
    match p {
        Profile::Tuned => 7_141_012,
        Profile::Baseline => 0,
    }
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum RgbBackend {
    /// Shared drift + paired modulation (ordered ramp)
    Cone,
    /// DNA-style (core backend): helix-twist modulation across channels
    Dna,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum RgbAlt {
    None,
    Parity,
}

#[derive(Args, Debug)]
pub struct SimArgs {
    /// Recipe path (.k8r). If omitted, uses built-in default recipe.
    #[arg(long)]
    pub recipe: Option<String>,

    /// Save the effective recipe (after SIM-only overrides; or best candidate in --qsearch) to this .k8r path.
    #[arg(long)]
    pub save_recipe: Option<String>,

    /// Convenience profile for qshift selection.
    /// - tuned:    qshift=7141012 (current default behavior)
    /// - baseline: qshift=0
    ///
    /// NOTE: --qshift overrides --profile.
    #[arg(long, value_enum, default_value_t = Profile::Tuned)]
    pub profile: Profile,

    /// Emissions to produce
    #[arg(long, default_value_t = 64)]
    pub emissions: u64,

    /// Max ticks guard
    #[arg(long, default_value_t = 5_000_000)]
    pub max_ticks: u64,

    /// Output format
    #[arg(long, value_enum, default_value_t = SimOutFmt::Jsonl)]
    pub fmt: SimOutFmt,

    /// Output mode
    #[arg(long, value_enum, default_value_t = SimMode::Pair)]
    pub mode: SimMode,

    /// Output path (required for --fmt bin). For qsearch, writes ONE run with the best shift.
    #[arg(long)]
    pub out: Option<String>,

    /// Print distribution stats / field clamp stats
    #[arg(long)]
    pub stats: bool,

    // --- SIM-only overrides (do NOT mutate recipe on disk) ---
    /// Override quant min (i64)
    #[arg(long)]
    pub qmin: Option<i64>,

    /// Override quant max (i64)
    #[arg(long)]
    pub qmax: Option<i64>,

    /// Override quant shift (i64)
    #[arg(long)]
    pub qshift: Option<i64>,

    /// Override clamp min (i64)
    #[arg(long)]
    pub clamp_min: Option<i64>,

    /// Override clamp max (i64)
    #[arg(long)]
    pub clamp_max: Option<i64>,

    // --- RGBPAIR: true field-based emission (cone/DNA law) ---
    /// If set, --mode rgbpair uses emission-time FIELD samples (clamped) to drive RGB law,
    /// instead of the palette16 mapping in PairToken::to_rgb_pair().
    #[arg(long)]
    pub rgb_from_field: bool,

    /// RGB backend: cone or dna
    #[arg(long, value_enum, default_value_t = RgbBackend::Dna)]
    pub rgb_backend: RgbBackend,

    /// Alternation: none or parity
    #[arg(long, value_enum, default_value_t = RgbAlt::Parity)]
    pub rgb_alt: RgbAlt,

    /// Base color for dot A: "r,g,b"
    #[arg(long, default_value = "255,0,0")]
    pub rgb_base_a: String,

    /// Base color for dot C: "r,g,b"
    #[arg(long, default_value = "0,255,255")]
    pub rgb_base_c: String,

    /// Shared drift step per emission (small int)
    #[arg(long, default_value_t = 2)]
    pub rgb_g_step: i16,

    /// Differential scale multiplier (small int)
    #[arg(long, default_value_t = 2)]
    pub rgb_p_scale: i16,

    // --- QSEARCH (shift neighborhood search) ---
    /// Search around the current quant.shift to find a better shift by quick sampling.
    #[arg(long)]
    pub qsearch: bool,

    /// Number of candidate shifts to evaluate (forced odd; center is base shift).
    #[arg(long, default_value_t = 9)]
    pub qsearch_candidates: usize,

    /// Step size for candidate shifts. Default = quant_width/32.
    #[arg(long)]
    pub qsearch_step: Option<i64>,

    /// Per-candidate emissions (keep small; qsearch is directional).
    #[arg(long, default_value_t = 2_000)]
    pub qsearch_emissions: u64,

    /// Per-candidate max ticks (defaults to 20,000,000 if omitted).
    #[arg(long)]
    pub qsearch_max_ticks: Option<u64>,
}

pub fn run(args: SimArgs) -> anyhow::Result<()> {
    // Load recipe (from file if provided, else default).
    let mut recipe: Recipe = if let Some(path) = args.recipe.as_deref() {
        recipe_file::load_k8r(path)?
    } else {
        k8dnz_core::recipe::defaults::default_recipe()
    };

    // Precedence (matches encode):
    // 1) explicit --qshift wins
    // 2) else if --recipe provided => recipe wins
    // 3) else profile shift
    if let Some(v) = args.qshift {
        recipe.quant.shift = v;
    } else if args.recipe.is_none() {
        recipe.quant.shift = profile_shift(args.profile);
    }

    // SIM-only overrides (explicit inputs preserve determinism).
    if let Some(v) = args.qmin {
        recipe.quant.min = v;
    }
    if let Some(v) = args.qmax {
        recipe.quant.max = v;
    }
    if let Some(v) = args.clamp_min {
        recipe.field_clamp.min = v;
    }
    if let Some(v) = args.clamp_max {
        recipe.field_clamp.max = v;
    }

    // Guard: quant range must be sane.
    if recipe.quant.min >= recipe.quant.max {
        anyhow::bail!(
            "invalid quant range: min={} max={} (need min < max)",
            recipe.quant.min,
            recipe.quant.max
        );
    }
    // Guard: clamp range must be sane.
    if recipe.field_clamp.min >= recipe.field_clamp.max {
        anyhow::bail!(
            "invalid clamp range: min={} max={} (need min < max)",
            recipe.field_clamp.min,
            recipe.field_clamp.max
        );
    }

    // Operator clarity label (align with encode):
    // - if --qshift provided => custom
    // - else if --recipe provided => recipe
    // - else => tuned/baseline
    let profile_label = if args.qshift.is_some() {
        "custom"
    } else if args.recipe.is_some() {
        "recipe"
    } else {
        match args.profile {
            Profile::Tuned => "tuned",
            Profile::Baseline => "baseline",
        }
    };

    // Always print the effective recipe ID so every run is traceable.
    let rid = k8dnz_core::recipe::format::recipe_id_hex(&recipe);
    eprintln!(
        "recipe_id={} version={} seed={} profile={} qshift={} qmin={} qmax={} clamp_min={} clamp_max={} mode={:?} fmt={:?}",
        rid,
        recipe.version,
        recipe.seed,
        profile_label,
        recipe.quant.shift,
        recipe.quant.min,
        recipe.quant.max,
        recipe.field_clamp.min,
        recipe.field_clamp.max,
        args.mode,
        args.fmt
    );

    if args.qsearch {
        return run_qsearch(args, recipe);
    }

    if let Some(path) = args.save_recipe.as_deref() {
        recipe_file::save_k8r(path, &recipe)?;
        eprintln!("saved recipe: {} (recipe_id={})", path, rid);
    }

    // Normal sim path.
    let mut engine = Engine::new(recipe.clone())?;

    // Pair stream (and optionally fields)
    let fr_opt: Option<FieldRangeStats>;
    let toks: Vec<PairToken>;
    let fields: Option<Vec<(PairToken, EmissionField)>>;

    if args.mode == SimMode::Rgbpair && args.rgb_from_field {
        // We need token + emission field samples.
        let pairs = engine.run_emissions_with_fields(args.emissions, args.max_ticks);
        toks = pairs.iter().map(|(t, _)| *t).collect();
        fields = Some(pairs);
        fr_opt = None;
    } else {
        fields = None;
        let (t, fr) = if args.stats {
            let (t, fr) = engine.run_emissions_with_field_stats(args.emissions, args.max_ticks);
            (t, Some(fr))
        } else {
            (engine.run_emissions(args.emissions, args.max_ticks), None)
        };
        toks = t;
        fr_opt = fr;
    }

    // Output (Pair or Rgbpair)
    write_output(&args, &toks, fields.as_deref(), &recipe)?;

    if args.stats {
        print_stats(&toks, fr_opt.as_ref(), &recipe);
    }

    eprintln!(
        "sim ok: ticks={} alignments={} emissions={}",
        engine.stats.ticks, engine.stats.alignments, engine.stats.emissions
    );

    Ok(())
}

fn parse_rgb_triplet(s: &str) -> anyhow::Result<Rgb> {
    let parts: Vec<&str> = s.split(',').map(|x| x.trim()).collect();
    if parts.len() != 3 {
        anyhow::bail!("invalid RGB triplet '{}', expected r,g,b", s);
    }
    let r: u8 = parts[0].parse()?;
    let g: u8 = parts[1].parse()?;
    let b: u8 = parts[2].parse()?;
    Ok(Rgb::new(r, g, b))
}

fn make_rgb_recipe(args: &SimArgs) -> anyhow::Result<RgbRecipe> {
    let base_a = parse_rgb_triplet(&args.rgb_base_a)?;
    let base_c = parse_rgb_triplet(&args.rgb_base_c)?;

    let backend: u8 = match args.rgb_backend {
        RgbBackend::Cone => 0,
        RgbBackend::Dna => 1,
    };
    let alt_mode: u8 = match args.rgb_alt {
        RgbAlt::None => 0,
        RgbAlt::Parity => 1,
    };

    Ok(RgbRecipe {
        backend,
        alt_mode,
        base_a: [base_a.r, base_a.g, base_a.b],
        base_c: [base_c.r, base_c.g, base_c.b],
        g_step: args.rgb_g_step,
        p_scale: args.rgb_p_scale,
    })
}

fn write_output(
    args: &SimArgs,
    toks: &[PairToken],
    fields: Option<&[(PairToken, EmissionField)]>,
    recipe: &Recipe,
) -> anyhow::Result<()> {
    match args.mode {
        SimMode::Pair => match args.fmt {
            SimOutFmt::Jsonl => {
                if let Some(path) = args.out.as_deref() {
                    jsonl::write_tokens_file(path, toks)?;
                } else {
                    jsonl::write_tokens_stdout(toks)?;
                }
            }
            SimOutFmt::Bin => {
                let Some(path) = args.out.as_deref() else {
                    anyhow::bail!("--fmt bin requires --out <path>");
                };
                bin::write_bytes_file(path, toks)?;
            }
        },

        SimMode::Rgbpair => {
            let rgb: Vec<RgbPairToken> = if args.rgb_from_field {
                let Some(pairs) = fields else {
                    anyhow::bail!("internal error: rgb_from_field requires fields");
                };

                let cfg = make_rgb_recipe(args)?;
                // Use a deterministic spread scale; matches prior intent.
                let spread = (recipe.quant.max - recipe.quant.min).abs().max(1);

                pairs
                    .iter()
                    .enumerate()
                    .map(|(i, (_t, ef))| {
                        emit_rgbpair_from_fields(&cfg, i as u64, ef.clamped_a, ef.clamped_c, spread)
                    })
                    .collect()
            } else {
                // Back-compat / MVP: palette16 mapping
                toks.iter().copied().map(|p| p.to_rgb_pair()).collect()
            };

            match args.fmt {
                SimOutFmt::Jsonl => {
                    if let Some(path) = args.out.as_deref() {
                        jsonl::write_rgbpairs_file(path, &rgb)?;
                    } else {
                        jsonl::write_rgbpairs_stdout(&rgb)?;
                    }
                }
                SimOutFmt::Bin => {
                    let Some(path) = args.out.as_deref() else {
                        anyhow::bail!("--fmt bin requires --out <path>");
                    };
                    bin::write_rgbpairs_file(path, &rgb)?;
                }
            }
        }
    }
    Ok(())
}

// ---- everything below this line is unchanged from your current sim.rs ----

#[derive(Clone, Debug)]
struct Metrics {
    distinct_bytes: usize,
    entropy_byte: f64,
    peak_nibble: u64,
    ticks: u64,
}

fn run_qsearch(args: SimArgs, base_recipe: Recipe) -> anyhow::Result<()> {
    let mut n = args.qsearch_candidates;
    if n < 1 {
        n = 1;
    }
    // force odd so there's a center candidate
    if n % 2 == 0 {
        n += 1;
    }
    let half = (n / 2) as i64;

    let width: i64 = base_recipe.quant.max - base_recipe.quant.min; // >0 guaranteed by guard above
    let default_step: i64 = (width / 32).max(1);
    let step: i64 = args.qsearch_step.unwrap_or(default_step);

    let base_shift: i64 = base_recipe.quant.shift;

    let per_emissions = args.qsearch_emissions;

    // IMPORTANT: do NOT inherit the user's main --max-ticks by default.
    // qsearch is meant to be fast + directional; validate top results with a full sim.
    let per_max_ticks = args.qsearch_max_ticks.unwrap_or(20_000_000);

    eprintln!("--- sim --qsearch ---");
    eprintln!(
        "base shift={} width={} step={} candidates={} (per-candidate emissions={} max_ticks={})",
        base_shift, width, step, n, per_emissions, per_max_ticks
    );

    if per_emissions >= 10_000 || per_max_ticks >= 80_000_000 {
        eprintln!(
            "note: large qsearch settings can take a long time ({} emissions, {} max_ticks per candidate).",
            per_emissions, per_max_ticks
        );
    }

    let t0 = Instant::now();

    let mut rows: Vec<(i64, Metrics, String)> = Vec::with_capacity(n);

    for idx in 0..n {
        let offset = (idx as i64) - half;
        let shift = base_shift.saturating_add(offset.saturating_mul(step));

        let mut r = base_recipe.clone();
        r.quant.shift = shift;

        let rid = k8dnz_core::recipe::format::recipe_id_hex(&r);

        let start = Instant::now();
        let mut e = Engine::new(r.clone())?;
        let toks = e.run_emissions(per_emissions, per_max_ticks);
        let m = compute_metrics(&toks, e.stats.ticks);

        eprintln!(
            "cand {}/{} shift={} recipe_id={} -> distinct={}/256 entropy_byte={:.4} peak_nibble={} ticks={} elapsed_ms={}",
            idx + 1,
            n,
            shift,
            rid,
            m.distinct_bytes,
            m.entropy_byte,
            m.peak_nibble,
            m.ticks,
            start.elapsed().as_millis()
        );

        rows.push((shift, m, rid));
    }

    // Rank: primary entropy_byte (desc), then distinct_bytes (desc), then lower peak_nibble (asc)
    rows.sort_by(|a, b| {
        b.1.entropy_byte
            .partial_cmp(&a.1.entropy_byte)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| b.1.distinct_bytes.cmp(&a.1.distinct_bytes))
            .then_with(|| a.1.peak_nibble.cmp(&b.1.peak_nibble))
    });

    eprintln!("--- qsearch ranking (top 9) ---");
    for (rank, (shift, m, rid)) in rows.iter().enumerate() {
        eprintln!(
            "#{:>2} shift={} recipe_id={} entropy_byte={:.4} distinct={}/256 peak_nibble={} ticks={}",
            rank + 1,
            shift,
            rid,
            m.entropy_byte,
            m.distinct_bytes,
            m.peak_nibble,
            m.ticks
        );
        if rank >= 9 {
            break;
        }
    }

    let (best_shift, _best_m, best_rid) = rows[0].clone();
    eprintln!(
        "best shift={} (base {}) recipe_id={} total_elapsed_ms={}",
        best_shift,
        base_shift,
        best_rid,
        t0.elapsed().as_millis()
    );

    let mut best_recipe = base_recipe.clone();
    best_recipe.quant.shift = best_shift;

    if let Some(path) = args.save_recipe.as_deref() {
        recipe_file::save_k8r(path, &best_recipe)?;
        eprintln!(
            "qsearch saved best recipe: recipe={} shift={} recipe_id={}",
            path, best_shift, best_rid
        );
    }

    // Optional: if user set --out, emit ONE run using the best shift with the user's normal emissions/max_ticks.
    if let Some(path) = args.out.as_deref() {
        let mut e = Engine::new(best_recipe.clone())?;
        let toks = e.run_emissions(args.emissions, args.max_ticks);

        match args.mode {
            SimMode::Pair => match args.fmt {
                SimOutFmt::Jsonl => jsonl::write_tokens_file(path, &toks)?,
                SimOutFmt::Bin => bin::write_bytes_file(path, &toks)?,
            },
            SimMode::Rgbpair => {
                let rgb: Vec<RgbPairToken> =
                    toks.iter().copied().map(|p| p.to_rgb_pair()).collect();
                match args.fmt {
                    SimOutFmt::Jsonl => jsonl::write_rgbpairs_file(path, &rgb)?,
                    SimOutFmt::Bin => bin::write_rgbpairs_file(path, &rgb)?,
                }
            }
        }

        eprintln!(
            "qsearch wrote best output: out={} shift={} recipe_id={} ticks={} emissions={}",
            path, best_shift, best_rid, e.stats.ticks, e.stats.emissions
        );
    } else {
        eprintln!("(no --out provided; qsearch did not write token output)");
    }

    Ok(())
}

fn compute_metrics(toks: &[PairToken], ticks: u64) -> Metrics {
    let mut ha = [0u64; 16];
    let mut hb = [0u64; 16];
    let mut hbyte = [0u64; 256];

    for t in toks {
        let a = (t.a & 0x0F) as usize;
        let b = (t.b & 0x0F) as usize;
        ha[a] += 1;
        hb[b] += 1;
        let byte = (((a as u8) & 0x0F) << 4) | ((b as u8) & 0x0F);
        hbyte[byte as usize] += 1;
    }

    let total = toks.len() as u64;
    let distinct_bytes = hbyte.iter().filter(|&&c| c > 0).count();
    let entropy_byte = entropy_bits_256(&hbyte, total);

    let peak_nibble = ha
        .iter()
        .copied()
        .max()
        .unwrap_or(0)
        .max(hb.iter().copied().max().unwrap_or(0));

    Metrics {
        distinct_bytes,
        entropy_byte,
        peak_nibble,
        ticks,
    }
}

fn print_stats(toks: &[PairToken], fr: Option<&FieldRangeStats>, recipe: &Recipe) {
    let mut ha = [0u64; 16];
    let mut hb = [0u64; 16];
    let mut hbyte = [0u64; 256];

    for t in toks {
        let a = (t.a & 0x0F) as usize;
        let b = (t.b & 0x0F) as usize;
        ha[a] += 1;
        hb[b] += 1;
        let byte = (((a as u8) & 0x0F) << 4) | ((b as u8) & 0x0F);
        hbyte[byte as usize] += 1;
    }

    let total = toks.len() as u64;
    let distinct_bytes = hbyte.iter().filter(|&&c| c > 0).count();

    let (min_a, max_a) = min_max_16(&ha);
    let (min_b, max_b) = min_max_16(&hb);
    let (min_byte, max_byte) = min_max_256(&hbyte);

    let h_a = entropy_bits_16(&ha, total);
    let h_b = entropy_bits_16(&hb, total);
    let h_byte = entropy_bits_256(&hbyte, total);

    let qmin_eff = recipe.quant.min.saturating_add(recipe.quant.shift);
    let qmax_eff = recipe.quant.max.saturating_add(recipe.quant.shift);

    eprintln!("--- sim --stats ---");
    eprintln!("pairs: {}", total);
    eprintln!("distinct packed bytes: {}/256", distinct_bytes);
    eprintln!("A histogram min/max: {}/{}", min_a, max_a);
    eprintln!("B histogram min/max: {}/{}", min_b, max_b);
    eprintln!(
        "BYTE histogram min/max (over 256): {}/{}",
        min_byte, max_byte
    );
    eprintln!("entropy: A={:.4} bits (max 4.0000)", h_a);
    eprintln!("entropy: B={:.4} bits (max 4.0000)", h_b);
    eprintln!("entropy: BYTE={:.4} bits (max 8.0000)", h_byte);

    if let Some(fr) = fr {
        if fr.saw_any {
            eprintln!(
                "field samples (raw):   min={} max={}",
                fr.raw_min, fr.raw_max
            );
            eprintln!(
                "field samples (clamp): min={} max={}",
                fr.clamped_min, fr.clamped_max
            );
            eprintln!(
                "quant range (recipe):  min={} max={} shift={} (effective min={} max={})",
                recipe.quant.min, recipe.quant.max, recipe.quant.shift, qmin_eff, qmax_eff
            );
            eprintln!(
                "clamp range (recipe):  min={} max={}",
                recipe.field_clamp.min, recipe.field_clamp.max
            );
        }
    }

    eprintln!("A counts (0..15): {:?}", ha);
    eprintln!("B counts (0..15): {:?}", hb);
}

fn min_max_16(h: &[u64; 16]) -> (u64, u64) {
    let mut min = u64::MAX;
    let mut max = 0u64;
    for &v in h.iter() {
        if v < min {
            min = v;
        }
        if v > max {
            max = v;
        }
    }
    (min, max)
}

fn min_max_256(h: &[u64; 256]) -> (u64, u64) {
    let mut min = u64::MAX;
    let mut max = 0u64;
    for &v in h.iter() {
        if v < min {
            min = v;
        }
        if v > max {
            max = v;
        }
    }
    (min, max)
}

fn entropy_bits_16(h: &[u64; 16], total: u64) -> f64 {
    if total == 0 {
        return 0.0;
    }
    let mut ent = 0.0;
    for &c in h.iter() {
        if c == 0 {
            continue;
        }
        let p = (c as f64) / (total as f64);
        ent -= p * p.log2();
    }
    ent
}

fn entropy_bits_256(h: &[u64; 256], total: u64) -> f64 {
    if total == 0 {
        return 0.0;
    }
    let mut ent = 0.0;
    for &c in h.iter() {
        if c == 0 {
            continue;
        }
        let p = (c as f64) / (total as f64);
        ent -= p * p.log2();
    }
    ent
}
