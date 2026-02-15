// crates/k8dnz-cli/src/cmd/tune.rs
//
// Tune = shift search / refinement (existing behavior) + optional "fit + residual" MVP.
//
// Back-compat preserved:
// - still writes a tuned .k8r via --out-recipe (required)
// - still supports measure_field / set_clamp_from_field
// - still supports passes / step_div refinement
// - still supports validate_best
//
// NEW (optional):
// - --fit-in <path> + --out-ark <path> => write a .ark whose data is RESIDUAL = plaintext XOR model_stream
// - --fit-by-residual => rank candidate shifts by residual metrics (enhanced proxies)
// - --rank-by-effective-zstd => rank candidate shifts by TRUE effective size:
//       effective_bytes = recipe_bytes + zstd(residual) at --zstd-level
// - --zstd-level <n> sets zstd compression level for effective ranking (default 3)
// - --keystream-mix none|splitmix64 stored in recipe + used for model stream generation
// - dumps: --dump-residual / --dump-model / --dump-raw-model (work with or without --out-ark)
// - per-pass dumps (optional): --dump-residual-pass / --dump-model-pass / --dump-raw-model-pass
//   Pattern supports "%d" for 1-based pass index, e.g. "/tmp/res_pass_%d.bin".
//
// NOTE:
// - "model_stream" here is the cadence keystream bytes (optionally mixed).
// - payload_kind is set to ResidualXor when writing --out-ark, so decode knows how to reconstruct.
//
// FIXES (2026-02-13):
// - Bound quant.shift during candidate generation (prevents degenerate all-zero keystream recipes).
// - Health-check penalty on model keystream distribution (reject/penalize dead streams even if residual ranks well).

use clap::{Args, ValueEnum};
use k8dnz_core::dynamics::engine::FieldRangeStats;
use k8dnz_core::recipe::format as recipe_format;
use k8dnz_core::recipe::recipe::{KeystreamMix, PayloadKind};
use k8dnz_core::signal::token::PairToken;
use k8dnz_core::{Engine, Recipe};

use crate::io::{ark, recipe_file};

use std::time::Instant;

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum KeystreamMixArg {
    None,
    Splitmix64,
}
impl KeystreamMixArg {
    fn to_core(self) -> KeystreamMix {
        match self {
            KeystreamMixArg::None => KeystreamMix::None,
            KeystreamMixArg::Splitmix64 => KeystreamMix::SplitMix64,
        }
    }
}

#[derive(Args, Debug)]
pub struct TuneArgs {
    /// Base recipe path (.k8r). If omitted, uses built-in default recipe.
    #[arg(long)]
    pub recipe: Option<String>,

    /// Output tuned recipe path (.k8r). REQUIRED.
    #[arg(long)]
    pub out_recipe: String,

    /// Optional text report path (human-readable)
    #[arg(long)]
    pub report: Option<String>,

    // --- Optional overrides applied BEFORE tuning (explicit = deterministic) ---

    /// Override quant min (i64)
    #[arg(long)]
    pub qmin: Option<i64>,

    /// Override quant max (i64)
    #[arg(long)]
    pub qmax: Option<i64>,

    /// Override quant shift seed (starting point for search). If omitted, uses recipe.quant.shift.
    #[arg(long)]
    pub qshift: Option<i64>,

    /// Override clamp min (i64)
    #[arg(long)]
    pub clamp_min: Option<i64>,

    /// Override clamp max (i64)
    #[arg(long)]
    pub clamp_max: Option<i64>,

    // --- Field measurement (optional) ---

    /// Measure field min/max via a sampling run and print observed ranges.
    /// This does NOT change the recipe unless you also pass --set-clamp-from-field.
    #[arg(long, default_value_t = false)]
    pub measure_field: bool,

    /// Emissions used for field measurement (only if --measure-field)
    #[arg(long, default_value_t = 5_000)]
    pub measure_emissions: u64,

    /// Max ticks for field measurement (only if --measure-field)
    #[arg(long, default_value_t = 20_000_000)]
    pub measure_max_ticks: u64,

    /// If set, will set field_clamp to the measured raw min/max
    /// (only meaningful with --measure-field).
    #[arg(long, default_value_t = false)]
    pub set_clamp_from_field: bool,

    // --- Shift search / refinement ---

    /// Number of candidate shifts to evaluate per pass (forced odd; center is base shift).
    #[arg(long, default_value_t = 9)]
    pub candidates: usize,

    /// Explicit step size for candidate shifts (SINGLE-PASS ONLY).
    /// If you use --passes > 1 or --step-div, step is derived from width/div.
    #[arg(long)]
    pub step: Option<i64>,

    /// Per-candidate emissions (keep small; tuning is directional).
    /// Used when ranking by token distribution (default mode).
    #[arg(long, default_value_t = 2_000)]
    pub per_emissions: u64,

    /// Per-candidate max ticks.
    /// Also used as max ticks when ranking by residual (fit-by-residual).
    #[arg(long, default_value_t = 20_000_000)]
    pub per_max_ticks: u64,

    /// Number of refinement passes. If > 1 and --step-div is not provided,
    /// we use divisors: 32, 256, 2048, 16384, ... (x8 each pass).
    #[arg(long, default_value_t = 1)]
    pub passes: usize,

    /// Comma-separated list of width divisors per pass, e.g. "32,256,2048".
    /// Each pass uses step = width/divisor (min 1).
    /// If provided, it overrides --passes (passes = list length).
    #[arg(long)]
    pub step_div: Option<String>,

    // --- Optional validation run for the final best candidate ---

    /// Optional validation run for the best candidate after all passes
    /// (more emissions, bigger max ticks).
    #[arg(long, default_value_t = false)]
    pub validate_best: bool,

    /// Emissions for validation run (only if --validate-best)
    #[arg(long, default_value_t = 20_000)]
    pub validate_emissions: u64,

    /// Max ticks for validation run (only if --validate-best)
    #[arg(long, default_value_t = 80_000_000)]
    pub validate_max_ticks: u64,

    // --- Fit + residual (optional) ---

    /// If set, tuner will load these bytes and (optionally) rank shifts by residual compressibility.
    #[arg(long)]
    pub fit_in: Option<String>,

    /// If set, writes a .ark where data_bytes = RESIDUAL (plaintext XOR model_stream).
    /// Requires --fit-in.
    #[arg(long)]
    pub out_ark: Option<String>,

    /// If set, ranks candidates by residual proxy metrics instead of token entropy.
    /// Requires --fit-in.
    #[arg(long, default_value_t = false)]
    pub fit_by_residual: bool,

    /// NEW: rank candidates by *true* effective size:
    /// effective_bytes = recipe_bytes_len + zstd(residual)_len at --zstd-level.
    /// Requires --fit-in. Implies residual-based evaluation.
    #[arg(long, default_value_t = false)]
    pub rank_by_effective_zstd: bool,

    /// zstd compression level for --rank-by-effective-zstd (and reporting).
    #[arg(long, default_value_t = 3)]
    pub zstd_level: i32,

    /// Keystream mixing used when generating model stream for fit/residual.
    /// Stored in the tuned recipe (and used when writing out_ark).
    #[arg(long, value_enum, default_value_t = KeystreamMixArg::None)]
    pub keystream_mix: KeystreamMixArg,

    /// Dump residual bytes (requires --fit-in). If --out-ark is present, dump is aligned to that output too.
    #[arg(long)]
    pub dump_residual: Option<String>,

    /// Dump model bytes used (mixed, if mix enabled) (requires --fit-in).
    #[arg(long)]
    pub dump_model: Option<String>,

    /// Dump raw cadence model bytes (pre-mix) (requires --fit-in).
    #[arg(long)]
    pub dump_raw_model: Option<String>,

    // --- NEW: per-pass dumps (optional) ---

    /// Dump residual for the best candidate of EACH pass.
    /// Pattern supports "%d" for 1-based pass index, e.g. "/tmp/res_pass_%d.bin".
    /// Requires --fit-in.
    #[arg(long)]
    pub dump_residual_pass: Option<String>,

    /// Dump model-used bytes for the best candidate of EACH pass.
    /// Pattern supports "%d" for 1-based pass index.
    /// Requires --fit-in.
    #[arg(long)]
    pub dump_model_pass: Option<String>,

    /// Dump raw cadence model bytes (pre-mix) for the best candidate of EACH pass.
    /// Pattern supports "%d" for 1-based pass index.
    /// Requires --fit-in.
    #[arg(long)]
    pub dump_raw_model_pass: Option<String>,
}

#[derive(Clone, Debug)]
struct Metrics {
    distinct_bytes: usize,
    entropy_byte: f64,
    peak_nibble: u64,
    ticks: u64,
}

#[derive(Clone, Debug)]
struct ResidualMetrics {
    distinct_bytes: usize,
    entropy_byte: f64,
    peak_byte: u64,
    zero_rate: f64,
    printable_rate: f64,
    top16_mass: f64,

    // NEW: real-world size metrics
    zstd_bytes: usize,
    recipe_bytes: usize,
    effective_bytes: usize,

    // NEW: model keystream health (so we can reject dead streams even if residual ranks well)
    model_distinct_bytes: usize,
    model_entropy_byte: f64,

    ticks: u64,
}

#[derive(Clone, Debug)]
struct ByteSummary {
    distinct_bytes: usize,
    peak: u64,
    entropy_byte: f64,
    zero_rate: f64,
    printable_rate: f64,
    top16_mass: f64,
}

const KEYSTREAM_DEAD_DISTINCT_MAX: usize = 2;
const KEYSTREAM_DEAD_ENTROPY_MAX: f64 = 0.50;

/// Bound candidate shifts into a safe deterministic window so the tuner cannot generate dead recipes.
/// We bind to +/- width where width = quant.max - quant.min.
fn clamp_shift_to_width(shift: i64, width: i64) -> i64 {
    if width <= 0 {
        return 0;
    }
    let lo = -width;
    let hi = width;
    shift.clamp(lo, hi)
}

/// Health check: returns true if the model keystream looks dead / near-dead.
fn keystream_is_dead(model: &ByteSummary) -> bool {
    model.distinct_bytes <= KEYSTREAM_DEAD_DISTINCT_MAX || model.entropy_byte <= KEYSTREAM_DEAD_ENTROPY_MAX
}

pub fn run(args: TuneArgs) -> anyhow::Result<()> {
    let mut recipe: Recipe = if let Some(path) = args.recipe.as_deref() {
        recipe_file::load_k8r(path)?
    } else {
        k8dnz_core::recipe::defaults::default_recipe()
    };

    // Apply deterministic overrides (explicit inputs).
    if let Some(v) = args.qmin {
        recipe.quant.min = v;
    }
    if let Some(v) = args.qmax {
        recipe.quant.max = v;
    }
    if let Some(v) = args.qshift {
        recipe.quant.shift = v;
    }
    if let Some(v) = args.clamp_min {
        recipe.field_clamp.min = v;
    }
    if let Some(v) = args.clamp_max {
        recipe.field_clamp.max = v;
    }

    // Apply requested keystream mix (matters for fit/residual; harmless otherwise).
    recipe.keystream_mix = args.keystream_mix.to_core();

    // Guards.
    if recipe.quant.min >= recipe.quant.max {
        anyhow::bail!(
            "invalid quant range: min={} max={} (need min < max)",
            recipe.quant.min,
            recipe.quant.max
        );
    }
    if recipe.field_clamp.min >= recipe.field_clamp.max {
        anyhow::bail!(
            "invalid clamp range: min={} max={} (need min < max)",
            recipe.field_clamp.min,
            recipe.field_clamp.max
        );
    }

    // Ensure base shift is also bounded (deterministic safety rail).
    let width0: i64 = recipe.quant.max - recipe.quant.min;
    let bounded0 = clamp_shift_to_width(recipe.quant.shift, width0);
    if bounded0 != recipe.quant.shift {
        eprintln!(
            "WARN: base quant.shift clamped from {} to {} (width={})",
            recipe.quant.shift, bounded0, width0
        );
        recipe.quant.shift = bounded0;
    }

    // Fit input (optional)
    let fit_bytes: Option<Vec<u8>> = if let Some(p) = args.fit_in.as_deref() {
        Some(std::fs::read(p)?)
    } else {
        None
    };

    let wants_any_fit_dump = args.dump_residual.is_some()
        || args.dump_model.is_some()
        || args.dump_raw_model.is_some()
        || args.dump_residual_pass.is_some()
        || args.dump_model_pass.is_some()
        || args.dump_raw_model_pass.is_some();

    if args.rank_by_effective_zstd && fit_bytes.is_none() {
        anyhow::bail!("--rank-by-effective-zstd requires --fit-in <path>");
    }

    let effective_implies_residual = args.rank_by_effective_zstd;

    if (args.fit_by_residual || effective_implies_residual) && fit_bytes.is_none() {
        anyhow::bail!("--fit-by-residual/--rank-by-effective-zstd requires --fit-in <path>");
    }
    if args.out_ark.is_some() && fit_bytes.is_none() {
        anyhow::bail!("--out-ark requires --fit-in <path>");
    }
    if wants_any_fit_dump && fit_bytes.is_none() {
        anyhow::bail!("--dump-* requires --fit-in <path>");
    }

    let base_rid = k8dnz_core::recipe::format::recipe_id_hex(&recipe);

    let mut report_lines: Vec<String> = Vec::new();
    report_lines.push("--- k8dnz tune report ---".to_string());
    report_lines.push(format!("base_recipe_id = {}", base_rid));
    report_lines.push(format!(
        "base_quant = min={} max={} shift={}",
        recipe.quant.min, recipe.quant.max, recipe.quant.shift
    ));
    report_lines.push(format!(
        "base_clamp = min={} max={}",
        recipe.field_clamp.min, recipe.field_clamp.max
    ));
    report_lines.push(format!("keystream_mix = {:?}", recipe.keystream_mix));
    report_lines.push(format!("fit_in = {:?}", args.fit_in));
    report_lines.push(format!("fit_by_residual = {}", args.fit_by_residual));
    report_lines.push(format!("rank_by_effective_zstd = {}", args.rank_by_effective_zstd));
    report_lines.push(format!("zstd_level = {}", args.zstd_level));
    report_lines.push(format!("dump_residual_pass = {:?}", args.dump_residual_pass));
    report_lines.push(format!("dump_model_pass = {:?}", args.dump_model_pass));
    report_lines.push(format!("dump_raw_model_pass = {:?}", args.dump_raw_model_pass));
    report_lines.push("".to_string());

    eprintln!("--- tune ---");
    eprintln!("base_recipe_id = {}", base_rid);
    eprintln!("keystream_mix = {:?}", recipe.keystream_mix);
    if let Some(p) = args.fit_in.as_deref() {
        eprintln!("fit_in = {} ({} bytes)", p, fit_bytes.as_ref().unwrap().len());
    }

    if args.rank_by_effective_zstd {
        eprintln!(
            "ranking mode = EFFECTIVE_ZSTD (effective_bytes = recipe_bytes + zstd(residual) @ level {})",
            args.zstd_level
        );
    } else if args.fit_by_residual {
        eprintln!("ranking mode = residual proxy metrics (top16_mass/zero_rate/entropy/distinct/peak)");
    } else {
        eprintln!("ranking mode = token metrics (entropy/distinct/peak_nibble)");
    }

    // Optional field measurement pass.
    if args.measure_field {
        let mut e = Engine::new(recipe.clone())?;
        let (_toks, fr): (Vec<PairToken>, FieldRangeStats) =
            e.run_emissions_with_field_stats(args.measure_emissions, args.measure_max_ticks);

        if fr.saw_any {
            eprintln!(
                "field_measured raw_min={} raw_max={} clamped_min={} clamped_max={}",
                fr.raw_min, fr.raw_max, fr.clamped_min, fr.clamped_max
            );
            report_lines.push(format!(
                "field_measured raw_min={} raw_max={} clamped_min={} clamped_max={}",
                fr.raw_min, fr.raw_max, fr.clamped_min, fr.clamped_max
            ));

            if args.set_clamp_from_field {
                recipe.field_clamp.min = fr.raw_min;
                recipe.field_clamp.max = fr.raw_max;
                eprintln!(
                    "set_clamp_from_field => clamp=[{}, {}]",
                    recipe.field_clamp.min, recipe.field_clamp.max
                );
                report_lines.push(format!(
                    "set_clamp_from_field => clamp=[{}, {}]",
                    recipe.field_clamp.min, recipe.field_clamp.max
                ));
            }
        } else {
            eprintln!("field_measured: no samples observed (unexpected)");
            report_lines.push("field_measured: no samples observed (unexpected)".to_string());
        }
        report_lines.push("".to_string());
    }

    // Multi-pass shift search / refinement.
    let (
        best_recipe,
        best_shift,
        best_metrics_opt,
        best_rmetrics_opt,
        per_pass_rankings,
        elapsed_ms,
    ) = tune_shift_multipass(&args, recipe, fit_bytes.as_deref())?;

    // Final safety rail: ensure the chosen recipe doesn't have a dead keystream.
    // We only need this check when fit/residual features are used, because residual ranking can
    // mistakenly favor dead keystreams (e.g., all-zero keystream => residual==plaintext).
    if let Some(plain) = fit_bytes.as_deref() {
        let mut e = Engine::new(best_recipe.clone())?;
        let model_used = ark::keystream_bytes(&mut e, plain.len().min(4096), args.per_max_ticks)?;
        let model_sum = byte_summary(&model_used);
        if keystream_is_dead(&model_sum) {
            anyhow::bail!(
                "tune produced dead keystream for best candidate: distinct={}/256 entropy={:.4}. Refusing to write out recipe. (shift={}, recipe_id={})",
                model_sum.distinct_bytes,
                model_sum.entropy_byte,
                best_shift,
                k8dnz_core::recipe::format::recipe_id_hex(&best_recipe)
            );
        }
    }

    let best_rid = k8dnz_core::recipe::format::recipe_id_hex(&best_recipe);

    // Save tuned recipe (required).
    recipe_file::save_k8r(&args.out_recipe, &best_recipe)?;
    eprintln!(
        "saved tuned recipe: {} (shift={} recipe_id={})",
        args.out_recipe, best_shift, best_rid
    );

    report_lines.push(format!("best_shift = {}", best_shift));
    report_lines.push(format!("best_recipe_id = {}", best_rid));

    if let Some(m) = best_metrics_opt.as_ref() {
        report_lines.push(format!(
            "best_token_metrics distinct={}/256 entropy_byte={:.4} peak_nibble={} ticks={}",
            m.distinct_bytes, m.entropy_byte, m.peak_nibble, m.ticks
        ));
    }
    if let Some(m) = best_rmetrics_opt.as_ref() {
        report_lines.push(format!(
            "best_residual_metrics effective_bytes={} (recipe_bytes={} + zstd_bytes={}) model_distinct={}/256 model_entropy={:.4} top16_mass={:.4} zero_rate={:.4} printable_rate={:.4} entropy_byte={:.4} distinct={}/256 peak_byte={} ticks={}",
            m.effective_bytes,
            m.recipe_bytes,
            m.zstd_bytes,
            m.model_distinct_bytes,
            m.model_entropy_byte,
            m.top16_mass,
            m.zero_rate,
            m.printable_rate,
            m.entropy_byte,
            m.distinct_bytes,
            m.peak_byte,
            m.ticks
        ));
    }
    report_lines.push(format!("elapsed_ms = {}", elapsed_ms));
    report_lines.push("".to_string());

    // Per-pass report.
    for (pass_idx, (div_opt, rows_token_opt, rows_resid_opt)) in per_pass_rankings.iter().enumerate() {
        report_lines.push(format!("--- pass {} ---", pass_idx + 1));
        if let Some(div) = div_opt {
            report_lines.push(format!("step_div = {}", div));
        } else {
            report_lines.push("step_div = (explicit step)".to_string());
        }

        if let Some(rows) = rows_token_opt.as_ref() {
            report_lines.push("ranking = token_metrics".to_string());
            for (rank, (shift, m, rid)) in rows.iter().take(9).enumerate() {
                report_lines.push(format!(
                    "#{:>2} shift={} recipe_id={} entropy_byte={:.4} distinct={}/256 peak_nibble={} ticks={}",
                    rank + 1,
                    shift,
                    rid,
                    m.entropy_byte,
                    m.distinct_bytes,
                    m.peak_nibble,
                    m.ticks
                ));
            }
        }

        if let Some(rows) = rows_resid_opt.as_ref() {
            if args.rank_by_effective_zstd {
                report_lines.push(format!("ranking = effective_zstd (zstd_level={})", args.zstd_level));
            } else {
                report_lines.push("ranking = residual_metrics".to_string());
            }
            for (rank, (shift, m, rid)) in rows.iter().take(9).enumerate() {
                report_lines.push(format!(
                    "#{:>2} shift={} recipe_id={} effective_bytes={} (recipe_bytes={} + zstd_bytes={}) model_distinct={}/256 model_entropy={:.4} top16_mass={:.4} zero_rate={:.4} printable_rate={:.4} entropy={:.4} distinct={}/256 peak_byte={} ticks={}",
                    rank + 1,
                    shift,
                    rid,
                    m.effective_bytes,
                    m.recipe_bytes,
                    m.zstd_bytes,
                    m.model_distinct_bytes,
                    m.model_entropy_byte,
                    m.top16_mass,
                    m.zero_rate,
                    m.printable_rate,
                    m.entropy_byte,
                    m.distinct_bytes,
                    m.peak_byte,
                    m.ticks
                ));
            }
        }

        report_lines.push("".to_string());
    }

    // Optional validation run (token stream)
    if args.validate_best {
        let mut e = Engine::new(best_recipe.clone())?;
        let toks = e.run_emissions(args.validate_emissions, args.validate_max_ticks);
        let m = compute_token_metrics(&toks, e.stats.ticks);
        eprintln!(
            "validate_best: emissions={} max_ticks={} -> distinct={}/256 entropy_byte={:.4} peak_nibble={} ticks={}",
            args.validate_emissions,
            args.validate_max_ticks,
            m.distinct_bytes,
            m.entropy_byte,
            m.peak_nibble,
            m.ticks
        );
        report_lines.push("--- validate_best ---".to_string());
        report_lines.push(format!(
            "validate_best: emissions={} max_ticks={} -> distinct={}/256 entropy_byte={:.4} peak_nibble={} ticks={}",
            args.validate_emissions,
            args.validate_max_ticks,
            m.distinct_bytes,
            m.entropy_byte,
            m.peak_nibble,
            m.ticks
        ));
        report_lines.push("".to_string());
    }

    // Optional: dump residual/model even without writing ark (requires fit_in)
    if let Some(plain) = fit_bytes.as_deref() {
        let want_any_dump =
            args.dump_residual.is_some() || args.dump_model.is_some() || args.dump_raw_model.is_some();
        if want_any_dump && args.out_ark.is_none() {
            let mut r = best_recipe.clone();
            r.payload_kind = PayloadKind::ResidualXor;

            let mut engine = Engine::new(r.clone())?;
            let want_raw = args.dump_raw_model.is_some();
            let (model_used, raw_opt) = if want_raw {
                ark::keystream_bytes_with_raw(&mut engine, plain.len(), args.per_max_ticks)?
            } else {
                (ark::keystream_bytes(&mut engine, plain.len(), args.per_max_ticks)?, Vec::new())
            };

            let mut residual = plain.to_vec();
            for (b, k) in residual.iter_mut().zip(model_used.iter()) {
                *b ^= *k;
            }

            if let Some(p) = args.dump_residual.as_deref() {
                std::fs::write(p, &residual)?;
                eprintln!("dumped residual: {} ({} bytes)", p, residual.len());
            }
            if let Some(p) = args.dump_model.as_deref() {
                std::fs::write(p, &model_used)?;
                eprintln!("dumped model: {} ({} bytes)", p, model_used.len());
            }
            if let Some(p) = args.dump_raw_model.as_deref() {
                if !raw_opt.is_empty() {
                    std::fs::write(p, &raw_opt)?;
                    eprintln!("dumped raw model: {} ({} bytes)", p, raw_opt.len());
                } else {
                    eprintln!("dump_raw_model requested but raw model not available (unexpected)");
                }
            }
        }
    }

    // Optional: write residual .ark (model+residual MVP)
    if let (Some(out_ark), Some(plain)) = (args.out_ark.as_deref(), fit_bytes.as_deref()) {
        let mut r = best_recipe.clone();
        r.payload_kind = PayloadKind::ResidualXor;

        let mut engine = Engine::new(r.clone())?;

        let want_raw = args.dump_raw_model.is_some();
        let (model_used, raw_opt) = if want_raw {
            ark::keystream_bytes_with_raw(&mut engine, plain.len(), args.per_max_ticks)?
        } else {
            (ark::keystream_bytes(&mut engine, plain.len(), args.per_max_ticks)?, Vec::new())
        };

        // Final guard: refuse to write ark if model keystream is dead.
        let model_sum = byte_summary(&model_used);
        if keystream_is_dead(&model_sum) {
            anyhow::bail!(
                "refusing to write .ark: dead model keystream (distinct={}/256 entropy={:.4}) shift={} recipe_id={}",
                model_sum.distinct_bytes,
                model_sum.entropy_byte,
                r.quant.shift,
                k8dnz_core::recipe::format::recipe_id_hex(&r)
            );
        }

        let mut residual = plain.to_vec();
        for (b, k) in residual.iter_mut().zip(model_used.iter()) {
            *b ^= *k;
        }

        if let Some(p) = args.dump_residual.as_deref() {
            std::fs::write(p, &residual)?;
            eprintln!("dumped residual: {} ({} bytes)", p, residual.len());
        }
        if let Some(p) = args.dump_model.as_deref() {
            std::fs::write(p, &model_used)?;
            eprintln!("dumped model: {} ({} bytes)", p, model_used.len());
        }
        if let Some(p) = args.dump_raw_model.as_deref() {
            if !raw_opt.is_empty() {
                std::fs::write(p, &raw_opt)?;
                eprintln!("dumped raw model: {} ({} bytes)", p, raw_opt.len());
            }
        }

        ark::write_ark(out_ark, &r, &residual)?;

        // Report effective size as well
        let rb = recipe_format::encode(&r);
        let z = zstd_compress_len(&residual, args.zstd_level);
        let eff = rb.len() + z;

        let m = residual_metrics(&residual);
        eprintln!(
            "wrote residual ark: out={} recipe_id={} ticks={} emissions={} residual: effective_bytes={} (recipe_bytes={} + zstd_bytes={} @ lvl {}) top16_mass={:.4} zero_rate={:.4} printable_rate={:.4} entropy={:.4} distinct={}/256 peak_byte={}",
            out_ark,
            k8dnz_core::recipe::format::recipe_id_hex(&r),
            engine.stats.ticks,
            engine.stats.emissions,
            eff,
            rb.len(),
            z,
            args.zstd_level,
            m.top16_mass,
            m.zero_rate,
            m.printable_rate,
            m.entropy_byte,
            m.distinct_bytes,
            m.peak,
        );

        report_lines.push("--- residual_ark ---".to_string());
        report_lines.push(format!("out_ark = {}", out_ark));
        report_lines.push(format!("payload_kind = {:?}", r.payload_kind));
        report_lines.push(format!("keystream_mix = {:?}", r.keystream_mix));
        report_lines.push(format!(
            "effective_bytes = {} (recipe_bytes={} + zstd_bytes={} @ lvl {})",
            eff,
            rb.len(),
            z,
            args.zstd_level
        ));
        report_lines.push(format!(
            "residual_metrics top16_mass={:.4} zero_rate={:.4} printable_rate={:.4} entropy={:.4} distinct={}/256 peak_byte={}",
            m.top16_mass,
            m.zero_rate,
            m.printable_rate,
            m.entropy_byte,
            m.distinct_bytes,
            m.peak
        ));
        report_lines.push("".to_string());
    }

    // Optional report write.
    if let Some(path) = args.report.as_deref() {
        let text = report_lines.join("\n") + "\n";
        std::fs::write(path, text)?;
        eprintln!("wrote report: {}", path);
    }

    // Final summary.
    eprintln!(
        "tune ok: best_shift={} best_recipe_id={} elapsed_ms={}",
        best_shift, best_rid, elapsed_ms
    );

    Ok(())
}

fn zstd_compress_len(bytes: &[u8], level: i32) -> usize {
    zstd::encode_all(bytes, level).map(|v| v.len()).unwrap_or(usize::MAX)
}

fn expand_pass_pattern(pat: &str, pass_1based: usize) -> String {
    if pat.contains("%d") {
        pat.replace("%d", &pass_1based.to_string())
    } else {
        format!("{pat}.pass{pass_1based}")
    }
}

fn maybe_dump_best_of_pass(
    args: &TuneArgs,
    pass_1based: usize,
    recipe_for_pass_best: &Recipe,
    plain: &[u8],
) -> anyhow::Result<()> {
    let want_any = args.dump_residual_pass.is_some()
        || args.dump_model_pass.is_some()
        || args.dump_raw_model_pass.is_some();

    if !want_any {
        return Ok(());
    }

    let mut r = recipe_for_pass_best.clone();
    r.payload_kind = PayloadKind::ResidualXor;

    let mut engine = Engine::new(r)?;
    let want_raw = args.dump_raw_model_pass.is_some();

    let (model_used, raw_opt) = if want_raw {
        ark::keystream_bytes_with_raw(&mut engine, plain.len(), args.per_max_ticks)?
    } else {
        (ark::keystream_bytes(&mut engine, plain.len(), args.per_max_ticks)?, Vec::new())
    };

    let mut residual = plain.to_vec();
    for (b, k) in residual.iter_mut().zip(model_used.iter()) {
        *b ^= *k;
    }

    if let Some(pat) = args.dump_residual_pass.as_deref() {
        let path = expand_pass_pattern(pat, pass_1based);
        std::fs::write(&path, &residual)?;
        eprintln!(
            "dumped residual(pass {}): {} ({} bytes)",
            pass_1based,
            path,
            residual.len()
        );
    }
    if let Some(pat) = args.dump_model_pass.as_deref() {
        let path = expand_pass_pattern(pat, pass_1based);
        std::fs::write(&path, &model_used)?;
        eprintln!(
            "dumped model(pass {}): {} ({} bytes)",
            pass_1based,
            path,
            model_used.len()
        );
    }
    if let Some(pat) = args.dump_raw_model_pass.as_deref() {
        let path = expand_pass_pattern(pat, pass_1based);
        if !raw_opt.is_empty() {
            std::fs::write(&path, &raw_opt)?;
            eprintln!(
                "dumped raw model(pass {}): {} ({} bytes)",
                pass_1based,
                path,
                raw_opt.len()
            );
        } else {
            eprintln!("dump_raw_model_pass requested but raw model not available (unexpected)");
        }
    }

    Ok(())
}

fn parse_step_div_list(s: &str) -> anyhow::Result<Vec<i64>> {
    let mut out = Vec::new();
    for part in s.split(',') {
        let p = part.trim();
        if p.is_empty() {
            continue;
        }
        let v: i64 = p
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid --step-div entry: {}", p))?;
        if v <= 0 {
            anyhow::bail!("--step-div entries must be > 0 (got {})", v);
        }
        out.push(v);
    }
    if out.is_empty() {
        anyhow::bail!("--step-div provided but no valid entries found");
    }
    Ok(out)
}

type TokenRows = Vec<(i64, Metrics, String)>;
type ResidRows = Vec<(i64, ResidualMetrics, String)>;

fn tune_shift_multipass(
    args: &TuneArgs,
    base_recipe: Recipe,
    fit_plain: Option<&[u8]>,
) -> anyhow::Result<(
    Recipe,
    i64,
    Option<Metrics>,
    Option<ResidualMetrics>,
    Vec<(Option<i64>, Option<TokenRows>, Option<ResidRows>)>,
    u128,
)> {
    let width: i64 = base_recipe.quant.max - base_recipe.quant.min;
    let t0 = Instant::now();

    let (pass_divs, use_explicit_step) = if let Some(s) = args.step_div.as_deref() {
        (Some(parse_step_div_list(s)?), false)
    } else if args.passes > 1 {
        let mut v = Vec::with_capacity(args.passes);
        let mut div: i64 = 32;
        for _ in 0..args.passes {
            v.push(div);
            div = div.saturating_mul(8);
        }
        (Some(v), false)
    } else {
        (None, true)
    };

    let mut current_recipe = base_recipe.clone();
    let mut per_pass_rows: Vec<(Option<i64>, Option<TokenRows>, Option<ResidRows>)> = Vec::new();

    if let Some(divs) = pass_divs {
        for (pass_idx, div) in divs.into_iter().enumerate() {
            let pass_1based = pass_idx + 1;
            let step = (width / div).max(1);
            eprintln!(
                "pass {}/? : derived step = width/{} = {}",
                pass_1based, div, step
            );

            let (
                best_recipe,
                best_shift,
                best_token_m,
                best_resid_m,
                rows_token_opt,
                rows_resid_opt,
            ) = tune_shift_once(args, current_recipe.clone(), Some(div), Some(step), fit_plain)?;

            per_pass_rows.push((Some(div), rows_token_opt, rows_resid_opt));

            if let Some(plain) = fit_plain {
                maybe_dump_best_of_pass(args, pass_1based, &best_recipe, plain)?;
            }

            current_recipe = best_recipe;
            current_recipe.quant.shift = best_shift;

            let _ = best_token_m;
            let _ = best_resid_m;
        }
    } else if use_explicit_step {
        let default_step: i64 = (width / 32).max(1);
        let step: i64 = args.step.unwrap_or(default_step);

        let (best_recipe, _best_shift, best_token_m, best_resid_m, rows_token_opt, rows_resid_opt) =
            tune_shift_once(args, current_recipe.clone(), None, Some(step), fit_plain)?;

        per_pass_rows.push((None, rows_token_opt, rows_resid_opt));

        if let Some(plain) = fit_plain {
            maybe_dump_best_of_pass(args, 1, &best_recipe, plain)?;
        }

        let elapsed_ms = t0.elapsed().as_millis();
        return Ok((
            best_recipe.clone(),
            best_recipe.quant.shift,
            best_token_m,
            best_resid_m,
            per_pass_rows,
            elapsed_ms,
        ));
    }

    if args.fit_by_residual || args.rank_by_effective_zstd {
        let Some(plain) = fit_plain else {
            anyhow::bail!("internal: residual mode but no fit_plain");
        };
        let mut e = Engine::new(current_recipe.clone())?;
        let used = ark::keystream_bytes(&mut e, plain.len(), args.per_max_ticks)?;
        let model_sum = byte_summary(&used);

        let mut residual = plain.to_vec();
        for (b, k) in residual.iter_mut().zip(used.iter()) {
            *b ^= *k;
        }

        let m0 = residual_metrics(&residual);

        let rb = recipe_format::encode(&current_recipe);
        let z = zstd_compress_len(&residual, args.zstd_level);
        let eff = rb.len() + z;

        let best_r = ResidualMetrics {
            distinct_bytes: m0.distinct_bytes,
            entropy_byte: m0.entropy_byte,
            peak_byte: m0.peak,
            zero_rate: m0.zero_rate,
            printable_rate: m0.printable_rate,
            top16_mass: m0.top16_mass,
            zstd_bytes: z,
            recipe_bytes: rb.len(),
            effective_bytes: eff,
            model_distinct_bytes: model_sum.distinct_bytes,
            model_entropy_byte: model_sum.entropy_byte,
            ticks: e.stats.ticks,
        };

        let elapsed_ms = t0.elapsed().as_millis();
        Ok((
            current_recipe.clone(),
            current_recipe.quant.shift,
            None,
            Some(best_r),
            per_pass_rows,
            elapsed_ms,
        ))
    } else {
        let mut e = Engine::new(current_recipe.clone())?;
        let toks = e.run_emissions(args.per_emissions, args.per_max_ticks);
        let best_m = compute_token_metrics(&toks, e.stats.ticks);
        let elapsed_ms = t0.elapsed().as_millis();
        Ok((
            current_recipe.clone(),
            current_recipe.quant.shift,
            Some(best_m),
            None,
            per_pass_rows,
            elapsed_ms,
        ))
    }
}

fn tune_shift_once(
    args: &TuneArgs,
    base_recipe: Recipe,
    pass_div: Option<i64>,
    step_override: Option<i64>,
    fit_plain: Option<&[u8]>,
) -> anyhow::Result<(
    Recipe,
    i64,
    Option<Metrics>,
    Option<ResidualMetrics>,
    Option<TokenRows>,
    Option<ResidRows>,
)> {
    let mut n = args.candidates.max(1);
    if n % 2 == 0 {
        n += 1;
    }
    let half = (n / 2) as i64;

    let width: i64 = base_recipe.quant.max - base_recipe.quant.min;
    let default_step: i64 = (width / 32).max(1);
    let step: i64 = step_override.unwrap_or(default_step);

    let base_shift: i64 = base_recipe.quant.shift;

    if let Some(div) = pass_div {
        eprintln!(
            "shift_search(pass div={}): base_shift={} width={} step={} candidates={} per_emissions={} per_max_ticks={}",
            div, base_shift, width, step, n, args.per_emissions, args.per_max_ticks
        );
    } else {
        eprintln!(
            "shift_search: base_shift={} width={} step={} candidates={} per_emissions={} per_max_ticks={}",
            base_shift, width, step, n, args.per_emissions, args.per_max_ticks
        );
    }

    let residual_mode = args.fit_by_residual || args.rank_by_effective_zstd;

    if residual_mode {
        let Some(plain) = fit_plain else {
            anyhow::bail!("internal: residual mode but no fit_plain");
        };

        let mut rows: Vec<(i64, ResidualMetrics, String)> = Vec::with_capacity(n);

        for idx in 0..n {
            let offset = (idx as i64) - half;
            let raw_shift = base_shift.saturating_add(offset.saturating_mul(step));
            let shift = clamp_shift_to_width(raw_shift, width);

            if shift != raw_shift {
                eprintln!(
                    "cand {}/{} raw_shift={} clamped_shift={} (width={})",
                    idx + 1,
                    n,
                    raw_shift,
                    shift,
                    width
                );
            }

            let mut r = base_recipe.clone();
            r.quant.shift = shift;

            let rid = k8dnz_core::recipe::format::recipe_id_hex(&r);

            let start = Instant::now();
            let mut e = Engine::new(r.clone())?;

            let used = match ark::keystream_bytes(&mut e, plain.len(), args.per_max_ticks) {
                Ok(v) => v,
                Err(err) => {
                    eprintln!(
                        "cand {}/{} shift={} recipe_id={} -> residual: FAILED ({})",
                        idx + 1,
                        n,
                        shift,
                        rid,
                        err
                    );
                    rows.push((
                        shift,
                        ResidualMetrics {
                            distinct_bytes: 256,
                            entropy_byte: 8.0,
                            peak_byte: 0,
                            zero_rate: 0.0,
                            printable_rate: 0.0,
                            top16_mass: 0.0,
                            zstd_bytes: usize::MAX,
                            recipe_bytes: recipe_format::encode(&r).len(),
                            effective_bytes: usize::MAX,
                            model_distinct_bytes: 0,
                            model_entropy_byte: 0.0,
                            ticks: e.stats.ticks,
                        },
                        rid,
                    ));
                    continue;
                }
            };

            let model_sum = byte_summary(&used);

            // Health-check: dead keystreams are disallowed.
            if keystream_is_dead(&model_sum) {
                eprintln!(
                    "cand {}/{} shift={} recipe_id={} -> DEAD_KEYSTREAM: model_distinct={}/256 model_entropy={:.4} (penalized)",
                    idx + 1,
                    n,
                    shift,
                    rid,
                    model_sum.distinct_bytes,
                    model_sum.entropy_byte
                );

                rows.push((
                    shift,
                    ResidualMetrics {
                        distinct_bytes: 256,
                        entropy_byte: 8.0,
                        peak_byte: 0,
                        zero_rate: 0.0,
                        printable_rate: 0.0,
                        top16_mass: 0.0,
                        zstd_bytes: usize::MAX,
                        recipe_bytes: recipe_format::encode(&r).len(),
                        effective_bytes: usize::MAX,
                        model_distinct_bytes: model_sum.distinct_bytes,
                        model_entropy_byte: model_sum.entropy_byte,
                        ticks: e.stats.ticks,
                    },
                    rid,
                ));
                continue;
            }

            let mut residual = plain.to_vec();
            for (b, k) in residual.iter_mut().zip(used.iter()) {
                *b ^= *k;
            }

            let m0 = residual_metrics(&residual);

            let rb = recipe_format::encode(&r);
            let z = zstd_compress_len(&residual, args.zstd_level);
            let eff = rb.len().saturating_add(z);

            let m = ResidualMetrics {
                distinct_bytes: m0.distinct_bytes,
                entropy_byte: m0.entropy_byte,
                peak_byte: m0.peak,
                zero_rate: m0.zero_rate,
                printable_rate: m0.printable_rate,
                top16_mass: m0.top16_mass,
                zstd_bytes: z,
                recipe_bytes: rb.len(),
                effective_bytes: eff,
                model_distinct_bytes: model_sum.distinct_bytes,
                model_entropy_byte: model_sum.entropy_byte,
                ticks: e.stats.ticks,
            };

            eprintln!(
                "cand {}/{} shift={} recipe_id={} -> residual: effective_bytes={} (recipe={} + zstd={} @lvl {}) model_distinct={}/256 model_entropy={:.4} top16_mass={:.4} zero_rate={:.4} printable_rate={:.4} entropy={:.4} distinct={}/256 peak_byte={} ticks={} elapsed_ms={}",
                idx + 1,
                n,
                shift,
                rid,
                m.effective_bytes,
                m.recipe_bytes,
                m.zstd_bytes,
                args.zstd_level,
                m.model_distinct_bytes,
                m.model_entropy_byte,
                m.top16_mass,
                m.zero_rate,
                m.printable_rate,
                m.entropy_byte,
                m.distinct_bytes,
                m.peak_byte,
                m.ticks,
                start.elapsed().as_millis()
            );

            rows.push((shift, m, rid));
        }

        if args.rank_by_effective_zstd {
            rows.sort_by(|a, b| {
                a.1.effective_bytes
                    .cmp(&b.1.effective_bytes)
                    .then_with(|| a.1.zstd_bytes.cmp(&b.1.zstd_bytes))
                    .then_with(|| a.1.recipe_bytes.cmp(&b.1.recipe_bytes))
                    .then_with(|| a.0.cmp(&b.0))
            });

            eprintln!(
                "--- tune ranking (EFFECTIVE_ZSTD top 9, zstd_level={}) ---",
                args.zstd_level
            );
            for (rank, (shift, m, rid)) in rows.iter().take(9).enumerate() {
                eprintln!(
                    "#{:>2} shift={} recipe_id={} effective_bytes={} (recipe={} + zstd={}) model_distinct={}/256 model_entropy={:.4} top16_mass={:.4} zero_rate={:.4} entropy={:.4} distinct={}/256 peak_byte={} ticks={}",
                    rank + 1,
                    shift,
                    rid,
                    m.effective_bytes,
                    m.recipe_bytes,
                    m.zstd_bytes,
                    m.model_distinct_bytes,
                    m.model_entropy_byte,
                    m.top16_mass,
                    m.zero_rate,
                    m.entropy_byte,
                    m.distinct_bytes,
                    m.peak_byte,
                    m.ticks
                );
            }
        } else {
            rows.sort_by(|a, b| {
                b.1.top16_mass
                    .partial_cmp(&a.1.top16_mass)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| {
                        b.1.zero_rate
                            .partial_cmp(&a.1.zero_rate)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    })
                    .then_with(|| {
                        a.1.entropy_byte
                            .partial_cmp(&b.1.entropy_byte)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    })
                    .then_with(|| a.1.distinct_bytes.cmp(&b.1.distinct_bytes))
                    .then_with(|| b.1.peak_byte.cmp(&a.1.peak_byte))
                    .then_with(|| a.0.cmp(&b.0))
            });

            eprintln!("--- tune ranking (residual proxy top 9) ---");
            for (rank, (shift, m, rid)) in rows.iter().take(9).enumerate() {
                eprintln!(
                    "#{:>2} shift={} recipe_id={} effective_bytes={} (recipe={} + zstd={}) model_distinct={}/256 model_entropy={:.4} top16_mass={:.4} zero_rate={:.4} printable_rate={:.4} entropy={:.4} distinct={}/256 peak_byte={} ticks={}",
                    rank + 1,
                    shift,
                    rid,
                    m.effective_bytes,
                    m.recipe_bytes,
                    m.zstd_bytes,
                    m.model_distinct_bytes,
                    m.model_entropy_byte,
                    m.top16_mass,
                    m.zero_rate,
                    m.printable_rate,
                    m.entropy_byte,
                    m.distinct_bytes,
                    m.peak_byte,
                    m.ticks
                );
            }
        }

        let (best_shift, best_m, _best_rid) = rows[0].clone();
        let mut best_recipe = base_recipe.clone();
        best_recipe.quant.shift = best_shift;

        Ok((best_recipe, best_shift, None, Some(best_m), None, Some(rows)))
    } else {
        let mut rows: Vec<(i64, Metrics, String)> = Vec::with_capacity(n);

        for idx in 0..n {
            let offset = (idx as i64) - half;
            let raw_shift = base_shift.saturating_add(offset.saturating_mul(step));
            let shift = clamp_shift_to_width(raw_shift, width);

            if shift != raw_shift {
                eprintln!(
                    "cand {}/{} raw_shift={} clamped_shift={} (width={})",
                    idx + 1,
                    n,
                    raw_shift,
                    shift,
                    width
                );
            }

            let mut r = base_recipe.clone();
            r.quant.shift = shift;

            let rid = k8dnz_core::recipe::format::recipe_id_hex(&r);

            let start = Instant::now();
            let mut e = Engine::new(r.clone())?;
            let toks = e.run_emissions(args.per_emissions, args.per_max_ticks);
            let m = compute_token_metrics(&toks, e.stats.ticks);

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

        rows.sort_by(|a, b| {
            b.1.entropy_byte
                .partial_cmp(&a.1.entropy_byte)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| b.1.distinct_bytes.cmp(&a.1.distinct_bytes))
                .then_with(|| a.1.peak_nibble.cmp(&b.1.peak_nibble))
        });

        eprintln!("--- tune ranking (token top 9) ---");
        for (rank, (shift, m, rid)) in rows.iter().take(9).enumerate() {
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
        }

        let (best_shift, best_m, _best_rid) = rows[0].clone();
        let mut best_recipe = base_recipe.clone();
        best_recipe.quant.shift = best_shift;

        Ok((best_recipe, best_shift, Some(best_m), None, Some(rows), None))
    }
}

fn compute_token_metrics(toks: &[PairToken], ticks: u64) -> Metrics {
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

fn residual_metrics(bytes: &[u8]) -> ByteSummary {
    byte_summary(bytes)
}

fn byte_summary(bytes: &[u8]) -> ByteSummary {
    let mut h = [0u64; 256];
    let mut zeros: u64 = 0;
    let mut printable: u64 = 0;

    for &b in bytes {
        h[b as usize] += 1;
        if b == 0 {
            zeros += 1;
        }
        if (0x20..=0x7E).contains(&b) {
            printable += 1;
        }
    }

    let total_u64 = bytes.len() as u64;
    let total_f = bytes.len() as f64;

    let distinct = h.iter().filter(|&&c| c > 0).count();
    let peak = h.iter().copied().max().unwrap_or(0);
    let entropy = entropy_bits_256(&h, total_u64);

    let mut counts: Vec<u64> = h.iter().copied().collect();
    counts.sort_unstable_by(|a, b| b.cmp(a));
    let top16_sum: u64 = counts.into_iter().take(16).sum();

    ByteSummary {
        distinct_bytes: distinct,
        peak,
        entropy_byte: entropy,
        zero_rate: if total_f == 0.0 { 0.0 } else { (zeros as f64) / total_f },
        printable_rate: if total_f == 0.0 { 0.0 } else { (printable as f64) / total_f },
        top16_mass: if total_f == 0.0 { 0.0 } else { (top16_sum as f64) / total_f },
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clamp_shift_respects_width() {
        let width = 100;
        assert_eq!(clamp_shift_to_width(0, width), 0);
        assert_eq!(clamp_shift_to_width(99, width), 99);
        assert_eq!(clamp_shift_to_width(100, width), 100);
        assert_eq!(clamp_shift_to_width(101, width), 100);
        assert_eq!(clamp_shift_to_width(-101, width), -100);
    }

    #[test]
    fn keystream_dead_thresholds_work() {
        let dead = ByteSummary {
            distinct_bytes: 1,
            peak: 100,
            entropy_byte: 0.0,
            zero_rate: 1.0,
            printable_rate: 0.0,
            top16_mass: 1.0,
        };
        assert!(keystream_is_dead(&dead));

        let ok = ByteSummary {
            distinct_bytes: 10,
            peak: 50,
            entropy_byte: 2.0,
            zero_rate: 0.1,
            printable_rate: 0.2,
            top16_mass: 0.5,
        };
        assert!(!keystream_is_dead(&ok));
    }
}
