use clap::Args;
use k8dnz_core::dynamics::engine::FieldRangeStats;
use k8dnz_core::signal::token::PairToken;
use k8dnz_core::{Engine, Recipe};

use crate::io::recipe_file;

use std::time::Instant;

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
    #[arg(long, default_value_t = 2_000)]
    pub per_emissions: u64,

    /// Per-candidate max ticks.
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
}

#[derive(Clone, Debug)]
struct Metrics {
    distinct_bytes: usize,
    entropy_byte: f64,
    peak_nibble: u64,
    ticks: u64,
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

    let base_rid = k8dnz_core::recipe::format::recipe_id_hex(&recipe);

    let mut report_lines: Vec<String> = Vec::new();
    report_lines.push(format!("--- k8dnz tune report ---"));
    report_lines.push(format!("base_recipe_id = {}", base_rid));
    report_lines.push(format!(
        "base_quant = min={} max={} shift={}",
        recipe.quant.min, recipe.quant.max, recipe.quant.shift
    ));
    report_lines.push(format!(
        "base_clamp = min={} max={}",
        recipe.field_clamp.min, recipe.field_clamp.max
    ));

    eprintln!("--- tune ---");
    eprintln!("base_recipe_id = {}", base_rid);

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
    }

    // Multi-pass shift search / refinement.
    let (best_recipe, best_shift, best_metrics, per_pass_rankings, elapsed_ms) =
        tune_shift_multipass(&args, recipe)?;

    let best_rid = k8dnz_core::recipe::format::recipe_id_hex(&best_recipe);

    // Save tuned recipe (required).
    recipe_file::save_k8r(&args.out_recipe, &best_recipe)?;
    eprintln!(
        "saved tuned recipe: {} (shift={} recipe_id={})",
        args.out_recipe, best_shift, best_rid
    );

    report_lines.push(format!("best_shift = {}", best_shift));
    report_lines.push(format!("best_recipe_id = {}", best_rid));
    report_lines.push(format!(
        "best_metrics distinct={}/256 entropy_byte={:.4} peak_nibble={} ticks={}",
        best_metrics.distinct_bytes,
        best_metrics.entropy_byte,
        best_metrics.peak_nibble,
        best_metrics.ticks
    ));
    report_lines.push(format!("elapsed_ms = {}", elapsed_ms));
    report_lines.push("".to_string());

    // Per-pass report.
    for (pass_idx, (div_opt, rows)) in per_pass_rankings.iter().enumerate() {
        report_lines.push(format!("--- pass {} ---", pass_idx + 1));
        if let Some(div) = div_opt {
            report_lines.push(format!("step_div = {}", div));
        } else {
            report_lines.push("step_div = (explicit step)".to_string());
        }
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
        report_lines.push("".to_string());
    }

    // Optional validation run.
    if args.validate_best {
        let mut e = Engine::new(best_recipe.clone())?;
        let toks = e.run_emissions(args.validate_emissions, args.validate_max_ticks);
        let m = compute_metrics(&toks, e.stats.ticks);
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

    // Optional report write.
    if let Some(path) = args.report.as_deref() {
        let text = report_lines.join("\n") + "\n";
        std::fs::write(path, text)?;
        eprintln!("wrote report: {}", path);
    }

    // Final summary.
    eprintln!(
        "tune ok: best_shift={} best_recipe_id={} entropy_byte={:.4} distinct={}/256 elapsed_ms={}",
        best_shift, best_rid, best_metrics.entropy_byte, best_metrics.distinct_bytes, elapsed_ms
    );

    Ok(())
}

fn parse_step_div_list(s: &str) -> anyhow::Result<Vec<i64>> {
    let mut out = Vec::new();
    for part in s.split(',') {
        let p = part.trim();
        if p.is_empty() {
            continue;
        }
        let v: i64 = p.parse().map_err(|_| anyhow::anyhow!("invalid --step-div entry: {}", p))?;
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

fn tune_shift_multipass(
    args: &TuneArgs,
    base_recipe: Recipe,
) -> anyhow::Result<(Recipe, i64, Metrics, Vec<(Option<i64>, Vec<(i64, Metrics, String)>)>, u128)> {
    let width: i64 = base_recipe.quant.max - base_recipe.quant.min;
    let t0 = Instant::now();

    // Decide pass divisors.
    let (pass_divs, use_explicit_step) = if let Some(s) = args.step_div.as_deref() {
        (Some(parse_step_div_list(s)?), false)
    } else if args.passes > 1 {
        // Default refinement schedule: 32, 256, 2048, 16384, ...
        let mut v = Vec::with_capacity(args.passes);
        let mut div: i64 = 32;
        for _ in 0..args.passes {
            v.push(div);
            div = div.saturating_mul(8);
        }
        (Some(v), false)
    } else {
        // Single pass: may use explicit step.
        (None, true)
    };

    let mut current_recipe = base_recipe.clone();
    let mut per_pass_rows: Vec<(Option<i64>, Vec<(i64, Metrics, String)>)> = Vec::new();

    if let Some(divs) = pass_divs {
        // Multi-pass derived steps.
        for (pass_idx, div) in divs.into_iter().enumerate() {
            let step = (width / div).max(1);
            eprintln!(
                "pass {}/? : derived step = width/{} = {}",
                pass_idx + 1,
                div,
                step
            );
            let (best_recipe, best_shift, best_metrics, rows) = tune_shift_once(
                args,
                current_recipe.clone(),
                Some(div),
                Some(step),
            )?;
            per_pass_rows.push((Some(div), rows));
            current_recipe = best_recipe;
            // Keep shift as center for next pass
            current_recipe.quant.shift = best_shift;
            // Small sanity: carry best_metrics forward not required here; final pass returns below
            let _ = best_metrics;
        }
    } else if use_explicit_step {
        // Single pass: explicit step or default width/32.
        let default_step: i64 = (width / 32).max(1);
        let step: i64 = args.step.unwrap_or(default_step);

        let (best_recipe, _best_shift, best_metrics, rows) =
            tune_shift_once(args, current_recipe.clone(), None, Some(step))?;
        per_pass_rows.push((None, rows));

        let elapsed_ms = t0.elapsed().as_millis();
        return Ok((
            best_recipe.clone(),
            best_recipe.quant.shift,
            best_metrics,
            per_pass_rows,
            elapsed_ms,
        ));
    }

    // After multi-pass, do one last metrics summary on the final chosen shift using per_emissions/per_max_ticks
    // to provide a deterministic "best_metrics" aligned with search settings.
    let mut e = Engine::new(current_recipe.clone())?;
    let toks = e.run_emissions(args.per_emissions, args.per_max_ticks);
    let best_metrics = compute_metrics(&toks, e.stats.ticks);

    let elapsed_ms = t0.elapsed().as_millis();
    Ok((
        current_recipe.clone(),
        current_recipe.quant.shift,
        best_metrics,
        per_pass_rows,
        elapsed_ms,
    ))
}

fn tune_shift_once(
    args: &TuneArgs,
    base_recipe: Recipe,
    pass_div: Option<i64>,
    step_override: Option<i64>,
) -> anyhow::Result<(Recipe, i64, Metrics, Vec<(i64, Metrics, String)>)> {
    let mut n = args.candidates;
    if n < 1 {
        n = 1;
    }
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

    let mut rows: Vec<(i64, Metrics, String)> = Vec::with_capacity(n);

    for idx in 0..n {
        let offset = (idx as i64) - half;
        let shift = base_shift.saturating_add(offset.saturating_mul(step));

        let mut r = base_recipe.clone();
        r.quant.shift = shift;

        let rid = k8dnz_core::recipe::format::recipe_id_hex(&r);

        let start = Instant::now();
        let mut e = Engine::new(r.clone())?;
        let toks = e.run_emissions(args.per_emissions, args.per_max_ticks);
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

    // Rank: entropy desc, distinct desc, peak nibble asc
    rows.sort_by(|a, b| {
        b.1.entropy_byte
            .partial_cmp(&a.1.entropy_byte)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| b.1.distinct_bytes.cmp(&a.1.distinct_bytes))
            .then_with(|| a.1.peak_nibble.cmp(&b.1.peak_nibble))
    });

    eprintln!("--- tune ranking (top 9) ---");
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

    Ok((best_recipe, best_shift, best_m, rows))
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
