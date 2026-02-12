use clap::{Args, ValueEnum};
use k8dnz_core::{Engine, Recipe};

use crate::io::{ark, recipe_file};

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

#[derive(Args)]
pub struct EncodeArgs {
    /// Input file to encode (e.g., text/Genesis1.txt)
    #[arg(long)]
    pub r#in: String,

    /// Output .ark path
    #[arg(long)]
    pub out: String,

    /// Recipe path (.k8r). If omitted, uses built-in default recipe.
    #[arg(long)]
    pub recipe: Option<String>,

    /// Convenience profile for qshift selection (only used when --recipe is NOT provided
    /// and --qshift is NOT provided).
    ///
    /// - tuned:    qshift=7141012 (current default behavior)
    /// - baseline: qshift=0
    ///
    /// NOTE: --qshift overrides everything.
    #[arg(long, value_enum, default_value_t = Profile::Tuned)]
    pub profile: Profile,

    /// SIM/encode-only override: shift applied to quant bounds (min/max).
    /// This moves bin boundaries without changing cadence timing or field sampling.
    ///
    /// Example (your current winner):
    ///   --qshift=7141012
    #[arg(long)]
    pub qshift: Option<i64>,

    /// Max ticks guard for keystream generation
    #[arg(long, default_value_t = 50_000_000)]
    pub max_ticks: u64,

    /// Optional: dump the raw cadence keystream bytes (same length as input).
    /// Useful for analyzing keystream quality independent of plaintext.
    #[arg(long)]
    pub dump_keystream: Option<String>,
}

pub fn run(args: EncodeArgs) -> anyhow::Result<()> {
    let plain = std::fs::read(&args.r#in)?;

    let recipe_from_file = args.recipe.is_some();
    let mut recipe: Recipe = if let Some(p) = args.recipe.as_deref() {
        recipe_file::load_k8r(p)?
    } else {
        k8dnz_core::recipe::defaults::default_recipe()
    };

    // Precedence:
    // 1) --qshift (explicit override)
    // 2) if --recipe was provided, keep the shift embedded in the recipe file
    // 3) otherwise, apply --profile convenience shift
    let effective_shift: i64 = if let Some(s) = args.qshift {
        recipe.quant.shift = s;
        s
    } else if recipe_from_file {
        recipe.quant.shift
    } else {
        let s = profile_shift(args.profile);
        recipe.quant.shift = s;
        s
    };

    let rid = k8dnz_core::recipe::format::recipe_id_hex(&recipe);

    let mut engine = Engine::new(recipe.clone())?;

    // Generate keystream bytes = N bytes (one packed byte per emission).
    let key = ark::keystream_bytes(&mut engine, plain.len(), args.max_ticks)?;

    // Optional keystream dump (exactly what was used for XOR).
    if let Some(path) = args.dump_keystream.as_deref() {
        std::fs::write(path, &key)?;
        eprintln!("dumped keystream: {} ({} bytes)", path, key.len());
    }

    // XOR
    let mut cipher = plain.clone();
    for (c, k) in cipher.iter_mut().zip(key.iter()) {
        *c ^= *k;
    }

    // The recipe written into the ark must match the stream used (includes qshift/profile).
    ark::write_ark(&args.out, &recipe, &cipher)?;

    // Operator clarity:
    // - "custom" if --qshift is explicitly provided
    // - "recipe" if a recipe file was provided and we used its embedded shift
    // - otherwise "tuned"/"baseline" from --profile
    let profile_label = if args.qshift.is_some() {
        "custom"
    } else if recipe_from_file {
        "recipe"
    } else {
        match args.profile {
            Profile::Tuned => "tuned",
            Profile::Baseline => "baseline",
        }
    };

    eprintln!(
        "encode ok: in_bytes={} out={} ticks={} emissions={} profile={} qshift={} recipe_id={}",
        plain.len(),
        args.out,
        engine.stats.ticks,
        engine.stats.emissions,
        profile_label,
        effective_shift,
        rid
    );

    Ok(())
}
