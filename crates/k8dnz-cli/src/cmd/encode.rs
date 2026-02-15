// crates/k8dnz-cli/src/cmd/encode.rs

use clap::{Args, ValueEnum};
use k8dnz_core::recipe::recipe::{KeystreamMix, PayloadKind};
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

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum PayloadArg {
    Cipher,
    Residual,
}

impl PayloadArg {
    fn to_core(self) -> PayloadKind {
        match self {
            PayloadArg::Cipher => PayloadKind::CipherXor,
            PayloadArg::Residual => PayloadKind::ResidualXor,
        }
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
    /// NOTE: --qshift overrides everything.
    #[arg(long, value_enum, default_value_t = Profile::Tuned)]
    pub profile: Profile,

    /// SIM/encode-only override: shift applied to quant bounds (min/max).
    #[arg(long)]
    pub qshift: Option<i64>,

    /// OPTIONAL: Keystream mixing (opt-in). If omitted, keep recipe’s value.
    #[arg(long, value_enum)]
    pub keystream_mix: Option<KeystreamMixArg>,

    /// OPTIONAL: Payload kind label stored in recipe. If omitted, keep recipe’s value.
    #[arg(long, value_enum)]
    pub payload: Option<PayloadArg>,

    /// Max ticks guard for keystream generation
    #[arg(long, default_value_t = 50_000_000)]
    pub max_ticks: u64,

    /// Optional: dump the USED keystream bytes (mixed if mixing enabled).
    #[arg(long)]
    pub dump_keystream: Option<String>,

    /// Optional: dump the RAW cadence keystream bytes (pre-mix).
    #[arg(long)]
    pub dump_raw_keystream: Option<String>,
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
    // 2) if --recipe was provided, keep shift embedded in recipe file
    // 3) otherwise apply --profile convenience shift
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

    // Optional knobs (do NOT override unless explicitly provided)
    if let Some(m) = args.keystream_mix {
        recipe.keystream_mix = m.to_core();
    }
    if let Some(p) = args.payload {
        recipe.payload_kind = p.to_core();
    }

    let rid = k8dnz_core::recipe::format::recipe_id_hex(&recipe);

    let mut engine = Engine::new(recipe.clone())?;

    let (key_used, key_raw_opt) = if args.dump_raw_keystream.is_some() {
        let (used, raw) = ark::keystream_bytes_with_raw(&mut engine, plain.len(), args.max_ticks)?;
        (used, Some(raw))
    } else {
        (
            ark::keystream_bytes(&mut engine, plain.len(), args.max_ticks)?,
            None,
        )
    };

    if let Some(path) = args.dump_keystream.as_deref() {
        std::fs::write(path, &key_used)?;
        eprintln!("dumped keystream: {} ({} bytes)", path, key_used.len());
    }

    if let (Some(path), Some(raw)) = (args.dump_raw_keystream.as_deref(), key_raw_opt.as_deref()) {
        std::fs::write(path, raw)?;
        eprintln!("dumped raw keystream: {} ({} bytes)", path, raw.len());
    }

    // XOR
    let mut data = plain.clone();
    for (c, k) in data.iter_mut().zip(key_used.iter()) {
        *c ^= *k;
    }

    ark::write_ark(&args.out, &recipe, &data)?;

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
        "encode ok: in_bytes={} out={} ticks={} emissions={} profile={} qshift={} recipe_id={} mix={:?} payload={:?}",
        plain.len(),
        args.out,
        engine.stats.ticks,
        engine.stats.emissions,
        profile_label,
        effective_shift,
        rid,
        recipe.keystream_mix,
        recipe.payload_kind
    );

    Ok(())
}
