use clap::Args;
use k8dnz_core::Engine;

use crate::io::ark;

#[derive(Args)]
pub struct DecodeFileArgs {
    /// Input .ark path
    #[arg(long)]
    pub r#in: String,

    /// Output decoded file path
    #[arg(long)]
    pub out: String,

    /// Max ticks guard for keystream generation
    #[arg(long, default_value_t = 50_000_000)]
    pub max_ticks: u64,
}

pub fn run(args: DecodeFileArgs) -> anyhow::Result<()> {
    // Read the embedded recipe_id directly from the ark payload (no recompute).
    let (rid, recipe, cipher) = ark::read_ark_with_id(&args.r#in)?;

    let mut engine = Engine::new(recipe.clone())?;

    let key = ark::keystream_bytes(&mut engine, cipher.len(), args.max_ticks)?;

    let mut plain = cipher;
    for (p, k) in plain.iter_mut().zip(key.iter()) {
        *p ^= *k;
    }

    std::fs::write(&args.out, plain)?;
    eprintln!(
        "decode ok: out={} ticks={} emissions={} recipe_id={}",
        args.out, engine.stats.ticks, engine.stats.emissions, rid
    );
    Ok(())
}
