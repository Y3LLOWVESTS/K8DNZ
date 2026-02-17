use anyhow::{Context, Result};
use clap::Parser;

use k8dnz_cli::merkle::runner::FitProfile;
use k8dnz_cli::merkle::zip::merkle_zip_bytes;

#[derive(Parser, Debug)]
#[command(name = "arkc")]
struct Args {
    input: String,

    #[arg(long, default_value = "./configs/tuned_validated.k8r")]
    recipe: String,

    #[arg(long)]
    out: Option<String>,

    #[arg(long, default_value_t = 2048)]
    chunk_bytes: usize,

    #[arg(long, default_value_t = 1)]
    map_seed: u64,

    #[arg(long, default_value_t = 1)]
    bits_per_emission: u8,

    #[arg(long, default_value_t = 3)]
    zstd_level: i32,
}

fn main() -> Result<()> {
    let a = Args::parse();

    let input = std::fs::read(&a.input).with_context(|| format!("read input: {}", a.input))?;

    let mut prof = FitProfile::default();
    prof.bits_per_emission = a.bits_per_emission;
    prof.zstd_level = a.zstd_level;

    let (root_bytes, rep) = merkle_zip_bytes(&a.recipe, &input, a.chunk_bytes, &prof, a.map_seed)?;

    let out_path = a.out.unwrap_or_else(|| format!("{}.arkm", a.input));
    std::fs::write(&out_path, &root_bytes).with_context(|| format!("write: {}", out_path))?;

    eprintln!("INPUT_BYTES={}", rep.input_bytes);
    eprintln!("CHUNK_BYTES={}", rep.chunk_bytes);
    eprintln!("LEAF_COUNT={}", rep.leaf_count);
    eprintln!("ROUNDS={}", rep.rounds);
    eprintln!("ROOT_BYTES={}", rep.root_bytes);
    eprintln!("ROOT_PATH={}", out_path);

    Ok(())
}
