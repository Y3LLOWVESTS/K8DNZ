use anyhow::{Context, Result};
use clap::Parser;

use k8dnz_cli::merkle::unzip::merkle_unzip_to_bytes;

#[derive(Parser, Debug)]
#[command(name = "arku")]
struct Args {
    root: String,
    out: String,
}

fn main() -> Result<()> {
    let a = Args::parse();

    let root_bytes = std::fs::read(&a.root).with_context(|| format!("read root: {}", a.root))?;
    let (out_bytes, rep) = merkle_unzip_to_bytes(&root_bytes)?;

    std::fs::write(&a.out, &out_bytes).with_context(|| format!("write out: {}", a.out))?;

    eprintln!("ROOT_BYTES={}", rep.root_bytes);
    eprintln!("LEAF_COUNT={}", rep.leaf_count);
    eprintln!("ROUNDS={}", rep.rounds);
    eprintln!("OUT_BYTES={}", rep.out_bytes);
    eprintln!("OUT_PATH={}", a.out);

    Ok(())
}
