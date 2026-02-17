// crates/k8dnz-cli/src/cmd/decode2kb.rs

use clap::Args;
use k8dnz_core::lane;

#[derive(Args)]
pub struct Decode2kbArgs {
    /// Input artifact path (K8L1)
    #[arg(long = "in")]
    pub r#in: String,

    /// Output file path
    #[arg(long)]
    pub out: String,
}

pub fn run(args: Decode2kbArgs) -> anyhow::Result<()> {
    let artifact = std::fs::read(&args.r#in)?;
    let decoded = lane::decode_k8l1(&artifact).map_err(|e| anyhow::anyhow!("{e}"))?;
    std::fs::write(&args.out, &decoded)?;
    println!("ok decode2kb: out={} bytes={}", args.out, decoded.len());
    Ok(())
}
