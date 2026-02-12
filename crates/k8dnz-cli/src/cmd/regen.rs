use clap::Args;
use k8dnz_core::Engine;

use crate::io::{bin, jsonl, recipe_file};

#[derive(Args)]
pub struct RegenArgs {
    /// Recipe path (.k8r)
    #[arg(long)]
    pub recipe: String,

    /// Emissions to produce
    #[arg(long, default_value_t = 64)]
    pub emissions: u64,

    /// Max ticks guard
    #[arg(long, default_value_t = 5_000_000)]
    pub max_ticks: u64,

    /// Output format: "jsonl" or "bin"
    #[arg(long, default_value = "jsonl")]
    pub out: String,

    /// Output file path; if omitted, prints to stdout (jsonl only).
    #[arg(long)]
    pub output: Option<String>,
}

pub fn run(args: RegenArgs) -> anyhow::Result<()> {
    let recipe = recipe_file::load_k8r(&args.recipe)?;
    let mut engine = Engine::new(recipe)?;
    let toks = engine.run_emissions(args.emissions, args.max_ticks);

    match args.out.as_str() {
        "jsonl" => {
            if let Some(p) = args.output.as_deref() {
                jsonl::write_tokens_file(p, &toks)?;
            } else {
                jsonl::write_tokens_stdout(&toks)?;
            }
        }
        "bin" => {
            let p = args
                .output
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("--out bin requires --output <file>"))?;
            bin::write_bytes_file(p, &toks)?;
        }
        other => anyhow::bail!("unknown --out format: {other}"),
    }

    Ok(())
}
