use clap::{Parser, Subcommand};

mod cmd;
mod io;

#[derive(Parser)]
#[command(name = "k8dnz-cli")]
#[command(about = "K8DNZ / Cadence Project CLI", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub cmd: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Run the simulator (tokens/stats/qsearch)
    Sim(cmd::sim::SimArgs),

    /// Regenerate token stream from a recipe (.k8r)
    Regen(cmd::regen::RegenArgs),

    /// Encode a file into .ark using cadence keystream XOR
    Encode(cmd::encode::EncodeArgs),

    /// Decode a .ark back to original bytes
    Decode(cmd::decode_file::DecodeFileArgs),

    /// Inspect a .ark artifact (magic/crc, embedded recipe + id, sizes)
    ArkInspect(cmd::ark_inspect::ArkInspectArgs),

    /// Analyze a file as raw bytes (histogram, entropy, top bytes)
    Analyze(cmd::analyze::AnalyzeArgs),

    /// Placeholder for tuner (Deliverable 4)
    Tune(cmd::tune::TuneArgs),
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.cmd {
        Commands::Sim(args) => cmd::sim::run(args),
        Commands::Regen(args) => cmd::regen::run(args),
        Commands::Encode(args) => cmd::encode::run(args),
        Commands::Decode(args) => cmd::decode_file::run(args),
        Commands::ArkInspect(args) => cmd::ark_inspect::run(args),
        Commands::Analyze(args) => cmd::analyze::run(args),
        Commands::Tune(args) => cmd::tune::run(args),
    }
}
