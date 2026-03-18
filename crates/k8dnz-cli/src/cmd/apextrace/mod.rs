use anyhow::Result;
use clap::{Args, Subcommand, ValueEnum};

mod common;
mod key_ops;
mod stats;
mod ws_lane;
mod ws_lane_render;
mod ws_lane_types;

#[derive(Args, Debug)]
pub struct ApexTraceArgs {
    #[command(subcommand)]
    pub cmd: ApexTraceCmd,
}

#[derive(Subcommand, Debug)]
pub enum ApexTraceCmd {
    /// Create a deterministic ApexKey (.atk) from explicit parameters
    Pack(PackArgs),
    /// Inspect an ApexKey (.atk)
    Inspect(InspectArgs),
    /// Generate bytes or quaternary stream from an ApexKey (.atk)
    Gen(GenArgs),
    /// Brute-force search for a good ApexKey against target bytes
    Fit(FitArgs),
    /// Render 90-degree pyramid coordinates from an ApexKey or by fitting input on the fly
    Render(RenderArgs),
    /// Render subtree-conditioned stats against a target
    Stats(StatsArgs),
    /// Scan moving windows and report local-fit diagnostics
    WindowScan(WindowScanArgs),
    /// Experimental ApexTrace predictor against the K8DNZ whitespace/class lane
    #[command(name = "ws-lane")]
    WsLane(WsLaneArgs),
}

#[derive(Args, Debug)]
pub struct PackArgs {
    #[arg(long)]
    pub byte_len: u64,
    #[arg(long, default_value_t = 0)]
    pub root_quadrant: u8,
    #[arg(long, default_value_t = 0)]
    pub root_seed: u64,
    #[arg(long, default_value_t = 1)]
    pub recipe_seed: u64,
    #[arg(long)]
    pub out: String,
}

#[derive(Args, Debug)]
pub struct InspectArgs {
    #[arg(long)]
    pub atk: String,
}

#[derive(Args, Debug)]
pub struct GenArgs {
    #[arg(long)]
    pub atk: String,
    #[arg(long)]
    pub out: Option<String>,
    #[arg(long, default_value_t = false)]
    pub quats: bool,
}

#[derive(Args, Debug)]
pub struct FitArgs {
    #[arg(long = "in")]
    pub r#in: String,
    #[arg(long, default_value_t = 0)]
    pub seed_from: u64,
    #[arg(long, default_value_t = 4096)]
    pub seed_count: u64,
    #[arg(long, default_value_t = 1)]
    pub seed_step: u64,
    #[arg(long, default_value_t = 1)]
    pub recipe_seed: u64,
    #[arg(long)]
    pub out_key: Option<String>,
    #[arg(long)]
    pub gen_out: Option<String>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum RenderMode {
    Lattice,
    Paths,
    Base,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum RenderFormat {
    Csv,
    Txt,
}

#[derive(Args, Debug)]
pub struct RenderArgs {
    #[arg(long)]
    pub atk: Option<String>,
    #[arg(long = "in")]
    pub r#in: Option<String>,
    #[arg(long, default_value_t = 0)]
    pub seed_from: u64,
    #[arg(long, default_value_t = 4096)]
    pub seed_count: u64,
    #[arg(long, default_value_t = 1)]
    pub seed_step: u64,
    #[arg(long, default_value_t = 1)]
    pub recipe_seed: u64,
    #[arg(long)]
    pub out_key: Option<String>,
    #[arg(long)]
    pub max_quats: Option<u64>,
    #[arg(long, default_value_t = false)]
    pub active_only: bool,
    #[arg(long, value_enum, default_value_t = RenderMode::Lattice)]
    pub mode: RenderMode,
    #[arg(long, value_enum, default_value_t = RenderFormat::Csv)]
    pub format: RenderFormat,
    #[arg(long)]
    pub out: Option<String>,
}

#[derive(Args, Debug)]
pub struct StatsArgs {
    #[arg(long)]
    pub atk: Option<String>,
    #[arg(long = "in")]
    pub r#in: String,
    #[arg(long, default_value_t = 0)]
    pub seed_from: u64,
    #[arg(long, default_value_t = 4096)]
    pub seed_count: u64,
    #[arg(long, default_value_t = 1)]
    pub seed_step: u64,
    #[arg(long, default_value_t = 1)]
    pub recipe_seed: u64,
    #[arg(long)]
    pub out_key: Option<String>,
    #[arg(long)]
    pub max_quats: Option<u64>,
    #[arg(long, default_value_t = false)]
    pub active_only: bool,
    #[arg(long, value_enum, default_value_t = RenderFormat::Csv)]
    pub format: RenderFormat,
    #[arg(long)]
    pub out: Option<String>,
}

#[derive(Args, Debug)]
pub struct WindowScanArgs {
    #[arg(long = "in")]
    pub r#in: String,
    #[arg(long, default_value_t = 16)]
    pub window_bytes: usize,
    #[arg(long, default_value_t = 1)]
    pub step_bytes: usize,
    #[arg(long)]
    pub max_windows: Option<usize>,
    #[arg(long, default_value_t = 0)]
    pub seed_from: u64,
    #[arg(long, default_value_t = 4096)]
    pub seed_count: u64,
    #[arg(long, default_value_t = 1)]
    pub seed_step: u64,
    #[arg(long, default_value_t = 1)]
    pub recipe_seed: u64,
    #[arg(long, value_enum, default_value_t = RenderFormat::Csv)]
    pub format: RenderFormat,
    #[arg(long)]
    pub out: Option<String>,
}

#[derive(Args, Debug)]
pub struct WsLaneArgs {
    #[arg(long)]
    pub recipe: String,
    #[arg(long = "in")]
    pub r#in: String,
    #[arg(long, default_value_t = 20_000_000)]
    pub max_ticks: u64,
    #[arg(long, default_value_t = 0)]
    pub seed_from: u64,
    #[arg(long, default_value_t = 4096)]
    pub seed_count: u64,
    #[arg(long, default_value_t = 1)]
    pub seed_step: u64,
    #[arg(long, default_value_t = 1)]
    pub recipe_seed: u64,
    #[arg(long)]
    pub chunk_bytes: Option<usize>,
    #[arg(long, help = "Comma-separated chunk sizes, e.g. 64,128,256,512")]
    pub chunk_sweep: Option<String>,
    #[arg(long)]
    pub out_key: Option<String>,
    #[arg(long)]
    pub out_pred: Option<String>,
    #[arg(long, value_enum, default_value_t = RenderFormat::Txt)]
    pub format: RenderFormat,
    #[arg(long)]
    pub out: Option<String>,
}

pub fn run(args: ApexTraceArgs) -> Result<()> {
    match args.cmd {
        ApexTraceCmd::Pack(a) => key_ops::run_pack(a),
        ApexTraceCmd::Inspect(a) => key_ops::run_inspect(a),
        ApexTraceCmd::Gen(a) => key_ops::run_gen(a),
        ApexTraceCmd::Fit(a) => key_ops::run_fit(a),
        ApexTraceCmd::Render(a) => key_ops::run_render(a),
        ApexTraceCmd::Stats(a) => stats::run_stats(a),
        ApexTraceCmd::WindowScan(a) => stats::run_window_scan(a),
        ApexTraceCmd::WsLane(a) => ws_lane::run_ws_lane(a),
    }
}
