use anyhow::Result;
use clap::{Args, Subcommand, ValueEnum};

mod apex_map_case;
mod apex_map_dibit;
mod apex_map_dibit_other;
mod apex_map_lane;
mod apex_map_punct;
mod apex_map_punct_kind;
mod class_metrics;
mod common;
mod compact_manifest;
mod key_ops;
mod stats;
mod symbol_metrics;
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
    Pack(PackArgs),
    Inspect(InspectArgs),
    Gen(GenArgs),
    Fit(FitArgs),
    Render(RenderArgs),
    Stats(StatsArgs),
    WindowScan(WindowScanArgs),
    #[command(name = "ws-lane")]
    WsLane(WsLaneArgs),
    #[command(name = "apex-map-lane")]
    ApexMapLane(ApexMapLaneArgs),
    #[command(name = "apex-map-punct")]
    ApexMapPunct(ApexMapPunctArgs),
    #[command(name = "apex-map-punct-kind")]
    ApexMapPunctKind(ApexMapPunctKindArgs),
    #[command(name = "apex-map-case")]
    ApexMapCase(ApexMapCaseArgs),
    #[command(name = "apex-map-dibit")]
    ApexMapDibit(ApexMapDibitArgs),
    #[command(name = "apex-map-dibit-other")]
    ApexMapDibitOther(ApexMapDibitOtherArgs),
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

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum ChunkSearchObjective {
    Raw,
    RawGuarded,
    Honest,
    Newline,
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
    #[arg(long)]
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

#[derive(Args, Debug)]
pub struct ApexMapDibitArgs {
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
    #[arg(long, default_value_t = 128)]
    pub chunk_bytes: usize,
    #[arg(long)]
    pub chunk_sweep: Option<String>,
    #[arg(long, value_enum, default_value_t = ChunkSearchObjective::Raw)]
    pub chunk_search_objective: ChunkSearchObjective,
    #[arg(long, default_value_t = 1)]
    pub chunk_raw_slack: u64,
    #[arg(long, default_value_t = 0)]
    pub map_max_depth: u8,
    #[arg(long, default_value_t = 1)]
    pub map_depth_shift: u8,
    #[arg(long, default_value_t = 16)]
    pub boundary_band: usize,
    #[arg(long)]
    pub boundary_band_sweep: Option<String>,
    #[arg(long, default_value_t = 1)]
    pub boundary_delta: usize,
    #[arg(long, default_value_t = 8)]
    pub field_margin: u64,
    #[arg(long)]
    pub field_margin_sweep: Option<String>,
    #[arg(long)]
    pub out_key: Option<String>,
    #[arg(long)]
    pub out_pred: Option<String>,
    #[arg(long)]
    pub out_diag: Option<String>,
    #[arg(long, value_enum, default_value_t = RenderFormat::Txt)]
    pub format: RenderFormat,
    #[arg(long)]
    pub out: Option<String>,
}

#[derive(Args, Debug)]
pub struct ApexMapDibitOtherArgs {
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
    #[arg(long, default_value_t = 128)]
    pub chunk_bytes: usize,
    #[arg(long)]
    pub chunk_sweep: Option<String>,
    #[arg(long, value_enum, default_value_t = ChunkSearchObjective::Raw)]
    pub chunk_search_objective: ChunkSearchObjective,
    #[arg(long, default_value_t = 1)]
    pub chunk_raw_slack: u64,
    #[arg(long, default_value_t = 0)]
    pub map_max_depth: u8,
    #[arg(long, default_value_t = 1)]
    pub map_depth_shift: u8,
    #[arg(long, default_value_t = 16)]
    pub boundary_band: usize,
    #[arg(long)]
    pub boundary_band_sweep: Option<String>,
    #[arg(long, default_value_t = 1)]
    pub boundary_delta: usize,
    #[arg(long, default_value_t = 8)]
    pub field_margin: u64,
    #[arg(long)]
    pub field_margin_sweep: Option<String>,
    #[arg(long)]
    pub out_key: Option<String>,
    #[arg(long)]
    pub out_pred: Option<String>,
    #[arg(long)]
    pub out_diag: Option<String>,
    #[arg(long, value_enum, default_value_t = RenderFormat::Txt)]
    pub format: RenderFormat,
    #[arg(long)]
    pub out: Option<String>,
}

#[derive(Args, Debug)]
pub struct ApexMapLaneArgs {
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
    #[arg(long, default_value_t = 128)]
    pub chunk_bytes: usize,
    #[arg(long)]
    pub chunk_sweep: Option<String>,
    #[arg(long, value_enum, default_value_t = ChunkSearchObjective::Raw)]
    pub chunk_search_objective: ChunkSearchObjective,
    #[arg(long, default_value_t = 1)]
    pub chunk_raw_slack: u64,
    #[arg(long, default_value_t = 0)]
    pub map_max_depth: u8,
    #[arg(long, default_value_t = 1)]
    pub map_depth_shift: u8,
    #[arg(long, default_value_t = 16)]
    pub boundary_band: usize,
    #[arg(long)]
    pub boundary_band_sweep: Option<String>,
    #[arg(long, default_value_t = 1)]
    pub boundary_delta: usize,
    #[arg(long, default_value_t = 8)]
    pub field_margin: u64,
    #[arg(long)]
    pub field_margin_sweep: Option<String>,
    #[arg(long, default_value_t = 96)]
    pub newline_margin_add: u64,
    #[arg(long, default_value_t = 64)]
    pub space_to_newline_margin_add: u64,
    #[arg(long, default_value_t = 550_000)]
    pub newline_share_ppm_min: u32,
    #[arg(long, default_value_t = 0)]
    pub newline_override_budget: usize,
    #[arg(long, default_value_t = 0)]
    pub newline_demote_margin: u64,
    #[arg(long)]
    pub newline_demote_margin_sweep: Option<String>,
    #[arg(long, default_value_t = 150_000)]
    pub newline_demote_keep_ppm_min: u32,
    #[arg(long, default_value_t = 1)]
    pub newline_demote_keep_min: usize,
    #[arg(long, action = clap::ArgAction::Set, default_value_t = true)]
    pub newline_only_from_spacelike: bool,
    #[arg(long, default_value_t = false)]
    pub field_from_global: bool,
    #[arg(long)]
    pub out_key: Option<String>,
    #[arg(long)]
    pub out_pred: Option<String>,
    #[arg(long)]
    pub out_diag: Option<String>,
    #[arg(long, default_value_t = 0)]
    pub diag_limit: usize,
    #[arg(long, value_enum, default_value_t = RenderFormat::Txt)]
    pub format: RenderFormat,
    #[arg(long)]
    pub out: Option<String>,
}

#[derive(Args, Debug)]
pub struct ApexMapPunctArgs {
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
    #[arg(long, default_value_t = 128)]
    pub chunk_bytes: usize,
    #[arg(long)]
    pub chunk_sweep: Option<String>,
    #[arg(long, value_enum, default_value_t = ChunkSearchObjective::Raw)]
    pub chunk_search_objective: ChunkSearchObjective,
    #[arg(long, default_value_t = 1)]
    pub chunk_raw_slack: u64,
    #[arg(long, default_value_t = 0)]
    pub map_max_depth: u8,
    #[arg(long, default_value_t = 1)]
    pub map_depth_shift: u8,
    #[arg(long, default_value_t = 16)]
    pub boundary_band: usize,
    #[arg(long)]
    pub boundary_band_sweep: Option<String>,
    #[arg(long, default_value_t = 1)]
    pub boundary_delta: usize,
    #[arg(long, default_value_t = 8)]
    pub field_margin: u64,
    #[arg(long)]
    pub field_margin_sweep: Option<String>,
    #[arg(long, default_value_t = 0)]
    pub term_margin_add: u64,
    #[arg(long, default_value_t = 0)]
    pub pause_margin_add: u64,
    #[arg(long, default_value_t = 0)]
    pub wrap_margin_add: u64,
    #[arg(long, default_value_t = 0)]
    pub term_share_ppm_min: u32,
    #[arg(long, default_value_t = 0)]
    pub pause_share_ppm_min: u32,
    #[arg(long, default_value_t = 0)]
    pub wrap_share_ppm_min: u32,
    #[arg(long, default_value_t = false)]
    pub field_from_global: bool,
    #[arg(long)]
    pub out_key: Option<String>,
    #[arg(long)]
    pub out_pred: Option<String>,
    #[arg(long)]
    pub out_diag: Option<String>,
    #[arg(long, default_value_t = 0)]
    pub diag_limit: usize,
    #[arg(long, value_enum, default_value_t = RenderFormat::Txt)]
    pub format: RenderFormat,
    #[arg(long)]
    pub out: Option<String>,
}

#[derive(Args, Debug)]
pub struct ApexMapPunctKindArgs {
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
    #[arg(long, default_value_t = 128)]
    pub chunk_bytes: usize,
    #[arg(long)]
    pub chunk_sweep: Option<String>,
    #[arg(long, value_enum, default_value_t = ChunkSearchObjective::Raw)]
    pub chunk_search_objective: ChunkSearchObjective,
    #[arg(long, default_value_t = 1)]
    pub chunk_raw_slack: u64,
    #[arg(long, default_value_t = 0)]
    pub map_max_depth: u8,
    #[arg(long, default_value_t = 1)]
    pub map_depth_shift: u8,
    #[arg(long, default_value_t = 16)]
    pub boundary_band: usize,
    #[arg(long)]
    pub boundary_band_sweep: Option<String>,
    #[arg(long, default_value_t = 1)]
    pub boundary_delta: usize,
    #[arg(long, default_value_t = 8)]
    pub field_margin: u64,
    #[arg(long)]
    pub field_margin_sweep: Option<String>,
    #[arg(long, default_value_t = 0)]
    pub term_margin_add: u64,
    #[arg(long, default_value_t = 0)]
    pub pause_margin_add: u64,
    #[arg(long, default_value_t = 0)]
    pub wrap_margin_add: u64,
    #[arg(long, default_value_t = 0)]
    pub term_share_ppm_min: u32,
    #[arg(long, default_value_t = 0)]
    pub pause_share_ppm_min: u32,
    #[arg(long, default_value_t = 0)]
    pub wrap_share_ppm_min: u32,
    #[arg(long, default_value_t = false)]
    pub field_from_global: bool,
    #[arg(long)]
    pub out_key: Option<String>,
    #[arg(long)]
    pub out_pred: Option<String>,
    #[arg(long)]
    pub out_diag: Option<String>,
    #[arg(long, default_value_t = 0)]
    pub diag_limit: usize,
    #[arg(long, value_enum, default_value_t = RenderFormat::Txt)]
    pub format: RenderFormat,
    #[arg(long)]
    pub out: Option<String>,
}

#[derive(Args, Debug)]
pub struct ApexMapCaseArgs {
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
    #[arg(long, default_value_t = 128)]
    pub chunk_bytes: usize,
    #[arg(long)]
    pub chunk_sweep: Option<String>,
    #[arg(long, value_enum, default_value_t = ChunkSearchObjective::Raw)]
    pub chunk_search_objective: ChunkSearchObjective,
    #[arg(long, default_value_t = 1)]
    pub chunk_raw_slack: u64,
    #[arg(long, default_value_t = 0)]
    pub map_max_depth: u8,
    #[arg(long, default_value_t = 1)]
    pub map_depth_shift: u8,
    #[arg(long, default_value_t = 16)]
    pub boundary_band: usize,
    #[arg(long)]
    pub boundary_band_sweep: Option<String>,
    #[arg(long, default_value_t = 1)]
    pub boundary_delta: usize,
    #[arg(long, default_value_t = 8)]
    pub field_margin: u64,
    #[arg(long)]
    pub field_margin_sweep: Option<String>,
    #[arg(long, default_value_t = 0)]
    pub lower_margin_add: u64,
    #[arg(long, default_value_t = 0)]
    pub upper_margin_add: u64,
    #[arg(long, default_value_t = 0)]
    pub lower_share_ppm_min: u32,
    #[arg(long, default_value_t = 0)]
    pub upper_share_ppm_min: u32,
    #[arg(long, default_value_t = false)]
    pub field_from_global: bool,
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
        ApexTraceCmd::ApexMapLane(a) => apex_map_lane::run_apex_map_lane(a),
        ApexTraceCmd::ApexMapPunct(a) => apex_map_punct::run_apex_map_punct(a),
        ApexTraceCmd::ApexMapPunctKind(a) => apex_map_punct_kind::run_apex_map_punct_kind(a),
        ApexTraceCmd::ApexMapCase(a) => apex_map_case::run_apex_map_case(a),
        ApexTraceCmd::ApexMapDibit(a) => apex_map_dibit::run_apex_map_dibit(a),
        ApexTraceCmd::ApexMapDibitOther(a) => apex_map_dibit_other::run_apex_map_dibit_other(a),
    }
}