use anyhow::Result;
use clap::{Args, Subcommand, ValueEnum};

mod apex_lane_atlas;
mod apex_lane_law_freeze;
mod apex_lane_law_split_freeze;
mod apex_lane_law_bridge_freeze;
mod apex_lane_law_profile;
mod apex_lane_manifest;
mod apex_lane_manifest_compare;
mod apex_lane_table;
mod apex_map_case;
mod apex_map_case_anchor;
mod apex_map_dibit;
mod apex_map_dibit_other;
mod apex_map_lane;
mod apex_map_punct;
mod apex_map_punct_kind;
mod baselines;
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
    #[command(name = "apex-lane-atlas")]
    ApexLaneAtlas(ApexLaneAtlasArgs),
    #[command(name = "apex-lane-law-freeze")]
    ApexLaneLawFreeze(ApexLaneLawFreezeArgs),
    #[command(name = "apex-lane-law-split-freeze")]
    ApexLaneLawSplitFreeze(ApexLaneLawSplitFreezeArgs),
    #[command(name = "apex-lane-law-bridge-freeze")]
    ApexLaneLawBridgeFreeze(ApexLaneLawBridgeFreezeArgs),
    #[command(name = "apex-lane-law-profile")]
    ApexLaneLawProfile(ApexLaneLawProfileArgs),
    #[command(name = "apex-lane-manifest")]
    ApexLaneManifest(ApexLaneManifestArgs),
    #[command(name = "apex-lane-manifest-compare")]
    ApexLaneManifestCompare(ApexLaneManifestCompareArgs),
    #[command(name = "apex-lane-table")]
    ApexLaneTable(ApexLaneTableArgs),
    #[command(name = "apex-map-lane")]
    ApexMapLane(ApexMapLaneArgs),
    #[command(name = "apex-map-punct")]
    ApexMapPunct(ApexMapPunctArgs),
    #[command(name = "apex-map-punct-kind")]
    ApexMapPunctKind(ApexMapPunctKindArgs),
    #[command(name = "apex-map-case")]
    ApexMapCase(ApexMapCaseArgs),
    #[command(name = "apex-map-case-anchor")]
    ApexMapCaseAnchor(ApexMapCaseAnchorArgs),
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
pub struct ApexLaneAtlasArgs {
    #[arg(long, default_value = "configs/tuned_validated.k8r")]
    pub recipe: String,
    #[arg(long = "in")]
    pub r#in: String,
    #[arg(long, default_value_t = 20_000_000)]
    pub max_ticks: u64,
    #[arg(long, default_value_t = 256)]
    pub window_bytes: usize,
    #[arg(long, default_value_t = 128)]
    pub step_bytes: usize,
    #[arg(long, default_value_t = 12)]
    pub max_windows: usize,
    #[arg(long, default_value_t = 0)]
    pub seed_from: u64,
    #[arg(long, default_value_t = 64)]
    pub seed_count: u64,
    #[arg(long, default_value_t = 1)]
    pub seed_step: u64,
    #[arg(long, default_value_t = 1)]
    pub recipe_seed: u64,
    #[arg(long, default_value = "32,64")]
    pub chunk_sweep: String,
    #[arg(long, value_enum, default_value_t = ChunkSearchObjective::Raw)]
    pub chunk_search_objective: ChunkSearchObjective,
    #[arg(long, default_value_t = 1)]
    pub chunk_raw_slack: u64,
    #[arg(long, default_value_t = 0)]
    pub map_max_depth: u8,
    #[arg(long, default_value_t = 1)]
    pub map_depth_shift: u8,
    #[arg(long, default_value = "8,12")]
    pub boundary_band_sweep: String,
    #[arg(long, default_value_t = 1)]
    pub boundary_delta: usize,
    #[arg(long, default_value = "4,8")]
    pub field_margin_sweep: String,
    #[arg(long, default_value_t = 96)]
    pub newline_margin_add: u64,
    #[arg(long, default_value_t = 64)]
    pub space_to_newline_margin_add: u64,
    #[arg(long, default_value_t = 550_000)]
    pub newline_share_ppm_min: u32,
    #[arg(long, default_value_t = 0)]
    pub newline_override_budget: usize,
    #[arg(long, default_value = "0,4")]
    pub newline_demote_margin_sweep: String,
    #[arg(long, default_value_t = 150_000)]
    pub newline_demote_keep_ppm_min: u32,
    #[arg(long, default_value_t = 1)]
    pub newline_demote_keep_min: usize,
    #[arg(long, action = clap::ArgAction::Set, default_value_t = true)]
    pub newline_only_from_spacelike: bool,
    #[arg(long, default_value_t = false)]
    pub field_from_global: bool,
    #[arg(long, default_value_t = 0)]
    pub merge_gap_bytes: usize,
    #[arg(long, default_value_t = false)]
    pub keep_temp_dir: bool,
    #[arg(long, value_enum, default_value_t = RenderFormat::Txt)]
    pub format: RenderFormat,
    #[arg(long)]
    pub out: Option<String>,
}

#[derive(Args, Debug)]
pub struct ApexLaneLawFreezeArgs {
    #[arg(long, default_value = "configs/tuned_validated.k8r")]
    pub recipe: String,
    #[arg(long = "in", required = true, num_args = 1..)]
    pub inputs: Vec<String>,
    #[arg(long, default_value_t = 20_000_000)]
    pub max_ticks: u64,
    #[arg(long, default_value_t = 256)]
    pub window_bytes: usize,
    #[arg(long, default_value_t = 256)]
    pub step_bytes: usize,
    #[arg(long, default_value_t = 12)]
    pub max_windows: usize,
    #[arg(long, default_value_t = 0)]
    pub seed_from: u64,
    #[arg(long, default_value_t = 64)]
    pub seed_count: u64,
    #[arg(long, default_value_t = 1)]
    pub seed_step: u64,
    #[arg(long, default_value_t = 1)]
    pub recipe_seed: u64,
    #[arg(long, default_value = "32,64")]
    pub chunk_sweep: String,
    #[arg(long, value_enum, default_value_t = ChunkSearchObjective::Raw)]
    pub chunk_search_objective: ChunkSearchObjective,
    #[arg(long, default_value_t = 1)]
    pub chunk_raw_slack: u64,
    #[arg(long, default_value_t = 0)]
    pub map_max_depth: u8,
    #[arg(long, default_value_t = 1)]
    pub map_depth_shift: u8,
    #[arg(long, default_value = "8,12")]
    pub boundary_band_sweep: String,
    #[arg(long, default_value_t = 1)]
    pub boundary_delta: usize,
    #[arg(long, default_value = "4,8")]
    pub field_margin_sweep: String,
    #[arg(long, default_value_t = 96)]
    pub newline_margin_add: u64,
    #[arg(long, default_value_t = 64)]
    pub space_to_newline_margin_add: u64,
    #[arg(long, default_value_t = 550_000)]
    pub newline_share_ppm_min: u32,
    #[arg(long, default_value_t = 0)]
    pub newline_override_budget: usize,
    #[arg(long, default_value = "0,4")]
    pub newline_demote_margin_sweep: String,
    #[arg(long, default_value_t = 150_000)]
    pub newline_demote_keep_ppm_min: u32,
    #[arg(long, default_value_t = 1)]
    pub newline_demote_keep_min: usize,
    #[arg(long, action = clap::ArgAction::Set, default_value_t = true)]
    pub newline_only_from_spacelike: bool,
    #[arg(long, default_value_t = 0)]
    pub merge_gap_bytes: usize,
    #[arg(long, default_value_t = false)]
    pub allow_overlap_scout: bool,
    #[arg(long)]
    pub freeze_boundary_band: Option<usize>,
    #[arg(long)]
    pub freeze_field_margin: Option<u64>,
    #[arg(long)]
    pub freeze_newline_demote_margin: Option<u64>,
    #[arg(long)]
    pub freeze_chunk_bytes: Option<usize>,
    #[arg(long, value_enum)]
    pub freeze_chunk_search_objective: Option<ChunkSearchObjective>,
    #[arg(long)]
    pub freeze_chunk_raw_slack: Option<u64>,
    #[arg(long)]
    pub global_law_id: Option<String>,
    #[arg(long, default_value_t = 8)]
    pub top_rows: usize,
    #[arg(long, default_value_t = false)]
    pub keep_temp_dir: bool,
    #[arg(long, value_enum, default_value_t = RenderFormat::Txt)]
    pub format: RenderFormat,
    #[arg(long)]
    pub out: Option<String>,
}


#[derive(Args, Debug)]
pub struct ApexLaneLawSplitFreezeArgs {
    #[arg(long, default_value = "configs/tuned_validated.k8r")]
    pub recipe: String,
    #[arg(long = "in", required = true, num_args = 1..)]
    pub inputs: Vec<String>,
    #[arg(long, default_value_t = 20_000_000)]
    pub max_ticks: u64,
    #[arg(long, default_value_t = 256)]
    pub window_bytes: usize,
    #[arg(long, default_value_t = 256)]
    pub step_bytes: usize,
    #[arg(long, default_value_t = 12)]
    pub max_windows: usize,
    #[arg(long, default_value_t = 0)]
    pub seed_from: u64,
    #[arg(long, default_value_t = 64)]
    pub seed_count: u64,
    #[arg(long, default_value_t = 1)]
    pub seed_step: u64,
    #[arg(long, default_value_t = 1)]
    pub recipe_seed: u64,
    #[arg(long, default_value = "32,64")]
    pub chunk_sweep: String,
    #[arg(long, value_enum, default_value_t = ChunkSearchObjective::Raw)]
    pub chunk_search_objective: ChunkSearchObjective,
    #[arg(long, default_value_t = 1)]
    pub chunk_raw_slack: u64,
    #[arg(long, default_value_t = 0)]
    pub map_max_depth: u8,
    #[arg(long, default_value_t = 1)]
    pub map_depth_shift: u8,
    #[arg(long, default_value = "8,12")]
    pub boundary_band_sweep: String,
    #[arg(long, default_value_t = 1)]
    pub boundary_delta: usize,
    #[arg(long, default_value = "4,8")]
    pub field_margin_sweep: String,
    #[arg(long, default_value_t = 96)]
    pub newline_margin_add: u64,
    #[arg(long, default_value_t = 64)]
    pub space_to_newline_margin_add: u64,
    #[arg(long, default_value_t = 550_000)]
    pub newline_share_ppm_min: u32,
    #[arg(long, default_value_t = 0)]
    pub newline_override_budget: usize,
    #[arg(long, default_value = "0,4")]
    pub newline_demote_margin_sweep: String,
    #[arg(long, default_value_t = 150_000)]
    pub newline_demote_keep_ppm_min: u32,
    #[arg(long, default_value_t = 1)]
    pub newline_demote_keep_min: usize,
    #[arg(long, action = clap::ArgAction::Set, default_value_t = true)]
    pub newline_only_from_spacelike: bool,
    #[arg(long, default_value_t = 0)]
    pub merge_gap_bytes: usize,
    #[arg(long, default_value_t = false)]
    pub allow_overlap_scout: bool,
    #[arg(long)]
    pub freeze_boundary_band: Option<usize>,
    #[arg(long)]
    pub freeze_field_margin: Option<u64>,
    #[arg(long)]
    pub freeze_newline_demote_margin: Option<u64>,
    #[arg(long)]
    pub split_chunk_sweep: Option<String>,
    #[arg(long, value_enum)]
    pub split_chunk_search_objective: Option<ChunkSearchObjective>,
    #[arg(long)]
    pub split_chunk_raw_slack: Option<u64>,
    #[arg(long)]
    pub global_law_id: Option<String>,
    #[arg(long, default_value_t = 8)]
    pub top_rows: usize,
    #[arg(long, default_value_t = false)]
    pub keep_temp_dir: bool,
    #[arg(long, value_enum, default_value_t = RenderFormat::Txt)]
    pub format: RenderFormat,
    #[arg(long)]
    pub out: Option<String>,
}


#[derive(Args, Debug)]
pub struct ApexLaneLawBridgeFreezeArgs {
    #[arg(long, default_value = "configs/tuned_validated.k8r")]
    pub recipe: String,
    #[arg(long = "in", required = true, num_args = 1..)]
    pub inputs: Vec<String>,
    #[arg(long, default_value_t = 20_000_000)]
    pub max_ticks: u64,
    #[arg(long, default_value_t = 256)]
    pub window_bytes: usize,
    #[arg(long, default_value_t = 256)]
    pub step_bytes: usize,
    #[arg(long, default_value_t = 12)]
    pub max_windows: usize,
    #[arg(long, default_value_t = 0)]
    pub seed_from: u64,
    #[arg(long, default_value_t = 64)]
    pub seed_count: u64,
    #[arg(long, default_value_t = 1)]
    pub seed_step: u64,
    #[arg(long, default_value_t = 1)]
    pub recipe_seed: u64,
    #[arg(long, default_value = "32,64")]
    pub chunk_sweep: String,
    #[arg(long, value_enum, default_value_t = ChunkSearchObjective::Raw)]
    pub chunk_search_objective: ChunkSearchObjective,
    #[arg(long, default_value_t = 1)]
    pub chunk_raw_slack: u64,
    #[arg(long, default_value_t = 0)]
    pub map_max_depth: u8,
    #[arg(long, default_value_t = 1)]
    pub map_depth_shift: u8,
    #[arg(long, default_value = "8,12")]
    pub boundary_band_sweep: String,
    #[arg(long, default_value_t = 1)]
    pub boundary_delta: usize,
    #[arg(long, default_value = "4,8")]
    pub field_margin_sweep: String,
    #[arg(long, default_value_t = 96)]
    pub newline_margin_add: u64,
    #[arg(long, default_value_t = 64)]
    pub space_to_newline_margin_add: u64,
    #[arg(long, default_value_t = 550_000)]
    pub newline_share_ppm_min: u32,
    #[arg(long, default_value_t = 0)]
    pub newline_override_budget: usize,
    #[arg(long, default_value = "0,4")]
    pub newline_demote_margin_sweep: String,
    #[arg(long, default_value_t = 150_000)]
    pub newline_demote_keep_ppm_min: u32,
    #[arg(long, default_value_t = 1)]
    pub newline_demote_keep_min: usize,
    #[arg(long, action = clap::ArgAction::Set, default_value_t = true)]
    pub newline_only_from_spacelike: bool,
    #[arg(long, default_value_t = 0)]
    pub merge_gap_bytes: usize,
    #[arg(long, default_value_t = false)]
    pub allow_overlap_scout: bool,
    #[arg(long)]
    pub freeze_boundary_band: Option<usize>,
    #[arg(long)]
    pub freeze_field_margin: Option<u64>,
    #[arg(long)]
    pub freeze_newline_demote_margin: Option<u64>,
    #[arg(long)]
    pub bridge_chunk_sweep: Option<String>,
    #[arg(long, value_enum)]
    pub bridge_chunk_search_objective: Option<ChunkSearchObjective>,
    #[arg(long)]
    pub bridge_chunk_raw_slack: Option<u64>,
    #[arg(long, default_value_t = 2)]
    pub bridge_max_windows: usize,
    #[arg(long, default_value_t = 512)]
    pub bridge_max_span_bytes: usize,
    #[arg(long, default_value_t = 8)]
    pub bridge_max_local_penalty_exact: usize,
    #[arg(long, default_value_t = 1)]
    pub bridge_min_total_gain_exact: usize,
    #[arg(long)]
    pub global_law_id: Option<String>,
    #[arg(long, default_value_t = 8)]
    pub top_rows: usize,
    #[arg(long, default_value_t = false)]
    pub keep_temp_dir: bool,
    #[arg(long, value_enum, default_value_t = RenderFormat::Txt)]
    pub format: RenderFormat,
    #[arg(long)]
    pub out: Option<String>,
}

#[derive(Args, Debug)]
pub struct ApexLaneLawProfileArgs {
    #[arg(long, default_value = "configs/tuned_validated.k8r")]
    pub recipe: String,
    #[arg(long = "in", required = true, num_args = 1..)]
    pub inputs: Vec<String>,
    #[arg(long, default_value_t = 20_000_000)]
    pub max_ticks: u64,
    #[arg(long, default_value_t = 256)]
    pub window_bytes: usize,
    #[arg(long, default_value_t = 256)]
    pub step_bytes: usize,
    #[arg(long, default_value_t = 12)]
    pub max_windows: usize,
    #[arg(long, default_value_t = 0)]
    pub seed_from: u64,
    #[arg(long, default_value_t = 64)]
    pub seed_count: u64,
    #[arg(long, default_value_t = 1)]
    pub seed_step: u64,
    #[arg(long, default_value_t = 1)]
    pub recipe_seed: u64,
    #[arg(long, default_value = "32,64")]
    pub chunk_sweep: String,
    #[arg(long, value_enum, default_value_t = ChunkSearchObjective::Raw)]
    pub chunk_search_objective: ChunkSearchObjective,
    #[arg(long, default_value_t = 1)]
    pub chunk_raw_slack: u64,
    #[arg(long, default_value_t = 0)]
    pub map_max_depth: u8,
    #[arg(long, default_value_t = 1)]
    pub map_depth_shift: u8,
    #[arg(long, default_value = "8,12")]
    pub boundary_band_sweep: String,
    #[arg(long, default_value_t = 1)]
    pub boundary_delta: usize,
    #[arg(long, default_value = "4,8")]
    pub field_margin_sweep: String,
    #[arg(long, default_value_t = 96)]
    pub newline_margin_add: u64,
    #[arg(long, default_value_t = 64)]
    pub space_to_newline_margin_add: u64,
    #[arg(long, default_value_t = 550_000)]
    pub newline_share_ppm_min: u32,
    #[arg(long, default_value_t = 0)]
    pub newline_override_budget: usize,
    #[arg(long, default_value = "0,4")]
    pub newline_demote_margin_sweep: String,
    #[arg(long, default_value_t = 150_000)]
    pub newline_demote_keep_ppm_min: u32,
    #[arg(long, default_value_t = 1)]
    pub newline_demote_keep_min: usize,
    #[arg(long, action = clap::ArgAction::Set, default_value_t = true)]
    pub newline_only_from_spacelike: bool,
    #[arg(long, default_value_t = false)]
    pub field_from_global: bool,
    #[arg(long, default_value_t = 0)]
    pub merge_gap_bytes: usize,
    #[arg(long, default_value_t = false)]
    pub allow_overlap_scout: bool,
    #[arg(long, default_value_t = false)]
    pub keep_temp_dir: bool,
    #[arg(long, value_enum, default_value_t = RenderFormat::Txt)]
    pub format: RenderFormat,
    #[arg(long)]
    pub out: Option<String>,
}

#[derive(Args, Debug)]
pub struct ApexLaneManifestArgs {
    #[arg(long, default_value = "configs/tuned_validated.k8r")]
    pub recipe: String,
    #[arg(long = "in")]
    pub r#in: String,
    #[arg(long, default_value_t = 20_000_000)]
    pub max_ticks: u64,
    #[arg(long, default_value_t = 256)]
    pub window_bytes: usize,
    #[arg(long, default_value_t = 256)]
    pub step_bytes: usize,
    #[arg(long, default_value_t = 12)]
    pub max_windows: usize,
    #[arg(long, default_value_t = 0)]
    pub seed_from: u64,
    #[arg(long, default_value_t = 64)]
    pub seed_count: u64,
    #[arg(long, default_value_t = 1)]
    pub seed_step: u64,
    #[arg(long, default_value_t = 1)]
    pub recipe_seed: u64,
    #[arg(long, default_value = "32,64")]
    pub chunk_sweep: String,
    #[arg(long, value_enum, default_value_t = ChunkSearchObjective::Raw)]
    pub chunk_search_objective: ChunkSearchObjective,
    #[arg(long, default_value_t = 1)]
    pub chunk_raw_slack: u64,
    #[arg(long, default_value_t = 0)]
    pub map_max_depth: u8,
    #[arg(long, default_value_t = 1)]
    pub map_depth_shift: u8,
    #[arg(long, default_value = "8,12")]
    pub boundary_band_sweep: String,
    #[arg(long, default_value_t = 1)]
    pub boundary_delta: usize,
    #[arg(long, default_value = "4,8")]
    pub field_margin_sweep: String,
    #[arg(long, default_value_t = 96)]
    pub newline_margin_add: u64,
    #[arg(long, default_value_t = 64)]
    pub space_to_newline_margin_add: u64,
    #[arg(long, default_value_t = 550_000)]
    pub newline_share_ppm_min: u32,
    #[arg(long, default_value_t = 0)]
    pub newline_override_budget: usize,
    #[arg(long, default_value = "0,4")]
    pub newline_demote_margin_sweep: String,
    #[arg(long, default_value_t = 150_000)]
    pub newline_demote_keep_ppm_min: u32,
    #[arg(long, default_value_t = 1)]
    pub newline_demote_keep_min: usize,
    #[arg(long, action = clap::ArgAction::Set, default_value_t = true)]
    pub newline_only_from_spacelike: bool,
    #[arg(long, default_value_t = false)]
    pub field_from_global: bool,
    #[arg(long, default_value_t = 0)]
    pub merge_gap_bytes: usize,
    #[arg(long, default_value_t = false)]
    pub allow_overlap_scout: bool,
    #[arg(long, default_value_t = false)]
    pub keep_temp_dir: bool,
    #[arg(long, value_enum, default_value_t = RenderFormat::Txt)]
    pub format: RenderFormat,
    #[arg(long)]
    pub out: Option<String>,
}

#[derive(Args, Debug)]
pub struct ApexLaneManifestCompareArgs {
    #[arg(long, default_value = "configs/tuned_validated.k8r")]
    pub recipe: String,
    #[arg(long = "in", required = true, num_args = 1..)]
    pub inputs: Vec<String>,
    #[arg(long, default_value_t = 20_000_000)]
    pub max_ticks: u64,
    #[arg(long, default_value_t = 256)]
    pub window_bytes: usize,
    #[arg(long, default_value_t = 256)]
    pub step_bytes: usize,
    #[arg(long, default_value_t = 12)]
    pub max_windows: usize,
    #[arg(long, default_value_t = 0)]
    pub seed_from: u64,
    #[arg(long, default_value_t = 64)]
    pub seed_count: u64,
    #[arg(long, default_value_t = 1)]
    pub seed_step: u64,
    #[arg(long, default_value_t = 1)]
    pub recipe_seed: u64,
    #[arg(long, default_value = "32,64")]
    pub chunk_sweep: String,
    #[arg(long, value_enum, default_value_t = ChunkSearchObjective::Raw)]
    pub chunk_search_objective: ChunkSearchObjective,
    #[arg(long, default_value_t = 1)]
    pub chunk_raw_slack: u64,
    #[arg(long, default_value_t = 0)]
    pub map_max_depth: u8,
    #[arg(long, default_value_t = 1)]
    pub map_depth_shift: u8,
    #[arg(long, default_value = "8,12")]
    pub boundary_band_sweep: String,
    #[arg(long, default_value_t = 1)]
    pub boundary_delta: usize,
    #[arg(long, default_value = "4,8")]
    pub field_margin_sweep: String,
    #[arg(long, default_value_t = 96)]
    pub newline_margin_add: u64,
    #[arg(long, default_value_t = 64)]
    pub space_to_newline_margin_add: u64,
    #[arg(long, default_value_t = 550_000)]
    pub newline_share_ppm_min: u32,
    #[arg(long, default_value_t = 0)]
    pub newline_override_budget: usize,
    #[arg(long, default_value = "0,4")]
    pub newline_demote_margin_sweep: String,
    #[arg(long, default_value_t = 150_000)]
    pub newline_demote_keep_ppm_min: u32,
    #[arg(long, default_value_t = 1)]
    pub newline_demote_keep_min: usize,
    #[arg(long, action = clap::ArgAction::Set, default_value_t = true)]
    pub newline_only_from_spacelike: bool,
    #[arg(long, default_value_t = false)]
    pub field_from_global: bool,
    #[arg(long, default_value_t = 0)]
    pub merge_gap_bytes: usize,
    #[arg(long, default_value_t = false)]
    pub allow_overlap_scout: bool,
    #[arg(long, value_enum, default_value_t = RenderFormat::Txt)]
    pub format: RenderFormat,
    #[arg(long)]
    pub out: Option<String>,
}

#[derive(Args, Debug)]
pub struct ApexLaneTableArgs {
    #[arg(long, default_value = "configs/tuned_validated.k8r")]
    pub recipe: String,
    #[arg(long, default_value = "text/Genesis1.txt")]
    pub genesis1: String,
    #[arg(long, default_value = "text/Genesis2.txt")]
    pub genesis2: String,
    #[arg(long, default_value_t = 0)]
    pub seed_from: u64,
    #[arg(long, default_value_t = 512)]
    pub seed_count: u64,
    #[arg(long, default_value_t = 1)]
    pub seed_step: u64,
    #[arg(long, default_value_t = 1)]
    pub recipe_seed: u64,
    #[arg(long, default_value = "64,128")]
    pub ws_chunk_sweep: String,
    #[arg(long, default_value = "32,64")]
    pub symbol_chunk_sweep: String,
    #[arg(long, default_value = "8,12,16")]
    pub boundary_band_sweep: String,
    #[arg(long, default_value = "4,8")]
    pub field_margin_sweep: String,
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
pub struct ApexMapCaseAnchorArgs {
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
    #[arg(long, default_value_t = 2)]
    pub hybrid_upper_consensus_min: usize,
    #[arg(long)]
    pub out_pred: Option<String>,
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
        ApexTraceCmd::ApexLaneAtlas(a) => apex_lane_atlas::run_apex_lane_atlas(a),
        ApexTraceCmd::ApexLaneLawFreeze(a) => apex_lane_law_freeze::run_apex_lane_law_freeze(a),
        ApexTraceCmd::ApexLaneLawSplitFreeze(a) => {
            apex_lane_law_split_freeze::run_apex_lane_law_split_freeze(a)
        }
        ApexTraceCmd::ApexLaneLawBridgeFreeze(a) => {
            apex_lane_law_bridge_freeze::run_apex_lane_law_bridge_freeze(a)
        }
        ApexTraceCmd::ApexLaneLawProfile(a) => {
            apex_lane_law_profile::run_apex_lane_law_profile(a)
        }
        ApexTraceCmd::ApexLaneManifest(a) => apex_lane_manifest::run_apex_lane_manifest(a),
        ApexTraceCmd::ApexLaneManifestCompare(a) => {
            apex_lane_manifest_compare::run_apex_lane_manifest_compare(a)
        }
        ApexTraceCmd::ApexLaneTable(a) => apex_lane_table::run_apex_lane_table(a),
        ApexTraceCmd::ApexMapLane(a) => apex_map_lane::run_apex_map_lane(a),
        ApexTraceCmd::ApexMapPunct(a) => apex_map_punct::run_apex_map_punct(a),
        ApexTraceCmd::ApexMapPunctKind(a) => apex_map_punct_kind::run_apex_map_punct_kind(a),
        ApexTraceCmd::ApexMapCase(a) => apex_map_case::run_apex_map_case(a),
        ApexTraceCmd::ApexMapCaseAnchor(a) => apex_map_case_anchor::run_apex_map_case_anchor(a),
        ApexTraceCmd::ApexMapDibit(a) => apex_map_dibit::run_apex_map_dibit(a),
        ApexTraceCmd::ApexMapDibitOther(a) => apex_map_dibit_other::run_apex_map_dibit_other(a),
    }
}
