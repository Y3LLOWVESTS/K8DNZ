// crates/k8dnz-cli/src/cmd/timemap/args.rs

use clap::{Args, Subcommand, ValueEnum};

#[derive(Copy, Clone, Debug, ValueEnum, PartialEq, Eq)]
pub enum ApplyMode {
    Pair,
    Rgbpair,
}

#[derive(Copy, Clone, Debug, ValueEnum, PartialEq, Eq)]
pub enum MapMode {
    None,
    Splitmix64,
    Ascii7,
    Ascii7Splitmix,

    Text40,
    Text40Weighted,

    Text40Lane,
    Text40Field,

    /// k-bit symbol stream derived from RGBPAIR emissions (vision-aligned "bits via field")
    Bitfield,

    Text64,
}

#[derive(Copy, Clone, Debug, ValueEnum, PartialEq, Eq)]
pub enum BitMapping {
    Geom,
    Hash,
}

#[derive(Copy, Clone, Debug, ValueEnum, PartialEq, Eq)]
pub enum FitObjective {
    Matches,
    Zstd,
}

#[derive(Copy, Clone, Debug, ValueEnum, PartialEq, Eq)]
pub enum ResidualMode {
    Xor,
    Sub,
}

/// Bitfield residual container encoding (only used when --map bitfield)
#[derive(Copy, Clone, Debug, ValueEnum, PartialEq, Eq)]
pub enum BitfieldResidualEncoding {
    /// BF1: packed symbols payload (current behavior)
    Packed,
    /// BF2: per-lane bitsets zstd-compressed separately (“time-split / lanes”)
    Lanes,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum SeedFmt {
    Text,
    Json,
}

#[derive(Copy, Clone, Debug, ValueEnum, PartialEq, Eq)]
pub enum TagFormat {
    /// 1 byte per tag
    Byte,
    /// Packed k-bit tags with TG1 header
    Packed,
}

#[derive(Args)]
pub struct TimemapArgs {
    #[command(subcommand)]
    pub cmd: TimemapCmd,
}

#[derive(Subcommand)]
pub enum TimemapCmd {
    Make(MakeArgs),
    Inspect(InspectArgs),

    /// Decode/inspect packed map_seed fields (decoder ring)
    MapSeed(MapSeedArgs),

    Apply(ApplyArgs),
    Fit(FitArgs),
    FitXor(FitXorArgs),
    FitXorChunked(FitXorChunkedArgs),
    Reconstruct(ReconstructArgs),

    /// Analyze BF1/BF2 bitfield residuals by splitting into per-symbol “lanes”
    BfLanes(BfLanesArgs),
}

#[derive(Args)]
pub struct MakeArgs {
    #[arg(long)]
    pub out: String,

    #[arg(long)]
    pub len: u64,

    #[arg(long, default_value_t = 0)]
    pub start: u64,

    #[arg(long, default_value_t = 1)]
    pub step: u64,
}

#[derive(Args)]
pub struct InspectArgs {
    #[arg(long)]
    pub r#in: String,
}

#[derive(Args)]
pub struct MapSeedArgs {
    /// mapping mode to interpret (decoder ring currently defined for text40-field)
    #[arg(long, value_enum, default_value_t = MapMode::Text40Field)]
    pub map: MapMode,

    /// seed as decimal u64
    #[arg(long, default_value_t = 0)]
    pub map_seed: u64,

    /// seed as hex (accepts "0x..." or raw hex). If set, overrides --map-seed.
    #[arg(long)]
    pub map_seed_hex: Option<String>,

    #[arg(long, value_enum, default_value_t = SeedFmt::Text)]
    pub fmt: SeedFmt,
}

#[derive(Args)]
pub struct ApplyArgs {
    #[arg(long)]
    pub recipe: String,

    #[arg(long)]
    pub timemap: String,

    #[arg(long)]
    pub out: String,

    #[arg(long, value_enum, default_value_t = ApplyMode::Pair)]
    pub mode: ApplyMode,

    #[arg(long, default_value_t = 50_000_000)]
    pub max_ticks: u64,
}

#[derive(Args)]
pub struct FitArgs {
    #[arg(long)]
    pub recipe: String,

    #[arg(long)]
    pub target: String,

    #[arg(long)]
    pub out: String,

    #[arg(long, default_value_t = 2_000_000)]
    pub search_emissions: u64,

    #[arg(long, default_value_t = 80_000_000)]
    pub max_ticks: u64,

    #[arg(long, default_value_t = 0)]
    pub start_emission: u64,
}

#[derive(Args)]
pub struct FitXorArgs {
    #[arg(long)]
    pub recipe: String,

    #[arg(long)]
    pub target: String,

    #[arg(long)]
    pub out_timemap: String,

    #[arg(long)]
    pub out_residual: String,

    #[arg(long, value_enum, default_value_t = ApplyMode::Pair)]
    pub mode: ApplyMode,

    #[arg(long, value_enum, default_value_t = MapMode::None)]
    pub map: MapMode,

    #[arg(long, default_value_t = 0)]
    pub map_seed: u64,

    /// seed as hex (accepts "0x..." or raw hex). If set, overrides --map-seed.
    #[arg(long)]
    pub map_seed_hex: Option<String>,

    #[arg(long, value_enum, default_value_t = ResidualMode::Xor)]
    pub residual: ResidualMode,

    #[arg(long, default_value_t = 2_000_000)]
    pub search_emissions: u64,

    #[arg(long, default_value_t = 80_000_000)]
    pub max_ticks: u64,

    #[arg(long, default_value_t = 0)]
    pub start_emission: u64,

    #[arg(long, default_value_t = 1)]
    pub scan_step: usize,

    #[arg(long, value_enum, default_value_t = FitObjective::Zstd)]
    pub objective: FitObjective,

    #[arg(long, default_value_t = 3)]
    pub zstd_level: i32,

    // -------- conditioning via tags --------
    #[arg(long)]
    pub cond_tags: Option<String>,

    #[arg(long, value_enum, default_value_t = TagFormat::Byte)]
    pub cond_tag_format: TagFormat,

    #[arg(long, default_value_t = 16)]
    pub cond_block_bytes: usize,

    #[arg(long, default_value_t = 0)]
    pub cond_seed: u64,

    #[arg(long)]
    pub cond_seed_hex: Option<String>,
}

#[derive(Args, Clone)]
pub struct FitXorChunkedArgs {
    #[arg(long)]
    pub recipe: String,

    #[arg(long)]
    pub target: String,

    #[arg(long)]
    pub out_timemap: String,

    #[arg(long)]
    pub out_residual: String,

    #[arg(long, value_enum, default_value_t = ApplyMode::Pair)]
    pub mode: ApplyMode,

    #[arg(long, value_enum, default_value_t = MapMode::None)]
    pub map: MapMode,

    #[arg(long, default_value_t = 0)]
    pub map_seed: u64,

    #[arg(long)]
    pub map_seed_hex: Option<String>,

    #[arg(long, value_enum, default_value_t = ResidualMode::Xor)]
    pub residual: ResidualMode,

    #[arg(long, default_value_t = 2_000_000)]
    pub search_emissions: u64,

    #[arg(long, default_value_t = 80_000_000)]
    pub max_ticks: u64,

    #[arg(long, default_value_t = 0)]
    pub start_emission: u64,

    #[arg(long, default_value_t = 1)]
    pub scan_step: usize,

    #[arg(long, default_value_t = 3)]
    pub zstd_level: i32,

    #[arg(long, default_value_t = 512)]
    pub chunk_size: usize,

    #[arg(long, default_value_t = 0)]
    pub max_chunks: usize,

    #[arg(long, value_enum, default_value_t = FitObjective::Matches)]
    pub objective: FitObjective,

    #[arg(long, default_value_t = 256)]
    pub refine_topk: usize,

    #[arg(long, default_value_t = 200_000)]
    pub lookahead: usize,

    /// Multiplier applied to tm jump-cost. (0 disables jump penalty; 1 = current behavior)
    #[arg(long, default_value_t = 1)]
    pub trans_penalty: u64,

    // -------- bitfield params (used only when --map bitfield) --------
    #[arg(long, default_value_t = 2)]
    pub bits_per_emission: u8,

    #[arg(long, value_enum, default_value_t = BitMapping::Geom)]
    pub bit_mapping: BitMapping,

    /// Bitfield residual encoding (BF1 packed or BF2 lanes). Only used when --map bitfield.
    #[arg(long, value_enum, default_value_t = BitfieldResidualEncoding::Packed)]
    pub bitfield_residual: BitfieldResidualEncoding,

    /// Convenience flag for the “time-split / lanes” experiment:
    /// same as `--bitfield-residual lanes`.
    #[arg(long, default_value_t = false)]
    pub time_split: bool,

    // -------- conditioning via tags (byte pipeline only) --------
    #[arg(long)]
    pub cond_tags: Option<String>,

    #[arg(long, value_enum, default_value_t = TagFormat::Byte)]
    pub cond_tag_format: TagFormat,

    #[arg(long, default_value_t = 16)]
    pub cond_block_bytes: usize,

    #[arg(long, default_value_t = 0)]
    pub cond_seed: u64,

    #[arg(long)]
    pub cond_seed_hex: Option<String>,
}

#[derive(Args)]
pub struct ReconstructArgs {
    #[arg(long)]
    pub recipe: String,

    #[arg(long)]
    pub timemap: String,

    #[arg(long)]
    pub residual: String,

    #[arg(long)]
    pub out: String,

    #[arg(long, value_enum, default_value_t = ApplyMode::Pair)]
    pub mode: ApplyMode,

    #[arg(long, value_enum, default_value_t = MapMode::None)]
    pub map: MapMode,

    #[arg(long, default_value_t = 0)]
    pub map_seed: u64,

    #[arg(long)]
    pub map_seed_hex: Option<String>,

    #[arg(long, value_enum, default_value_t = ResidualMode::Xor)]
    pub residual_mode: ResidualMode,

    #[arg(long, default_value_t = 80_000_000)]
    pub max_ticks: u64,

    // -------- bitfield params (used only when --map bitfield) --------
    #[arg(long, default_value_t = 2)]
    pub bits_per_emission: u8,

    #[arg(long, value_enum, default_value_t = BitMapping::Geom)]
    pub bit_mapping: BitMapping,

    // -------- conditioning via tags (byte pipeline only) --------
    #[arg(long)]
    pub cond_tags: Option<String>,

    #[arg(long, value_enum, default_value_t = TagFormat::Byte)]
    pub cond_tag_format: TagFormat,

    #[arg(long, default_value_t = 16)]
    pub cond_block_bytes: usize,

    #[arg(long, default_value_t = 0)]
    pub cond_seed: u64,

    #[arg(long)]
    pub cond_seed_hex: Option<String>,
}

#[derive(Args)]
pub struct BfLanesArgs {
    #[arg(long)]
    pub r#in: String,

    #[arg(long, default_value_t = 3)]
    pub zstd_level: i32,
}
