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

    /// NEW: low-pass intensity -> IIR smooth -> threshold (bits_per_emission must be 1)
    LowpassThresh,
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

/// Optional per-chunk transform applied to predicted symbols before residual.
/// Data-agnostic knob: rotate symbol alphabet by k (mod 2^b).
#[derive(Copy, Clone, Debug, ValueEnum, PartialEq, Eq)]
pub enum ChunkXform {
    None,
    /// For each chunk choose k in [0, 2^b) and use pred' = (pred + k) mod 2^b.
    Addk,
}

/// selects which "law" generates tm indices in `timemap gen-law`.
#[derive(Copy, Clone, Debug, ValueEnum, PartialEq, Eq)]
pub enum LawType {
    /// Existing phase/jump-walk law (single contiguous window chosen by offset_total).
    JumpWalk,
    /// Closed-form per-chunk start_pos(k) (structured revisits / gear formula).
    ClosedForm,
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

    /// Generate a timemap from a deterministic “law” (no scanning). (MVP: bitfield only)
    GenLaw(GenLawArgs),

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

    /// NEW: Lowpass threshold (only used for bit_mapping=lowpass-thresh)
    #[arg(long, default_value_t = 128)]
    pub bit_tau: u8,

    /// NEW: Lowpass IIR smooth strength (0 disables, larger = smoother). Only used for lowpass-thresh.
    #[arg(long, default_value_t = 3)]
    pub bit_smooth_shift: u8,

    /// Bitfield residual encoding (BF1 packed or BF2 lanes). Only used when --map bitfield.
    #[arg(long, value_enum, default_value_t = BitfieldResidualEncoding::Packed)]
    pub bitfield_residual: BitfieldResidualEncoding,

    /// Convenience flag for the “time-split / lanes” experiment:
    /// same as `--bitfield-residual lanes`.
    #[arg(long, default_value_t = false)]
    pub time_split: bool,

    /// Optional per-chunk transform applied to predicted symbols before residual.
    #[arg(long, value_enum, default_value_t = ChunkXform::None)]
    pub chunk_xform: ChunkXform,

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

#[derive(Args, Clone)]
pub struct GenLawArgs {
    #[arg(long)]
    pub recipe: String,

    #[arg(long)]
    pub target: String,

    #[arg(long)]
    pub out_timemap: String,

    #[arg(long)]
    pub out_residual: String,

    /// MVP: must be rgbpair for bitfield
    #[arg(long, value_enum, default_value_t = ApplyMode::Rgbpair)]
    pub mode: ApplyMode,

    /// MVP: only bitfield implemented
    #[arg(long, value_enum, default_value_t = MapMode::Bitfield)]
    pub map: MapMode,

    /// map seed (used by bit_mapping hash)
    #[arg(long, default_value_t = 0)]
    pub map_seed: u64,

    /// seed as hex (accepts "0x..." or raw hex). If set, overrides --map-seed.
    #[arg(long)]
    pub map_seed_hex: Option<String>,

    #[arg(long, value_enum, default_value_t = ResidualMode::Xor)]
    pub residual: ResidualMode,

    #[arg(long, default_value_t = 3)]
    pub zstd_level: i32,

    /// hard cap on emissions we can consume (upper bound)
    #[arg(long, default_value_t = 2_000_000)]
    pub search_emissions: u64,

    /// hard cap on ticks
    #[arg(long, default_value_t = 80_000_000)]
    pub max_ticks: u64,

    /// start emission index in the engine stream
    #[arg(long, default_value_t = 0)]
    pub start_emission: u64,

    // ---- bitfield params ----
    #[arg(long, default_value_t = 1)]
    pub bits_per_emission: u8,

    #[arg(long, value_enum, default_value_t = BitMapping::Geom)]
    pub bit_mapping: BitMapping,

    /// NEW: Lowpass threshold (only used for bit_mapping=lowpass-thresh)
    #[arg(long, default_value_t = 128)]
    pub bit_tau: u8,

    /// NEW: Lowpass IIR smooth strength (0 disables, larger = smoother). Only used for lowpass-thresh.
    #[arg(long, default_value_t = 3)]
    pub bit_smooth_shift: u8,

    /// Bitfield residual encoding (BF1 packed or BF2 lanes).
    #[arg(long, value_enum, default_value_t = BitfieldResidualEncoding::Packed)]
    pub bitfield_residual: BitfieldResidualEncoding,

    /// Convenience flag for BF2 lanes.
    #[arg(long, default_value_t = false)]
    pub time_split: bool,

    /// Optional per-chunk transform applied to predicted symbols before residual.
    #[arg(long, value_enum, default_value_t = ChunkXform::None)]
    pub chunk_xform: ChunkXform,

    /// Chunk size used when chunk_xform=addk (and for reporting and/or per-chunk law).
    #[arg(long, default_value_t = 4096)]
    pub chunk_size: usize,

    // ---- law selector ----
    /// Which law implementation to use to generate indices.
    #[arg(long, value_enum, default_value_t = LawType::JumpWalk)]
    pub law_type: LawType,

    // ---- “law” params (JumpWalk Θ) ----
    /// Primary seed for the law (separate from map_seed).
    #[arg(long, default_value_t = 1)]
    pub law_seed: u64,

    /// law_seed as hex (accepts "0x..." or raw hex). If set, overrides --law-seed.
    #[arg(long)]
    pub law_seed_hex: Option<String>,

    /// Alignment window ε in Turn32 units (u32 wrap space).
    #[arg(long, default_value_t = 1024)]
    pub law_epsilon: u32,

    /// Orbit velocity for A per step (Turn32 units).
    #[arg(long, default_value_t = 1_234_567)]
    pub law_v_a: u32,

    /// Orbit velocity for C per step (Turn32 units).
    #[arg(long, default_value_t = 2_345_678)]
    pub law_v_c: u32,

    /// Lockstep velocity (phase advance while “locked”).
    #[arg(long, default_value_t = 3_456_789)]
    pub law_v_l: u32,

    /// Δ between paired points (Turn32). Default 0.5 turns.
    #[arg(long, default_value_t = 0x8000_0000)]
    pub law_delta: u32,

    /// “Pitch” influences jump selection.
    #[arg(long, default_value_t = 12_345)]
    pub law_pitch: u32,

    /// Maximum emission jump per symbol (>=1). Used only for --law-type jumpwalk.
    #[arg(long, default_value_t = 4096)]
    pub law_max_jump: u32,

    /// When locked, divide max_jump by this factor (>=1). Used only for --law-type jumpwalk.
    #[arg(long, default_value_t = 4)]
    pub law_lock_div: u32,

    // ---- ClosedForm params (formula-driven per-chunk start_pos(k)) ----
    /// Closed-form offset b (start phase). Used only for --law-type closedform.
    #[arg(long, default_value_t = 0)]
    pub law_cf_b: i64,

    /// Closed-form slope a (linear stepping). Used only for --law-type closedform.
    #[arg(long, default_value_t = 1)]
    pub law_cf_a: i64,

    /// Closed-form curvature c (multiplies k*(k-1)/2). Used only for --law-type closedform.
    #[arg(long, default_value_t = 0)]
    pub law_cf_c: i64,

    /// Gear 1 period P1 (0 disables). Used only for --law-type closedform.
    #[arg(long, default_value_t = 0)]
    pub law_cf_p1: u64,

    /// Gear 1 amplitude g1 (0 disables). Used only for --law-type closedform.
    #[arg(long, default_value_t = 0)]
    pub law_cf_g1: i64,

    /// Gear 1 phase φ1. Used only for --law-type closedform.
    #[arg(long, default_value_t = 0)]
    pub law_cf_phi1: u64,

    /// Gear 2 period P2 (0 disables). Used only for --law-type closedform.
    #[arg(long, default_value_t = 0)]
    pub law_cf_p2: u64,

    /// Gear 2 amplitude g2 (0 disables). Used only for --law-type closedform.
    #[arg(long, default_value_t = 0)]
    pub law_cf_g2: i64,

    /// Gear 2 phase φ2. Used only for --law-type closedform.
    #[arg(long, default_value_t = 0)]
    pub law_cf_phi2: u64,
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

    /// NEW: Lowpass threshold (only used for bit_mapping=lowpass-thresh)
    #[arg(long, default_value_t = 128)]
    pub bit_tau: u8,

    /// NEW: Lowpass IIR smooth strength (0 disables, larger = smoother). Only used for lowpass-thresh.
    #[arg(long, default_value_t = 3)]
    pub bit_smooth_shift: u8,

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
