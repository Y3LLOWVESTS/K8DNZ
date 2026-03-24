use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "apex_law_program")]
#[command(about = "Build, inspect, and replay deterministic K8DNZ law-program artifacts")]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub(crate) cmd: Cmd,
}

#[derive(Subcommand, Debug)]
pub(crate) enum Cmd {
    Build(BuildArgs),
    Inspect(InspectArgs),
    Replay(ReplayArgs),
}

#[derive(Parser, Debug, Clone)]
pub(crate) struct BuildArgs {
    #[arg(long, default_value = "configs/tuned_validated.k8r")]
    pub(crate) recipe: String,
    #[arg(long = "in", required = true, num_args = 1..)]
    pub(crate) inputs: Vec<String>,
    #[arg(long, default_value_t = 20_000_000)]
    pub(crate) max_ticks: u64,
    #[arg(long, default_value_t = 256)]
    pub(crate) window_bytes: usize,
    #[arg(long, default_value_t = 256)]
    pub(crate) step_bytes: usize,
    #[arg(long, default_value_t = 12)]
    pub(crate) max_windows: usize,
    #[arg(long, default_value_t = 0)]
    pub(crate) seed_from: u64,
    #[arg(long, default_value_t = 64)]
    pub(crate) seed_count: u64,
    #[arg(long, default_value_t = 1)]
    pub(crate) seed_step: u64,
    #[arg(long, default_value_t = 1)]
    pub(crate) recipe_seed: u64,
    #[arg(long, default_value = "32,64")]
    pub(crate) chunk_sweep: String,
    #[arg(long, default_value = "raw")]
    pub(crate) chunk_search_objective: String,
    #[arg(long, default_value_t = 1)]
    pub(crate) chunk_raw_slack: u64,
    #[arg(long, default_value_t = 0)]
    pub(crate) map_max_depth: u8,
    #[arg(long, default_value_t = 1)]
    pub(crate) map_depth_shift: u8,
    #[arg(long, default_value = "8,12")]
    pub(crate) boundary_band_sweep: String,
    #[arg(long, default_value_t = 1)]
    pub(crate) boundary_delta: usize,
    #[arg(long, default_value = "4,8")]
    pub(crate) field_margin_sweep: String,
    #[arg(long, default_value_t = 96)]
    pub(crate) newline_margin_add: u64,
    #[arg(long, default_value_t = 64)]
    pub(crate) space_to_newline_margin_add: u64,
    #[arg(long, default_value_t = 550_000)]
    pub(crate) newline_share_ppm_min: u32,
    #[arg(long, default_value_t = 0)]
    pub(crate) newline_override_budget: usize,
    #[arg(long, default_value = "0,4")]
    pub(crate) newline_demote_margin_sweep: String,
    #[arg(long, default_value_t = 150_000)]
    pub(crate) newline_demote_keep_ppm_min: u32,
    #[arg(long, default_value_t = 1)]
    pub(crate) newline_demote_keep_min: usize,
    #[arg(long, action = clap::ArgAction::Set, default_value_t = true)]
    pub(crate) newline_only_from_spacelike: bool,
    #[arg(long, default_value_t = 0)]
    pub(crate) merge_gap_bytes: usize,
    #[arg(long, default_value_t = false)]
    pub(crate) allow_overlap_scout: bool,
    #[arg(long)]
    pub(crate) freeze_boundary_band: Option<usize>,
    #[arg(long)]
    pub(crate) freeze_field_margin: Option<u64>,
    #[arg(long)]
    pub(crate) freeze_newline_demote_margin: Option<u64>,
    #[arg(long, default_value = "32,64,96,128")]
    pub(crate) local_chunk_sweep: String,
    #[arg(long)]
    pub(crate) local_chunk_search_objective: Option<String>,
    #[arg(long)]
    pub(crate) local_chunk_raw_slack: Option<u64>,
    #[arg(long)]
    pub(crate) default_local_chunk_bytes: Option<usize>,
    #[arg(long, default_value_t = false)]
    pub(crate) tune_default_body: bool,
    #[arg(long)]
    pub(crate) default_body_chunk_sweep: Option<String>,
    #[arg(long, default_value = "selected-total")]
    pub(crate) body_select_objective: String,
    #[arg(long, default_value_t = false)]
    pub(crate) emit_body_scoreboard: bool,
    #[arg(long, default_value_t = 1)]
    pub(crate) min_override_gain_exact: usize,
    #[arg(long, default_value_t = 12)]
    pub(crate) exact_subset_limit: usize,
    #[arg(long)]
    pub(crate) global_law_id: Option<String>,
    #[arg(long, default_value_t = 12)]
    pub(crate) top_rows: usize,
    #[arg(long)]
    pub(crate) out: String,
    #[arg(long)]
    pub(crate) out_report: Option<String>,
}

#[derive(Parser, Debug)]
pub(crate) struct InspectArgs {
    #[arg(long)]
    pub(crate) artifact: String,
}

#[derive(Parser, Debug)]
pub(crate) struct ReplayArgs {
    #[arg(long)]
    pub(crate) artifact: String,
    #[arg(long)]
    pub(crate) out_report: Option<String>,
    #[arg(long, default_value_t = false)]
    pub(crate) compare_surfaces: bool,
}
