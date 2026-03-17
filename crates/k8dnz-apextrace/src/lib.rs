pub mod dibit;
pub mod error;
pub mod fit;
pub mod generator;
pub mod key;
pub mod law;
pub mod render;

pub use dibit::{bytes_to_quats, quats_to_bytes};
pub use error::{ApexError, Result};
pub use fit::{
    analyze_key_against_bytes, analyze_quat_streams, brute_force_best, score_key_against_bytes,
    FitCandidate, FitDiagnostics, FitScore, SearchCfg,
};
pub use generator::{generate_bytes, generate_quats};
pub use key::{ceil_log2_u64, ApexKey, LAW_QDL1, MAGIC_ATK1, MODE_DIBIT_V1, VERSION_V1};
pub use render::{
    branch_name, point_xy, render_lattice, render_paths, render_subtree_stats, BaseLabel,
    LatticePoint, PathPoint, SubtreeStats,
};
