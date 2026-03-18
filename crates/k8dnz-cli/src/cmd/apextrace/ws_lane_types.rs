use std::collections::BTreeSet;

use k8dnz_apextrace::ApexKey;

use crate::cmd::apextrace::WsLaneArgs;

use super::common::match_pct;

pub const APEX_KEY_BYTES_EXACT: usize = 48;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WsLaneScore {
    pub matches: u64,
    pub prefix: u64,
    pub total: u64,
    pub longest_run: u64,
    pub longest_run_start: u64,
}

impl WsLaneScore {
    pub fn better_than(&self, other: &Self) -> bool {
        (self.matches, self.longest_run, self.prefix) > (other.matches, other.longest_run, other.prefix)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WsLaneDiagnostics {
    pub score: WsLaneScore,
    pub target_hist: [u64; 3],
    pub pred_hist: [u64; 3],
}

#[derive(Clone, Debug)]
pub struct WsLaneBest {
    pub key: ApexKey,
    pub predicted: Vec<u8>,
    pub diag: WsLaneDiagnostics,
}

#[derive(Clone, Debug)]
pub struct WsLaneChunkBest {
    pub chunk_index: usize,
    pub start: usize,
    pub end: usize,
    pub key: ApexKey,
    pub diag: WsLaneDiagnostics,
    pub patch_entries: usize,
    pub patch_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct WsLaneChunkedBest {
    pub chunk_bytes: usize,
    pub chunk_key_bytes_exact: usize,
    pub predicted: Vec<u8>,
    pub diag: WsLaneDiagnostics,
    pub chunks: Vec<WsLaneChunkBest>,
}

#[derive(Clone, Debug)]
pub struct WsLaneChunkReport {
    pub chunk_index: usize,
    pub start: usize,
    pub end: usize,
    pub len: usize,
    pub root_quadrant: u8,
    pub root_seed: u64,
    pub recipe_seed: u64,
    pub matches: u64,
    pub prefix: u64,
    pub total: u64,
    pub match_pct: f64,
    pub longest_run: u64,
    pub longest_run_start: u64,
    pub patch_entries: usize,
    pub patch_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct ChunkSnapshot {
    pub chunk_bytes: usize,
    pub chunk_key_bytes_exact: usize,
    pub patch_entries: usize,
    pub patch_bytes: usize,
    pub total_payload_exact: usize,
    pub diag: WsLaneDiagnostics,
    pub unique_key_count: usize,
    pub unique_seed_count: usize,
    pub chunk_reports: Vec<WsLaneChunkReport>,
}

#[derive(Clone, Debug)]
pub struct WsLaneReport {
    pub input: String,
    pub recipe: String,
    pub normalized_len: usize,
    pub class_len: usize,
    pub other_len: usize,
    pub baseline_artifact_bytes: usize,
    pub baseline_max_ticks_used: u64,
    pub baseline_class_mismatches: usize,
    pub baseline_class_patch_entries: usize,
    pub baseline_class_patch_bytes: usize,
    pub apex_byte_len: u64,
    pub apex_quat_len: u64,
    pub apex_depth: u16,
    pub apex_root_quadrant: u8,
    pub apex_root_seed: u64,
    pub apex_recipe_seed: u64,
    pub apex_key_bytes_exact: usize,
    pub apex_matches: u64,
    pub apex_prefix: u64,
    pub apex_total: u64,
    pub apex_match_pct: f64,
    pub apex_longest_run: u64,
    pub apex_longest_run_start: u64,
    pub apex_patch_entries: usize,
    pub apex_patch_bytes: usize,
    pub apex_total_payload_exact: usize,
    pub delta_patch_bytes: i64,
    pub delta_patch_entries: i64,
    pub delta_total_payload_exact_vs_baseline: i64,
    pub target_hist: [u64; 3],
    pub pred_hist: [u64; 3],
    pub chunk_bytes: Option<usize>,
    pub chunk_count: Option<usize>,
    pub chunk_key_bytes_exact: Option<usize>,
    pub chunk_patch_entries: Option<usize>,
    pub chunk_patch_bytes: Option<usize>,
    pub chunk_total_payload_exact: Option<usize>,
    pub chunk_unique_key_count: Option<usize>,
    pub chunk_unique_seed_count: Option<usize>,
    pub chunk_delta_patch_bytes_vs_baseline: Option<i64>,
    pub chunk_delta_patch_bytes_vs_global: Option<i64>,
    pub chunk_delta_total_payload_exact_vs_baseline: Option<i64>,
    pub chunk_delta_total_payload_exact_vs_global: Option<i64>,
    pub chunk_matches: Option<u64>,
    pub chunk_prefix: Option<u64>,
    pub chunk_total: Option<u64>,
    pub chunk_match_pct: Option<f64>,
    pub chunk_longest_run: Option<u64>,
    pub chunk_longest_run_start: Option<u64>,
    pub chunk_target_hist: Option<[u64; 3]>,
    pub chunk_pred_hist: Option<[u64; 3]>,
    pub chunk_reports: Vec<WsLaneChunkReport>,
}

#[derive(Clone, Debug)]
pub struct WsLaneSweepRow {
    pub chunk_bytes: usize,
    pub chunk_count: usize,
    pub key_bytes_exact: usize,
    pub patch_entries: usize,
    pub patch_bytes: usize,
    pub total_payload_exact: usize,
    pub unique_key_count: usize,
    pub unique_seed_count: usize,
    pub matches: u64,
    pub total: u64,
    pub match_pct: f64,
    pub longest_run: u64,
    pub delta_patch_vs_baseline: i64,
    pub delta_patch_vs_global: i64,
    pub delta_total_vs_baseline: i64,
    pub delta_total_vs_global: i64,
}

impl WsLaneReport {
    pub fn from_parts(
        args: &WsLaneArgs,
        normalized_len: usize,
        class_len: usize,
        other_len: usize,
        baseline_artifact_bytes: usize,
        baseline_max_ticks_used: u64,
        baseline_class_mismatches: usize,
        baseline_class_patch_entries: usize,
        baseline_class_patch_bytes: usize,
        best: &WsLaneBest,
        apex_patch_entries: usize,
        apex_patch_bytes: usize,
        chunk: Option<ChunkSnapshot>,
    ) -> Self {
        let apex_key_bytes_exact = APEX_KEY_BYTES_EXACT;
        let apex_total_payload_exact = apex_patch_bytes.saturating_add(apex_key_bytes_exact);
        Self {
            input: args.r#in.clone(),
            recipe: args.recipe.clone(),
            normalized_len,
            class_len,
            other_len,
            baseline_artifact_bytes,
            baseline_max_ticks_used,
            baseline_class_mismatches,
            baseline_class_patch_entries,
            baseline_class_patch_bytes,
            apex_byte_len: best.key.byte_len,
            apex_quat_len: best.key.quat_len,
            apex_depth: best.key.depth,
            apex_root_quadrant: best.key.root_quadrant,
            apex_root_seed: best.key.root_seed,
            apex_recipe_seed: best.key.recipe_seed,
            apex_key_bytes_exact,
            apex_matches: best.diag.score.matches,
            apex_prefix: best.diag.score.prefix,
            apex_total: best.diag.score.total,
            apex_match_pct: match_pct(best.diag.score.matches, best.diag.score.total),
            apex_longest_run: best.diag.score.longest_run,
            apex_longest_run_start: best.diag.score.longest_run_start,
            apex_patch_entries,
            apex_patch_bytes,
            apex_total_payload_exact,
            delta_patch_bytes: (apex_patch_bytes as i64) - (baseline_class_patch_bytes as i64),
            delta_patch_entries: (apex_patch_entries as i64) - (baseline_class_patch_entries as i64),
            delta_total_payload_exact_vs_baseline: (apex_total_payload_exact as i64) - (baseline_class_patch_bytes as i64),
            target_hist: best.diag.target_hist,
            pred_hist: best.diag.pred_hist,
            chunk_bytes: chunk.as_ref().map(|v| v.chunk_bytes),
            chunk_count: chunk.as_ref().map(|v| v.chunk_reports.len()),
            chunk_key_bytes_exact: chunk.as_ref().map(|v| v.chunk_key_bytes_exact),
            chunk_patch_entries: chunk.as_ref().map(|v| v.patch_entries),
            chunk_patch_bytes: chunk.as_ref().map(|v| v.patch_bytes),
            chunk_total_payload_exact: chunk.as_ref().map(|v| v.total_payload_exact),
            chunk_unique_key_count: chunk.as_ref().map(|v| v.unique_key_count),
            chunk_unique_seed_count: chunk.as_ref().map(|v| v.unique_seed_count),
            chunk_delta_patch_bytes_vs_baseline: chunk.as_ref().map(|v| (v.patch_bytes as i64) - (baseline_class_patch_bytes as i64)),
            chunk_delta_patch_bytes_vs_global: chunk.as_ref().map(|v| (v.patch_bytes as i64) - (apex_patch_bytes as i64)),
            chunk_delta_total_payload_exact_vs_baseline: chunk.as_ref().map(|v| (v.total_payload_exact as i64) - (baseline_class_patch_bytes as i64)),
            chunk_delta_total_payload_exact_vs_global: chunk.as_ref().map(|v| (v.total_payload_exact as i64) - (apex_total_payload_exact as i64)),
            chunk_matches: chunk.as_ref().map(|v| v.diag.score.matches),
            chunk_prefix: chunk.as_ref().map(|v| v.diag.score.prefix),
            chunk_total: chunk.as_ref().map(|v| v.diag.score.total),
            chunk_match_pct: chunk.as_ref().map(|v| match_pct(v.diag.score.matches, v.diag.score.total)),
            chunk_longest_run: chunk.as_ref().map(|v| v.diag.score.longest_run),
            chunk_longest_run_start: chunk.as_ref().map(|v| v.diag.score.longest_run_start),
            chunk_target_hist: chunk.as_ref().map(|v| v.diag.target_hist),
            chunk_pred_hist: chunk.as_ref().map(|v| v.diag.pred_hist),
            chunk_reports: chunk.map(|v| v.chunk_reports).unwrap_or_default(),
        }
    }
}

pub fn build_sweep_row(snapshot: &ChunkSnapshot, baseline_class_patch_bytes: usize, apex_patch_bytes: usize) -> WsLaneSweepRow {
    WsLaneSweepRow {
        chunk_bytes: snapshot.chunk_bytes,
        chunk_count: snapshot.chunk_reports.len(),
        key_bytes_exact: snapshot.chunk_key_bytes_exact,
        patch_entries: snapshot.patch_entries,
        patch_bytes: snapshot.patch_bytes,
        total_payload_exact: snapshot.total_payload_exact,
        unique_key_count: snapshot.unique_key_count,
        unique_seed_count: snapshot.unique_seed_count,
        matches: snapshot.diag.score.matches,
        total: snapshot.diag.score.total,
        match_pct: match_pct(snapshot.diag.score.matches, snapshot.diag.score.total),
        longest_run: snapshot.diag.score.longest_run,
        delta_patch_vs_baseline: (snapshot.patch_bytes as i64) - (baseline_class_patch_bytes as i64),
        delta_patch_vs_global: (snapshot.patch_bytes as i64) - (apex_patch_bytes as i64),
        delta_total_vs_baseline: (snapshot.total_payload_exact as i64) - (baseline_class_patch_bytes as i64),
        delta_total_vs_global: (snapshot.total_payload_exact as i64) - ((apex_patch_bytes + APEX_KEY_BYTES_EXACT) as i64),
    }
}

pub fn unique_counts(chunks: &[WsLaneChunkBest]) -> (usize, usize) {
    let mut keys = BTreeSet::new();
    let mut seeds = BTreeSet::new();
    for chunk in chunks {
        keys.insert((chunk.key.root_quadrant, chunk.key.root_seed, chunk.key.recipe_seed, chunk.key.byte_len));
        seeds.insert((chunk.key.root_quadrant, chunk.key.root_seed));
    }
    (keys.len(), seeds.len())
}
