use crc32fast::Hasher;

use super::types::{
    BodyCandidateScore, LawProgramArtifact, ReplayEvalRow, ReplayFileSummary, SurfaceScoreboard,
};

pub(crate) fn render_artifact_report(
    artifact: &LawProgramArtifact,
    bytes: &[u8],
    path: &str,
    body_scores: Option<&[BodyCandidateScore]>,
) -> String {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    let crc32 = hasher.finalize();
    let summary = &artifact.summary;

    let mut out = String::new();
    out.push_str(&format!("artifact_path={}\n", path));
    out.push_str(&format!("artifact_bytes={}\n", bytes.len()));
    out.push_str(&format!("artifact_crc32=0x{:08X}\n", crc32));
    out.push_str(&format!("recipe={}\n", summary.recipe));
    out.push_str(&format!("file_count={}\n", summary.file_count));
    out.push_str(&format!("honest_file_count={}\n", summary.honest_file_count));
    out.push_str(&format!("union_law_count={}\n", summary.union_law_count));
    out.push_str(&format!("target_global_law_id={}\n", summary.target_global_law_id));
    out.push_str(&format!(
        "body_select_objective={}\n",
        artifact.config.body_select_objective
    ));
    out.push_str(&format!(
        "default_local_chunk_bytes={}\n",
        summary.default_local_chunk_bytes
    ));
    out.push_str(&format!(
        "searched_total_piecewise_payload_exact={}\n",
        summary.searched_total_piecewise_payload_exact
    ));
    out.push_str(&format!(
        "projected_default_total_piecewise_payload_exact={}\n",
        summary.projected_default_total_piecewise_payload_exact
    ));
    out.push_str(&format!(
        "projected_unpriced_best_mix_total_piecewise_payload_exact={}\n",
        summary.projected_unpriced_best_mix_total_piecewise_payload_exact
    ));
    out.push_str(&format!(
        "selected_total_piecewise_payload_exact={}\n",
        summary.selected_total_piecewise_payload_exact
    ));
    out.push_str("selected_total_semantics=canonical-fixed-replay\n");
    out.push_str(&format!(
        "closure_penalty_exact={}\n",
        summary.closure_penalty_exact
    ));
    out.push_str(&format!(
        "closure_total_exact={}\n",
        summary.closure_total_exact
    ));
    out.push_str(&format!(
        "override_path_mode={}\n",
        summary.override_path_mode
    ));
    out.push_str(&format!(
        "override_path_bytes_exact={}\n",
        summary.override_path_bytes_exact
    ));
    out.push_str(&format!("target_window_count={}\n", summary.target_window_count));
    out.push_str(&format!("window_count={}\n", artifact.windows.len()));
    out.push_str(&format!("override_count={}\n", artifact.overrides.len()));
    out.push_str(&format!(
        "closure_override_count={}\n",
        summary.closure_override_count
    ));
    out.push_str(&format!(
        "closure_override_run_count={}\n",
        summary.closure_override_run_count
    ));
    out.push_str(&format!(
        "closure_max_override_run_length={}\n",
        summary.closure_max_override_run_length
    ));
    out.push_str(&format!(
        "closure_untouched_window_count={}\n",
        summary.closure_untouched_window_count
    ));
    out.push_str(&format!(
        "closure_override_density_ppm={}\n",
        summary.closure_override_density_ppm
    ));
    out.push_str(&format!(
        "closure_untouched_window_pct_ppm={}\n",
        summary.closure_untouched_window_pct_ppm
    ));
    out.push_str(&format!(
        "closure_override_density_pct={:.6}\n",
        ppm_to_pct(summary.closure_override_density_ppm)
    ));
    out.push_str(&format!(
        "closure_untouched_window_pct={:.6}\n",
        ppm_to_pct(summary.closure_untouched_window_pct_ppm)
    ));
    out.push_str(&format!(
        "closure_mode_penalty_exact={}\n",
        summary.closure_mode_penalty_exact
    ));

    if let Some(body_scores) = body_scores {
        out.push_str("\n--- body-scoreboard ---\n");
        for row in body_scores {
            out.push_str(&format!(
                "chunk_bytes={} selected_total_piecewise_payload_exact={} closure_total_exact={} closure_penalty_exact={} mode_penalty_exact={} selected_target_window_payload_exact={} selected_override_window_count={} override_run_count={} max_override_run_length={} untouched_window_count={} override_density_ppm={} untouched_window_pct_ppm={} override_density_pct={:.6} untouched_window_pct={:.6} override_path_bytes_exact={} projected_default_total_piecewise_payload_exact={} target_window_count={}\n",
                row.chunk_bytes,
                row.selected_total_piecewise_payload_exact,
                row.closure_total_exact,
                row.closure_penalty_exact,
                row.mode_penalty_exact,
                row.selected_target_window_payload_exact,
                row.selected_override_window_count,
                row.override_run_count,
                row.max_override_run_length,
                row.untouched_window_count,
                row.override_density_ppm,
                row.untouched_window_pct_ppm,
                ppm_to_pct(row.override_density_ppm),
                ppm_to_pct(row.untouched_window_pct_ppm),
                row.override_path_bytes_exact,
                row.projected_default_total_piecewise_payload_exact,
                row.target_window_count,
            ));
        }
    }

    out.push_str("\n--- files ---\n");
    for file in &artifact.files {
        out.push_str(&format!(
            "input={} searched_total_piecewise_payload_exact={} projected_default_total_piecewise_payload_exact={} projected_unpriced_best_mix_total_piecewise_payload_exact={} selected_total_piecewise_payload_exact={} closure_total_exact={} target_window_count={} override_path_mode={} override_path_bytes_exact={} selected_override_window_count={} closure_override_count={} closure_override_run_count={} closure_max_override_run_length={} closure_untouched_window_count={} closure_override_density_ppm={} closure_untouched_window_pct_ppm={} closure_override_density_pct={:.6} closure_untouched_window_pct={:.6} closure_mode_penalty_exact={} closure_penalty_exact={}\n",
            file.input,
            file.searched_total_piecewise_payload_exact,
            file.projected_default_total_piecewise_payload_exact,
            file.projected_unpriced_best_mix_total_piecewise_payload_exact,
            file.selected_total_piecewise_payload_exact,
            file.closure_total_exact,
            file.target_window_count,
            file.override_path_mode,
            file.override_path_bytes_exact,
            file.selected_override_window_count,
            file.closure_override_count,
            file.closure_override_run_count,
            file.closure_max_override_run_length,
            file.closure_untouched_window_count,
            file.closure_override_density_ppm,
            file.closure_untouched_window_pct_ppm,
            ppm_to_pct(file.closure_override_density_ppm),
            ppm_to_pct(file.closure_untouched_window_pct_ppm),
            file.closure_mode_penalty_exact,
            file.closure_penalty_exact,
        ));
    }

    out.push_str("\n--- selected-overrides ---\n");
    for row in &artifact.overrides {
        out.push_str(&format!(
            "input={} window_idx={} target_ordinal={} best_chunk_bytes={} default_payload_exact={} best_payload_exact={} gain_exact={}\n",
            row.input,
            row.window_idx,
            row.target_ordinal,
            row.best_chunk_bytes,
            row.default_payload_exact,
            row.best_payload_exact,
            row.gain_exact,
        ));
    }

    out
}

pub(crate) fn render_replay_report(
    artifact_path: &str,
    artifact: &LawProgramArtifact,
    rows: &[ReplayEvalRow],
    file_summaries: &[ReplayFileSummary],
    replay_selected_total_piecewise_payload_exact: i64,
    drift_exact: i64,
    collapse_90_failures: usize,
    newline_extinct_failures: usize,
    scoreboard: Option<&SurfaceScoreboard>,
) -> String {
    let mut out = String::new();
    out.push_str(&format!("artifact_path={}\n", artifact_path));
    out.push_str(&format!(
        "target_global_law_id={}\n",
        artifact.summary.target_global_law_id
    ));
    out.push_str(&format!(
        "artifact_selected_total_piecewise_payload_exact={}\n",
        artifact.summary.selected_total_piecewise_payload_exact
    ));
    out.push_str("artifact_selected_total_semantics=canonical-fixed-replay\n");
    out.push_str(&format!(
        "artifact_closure_total_exact={}\n",
        artifact.summary.closure_total_exact
    ));
    out.push_str(&format!(
        "artifact_closure_penalty_exact={}\n",
        artifact.summary.closure_penalty_exact
    ));
    out.push_str(&format!(
        "artifact_closure_override_count={}\n",
        artifact.summary.closure_override_count
    ));
    out.push_str(&format!(
        "artifact_closure_override_run_count={}\n",
        artifact.summary.closure_override_run_count
    ));
    out.push_str(&format!(
        "artifact_closure_max_override_run_length={}\n",
        artifact.summary.closure_max_override_run_length
    ));
    out.push_str(&format!(
        "artifact_closure_untouched_window_count={}\n",
        artifact.summary.closure_untouched_window_count
    ));
    out.push_str(&format!(
        "artifact_closure_override_density_ppm={}\n",
        artifact.summary.closure_override_density_ppm
    ));
    out.push_str(&format!(
        "artifact_closure_untouched_window_pct_ppm={}\n",
        artifact.summary.closure_untouched_window_pct_ppm
    ));
    out.push_str(&format!(
        "artifact_override_path_mode={}\n",
        artifact.summary.override_path_mode
    ));
    out.push_str(&format!(
        "artifact_override_path_bytes_exact={}\n",
        artifact.summary.override_path_bytes_exact
    ));
    out.push_str(&format!(
        "replay_selected_total_piecewise_payload_exact={}\n",
        replay_selected_total_piecewise_payload_exact
    ));
    out.push_str(&format!("drift_exact={}\n", drift_exact));
    out.push_str(&format!("collapse_90_failures={}\n", collapse_90_failures));
    out.push_str(&format!(
        "newline_extinct_failures={}\n",
        newline_extinct_failures
    ));

    if let Some(scoreboard) = scoreboard {
        out.push_str("\n--- scoreboard ---\n");
        out.push_str(&format!(
            "searched_total_piecewise_payload_exact={}\n",
            scoreboard.searched_total_piecewise_payload_exact
        ));
        out.push_str(&format!(
            "artifact_selected_total_piecewise_payload_exact={}\n",
            scoreboard.artifact_selected_total_piecewise_payload_exact
        ));
        out.push_str(&format!(
            "replay_selected_total_piecewise_payload_exact={}\n",
            scoreboard.replay_selected_total_piecewise_payload_exact
        ));
        if let Some(v) = scoreboard.frozen_total_piecewise_payload_exact {
            out.push_str(&format!("frozen_total_piecewise_payload_exact={}\n", v));
        }
        if let Some(v) = scoreboard.split_total_piecewise_payload_exact {
            out.push_str(&format!("split_total_piecewise_payload_exact={}\n", v));
        }
        if let Some(v) = scoreboard.bridge_total_piecewise_payload_exact {
            out.push_str(&format!("bridge_total_piecewise_payload_exact={}\n", v));
        }
        out.push_str(&format!("best_surface={}\n", scoreboard.best_surface));
        out.push_str(&format!(
            "best_total_piecewise_payload_exact={}\n",
            scoreboard.best_total_piecewise_payload_exact
        ));
        out.push_str(&format!(
            "best_delta_vs_artifact_exact={}\n",
            scoreboard.best_delta_vs_artifact_exact
        ));
    }

    out.push_str("\n--- files ---\n");
    for file in file_summaries {
        out.push_str(&format!(
            "input={} searched_total_piecewise_payload_exact={} artifact_selected_total_piecewise_payload_exact={} replay_selected_total_piecewise_payload_exact={} searched_target_window_payload_exact={} artifact_selected_target_window_payload_exact={} replay_target_window_payload_exact={} override_path_bytes_exact={} target_window_count={} drift_exact={} improved_vs_searched_count={} equal_vs_searched_count={} worsened_vs_searched_count={}\n",
            file.input,
            file.searched_total_piecewise_payload_exact,
            file.artifact_selected_total_piecewise_payload_exact,
            file.replay_selected_total_piecewise_payload_exact,
            file.searched_target_window_payload_exact,
            file.artifact_selected_target_window_payload_exact,
            file.replay_target_window_payload_exact,
            file.override_path_bytes_exact,
            file.target_window_count,
            file.drift_exact,
            file.improved_vs_searched_count,
            file.equal_vs_searched_count,
            file.worsened_vs_searched_count,
        ));
    }

    let mut drifts = rows.to_vec();
    drifts.sort_by_key(|row| {
        (
            std::cmp::Reverse(row.delta_vs_artifact_exact.abs()),
            row.input_index,
            row.window_idx,
        )
    });

    out.push_str("\n--- largest-window-drifts ---\n");
    for row in drifts.into_iter().take(12) {
        out.push_str(&format!(
            "input={} window_idx={} target_ordinal={} start={} end={} selected_chunk_bytes={} searched_payload_exact={} artifact_selected_payload_exact={} replay_payload_exact={} delta_vs_artifact_exact={} delta_vs_searched_exact={} field_match_pct={:.6} collapse_90_flag={} newline_extinct_flag={} newline_floor_used={}\n",
            row.input,
            row.window_idx,
            row.target_ordinal,
            row.start,
            row.end,
            row.selected_chunk_bytes,
            row.searched_payload_exact,
            row.artifact_selected_payload_exact,
            row.replay_payload_exact,
            row.delta_vs_artifact_exact,
            row.delta_vs_searched_exact,
            row.field_match_pct,
            row.collapse_90_flag,
            row.newline_extinct_flag,
            row.newline_floor_used,
        ));
    }

    let mut ordered = rows.to_vec();
    ordered.sort_by_key(|row| (row.input_index, row.target_ordinal, row.window_idx));
    out.push_str("\n--- all-window-reconciliation ---\n");
    for row in ordered {
        out.push_str(&format!(
            "input={} window_idx={} target_ordinal={} start={} end={} selected_chunk_bytes={} searched_payload_exact={} artifact_selected_payload_exact={} replay_payload_exact={} delta_vs_artifact_exact={} delta_vs_searched_exact={} field_match_pct={:.6} collapse_90_flag={} newline_extinct_flag={} newline_floor_used={}\n",
            row.input,
            row.window_idx,
            row.target_ordinal,
            row.start,
            row.end,
            row.selected_chunk_bytes,
            row.searched_payload_exact,
            row.artifact_selected_payload_exact,
            row.replay_payload_exact,
            row.delta_vs_artifact_exact,
            row.delta_vs_searched_exact,
            row.field_match_pct,
            row.collapse_90_flag,
            row.newline_extinct_flag,
            row.newline_floor_used,
        ));
    }

    out
}

fn ppm_to_pct(ppm: u32) -> f64 {
    ppm as f64 / 10_000.0
}
