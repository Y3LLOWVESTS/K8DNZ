use crate::cmd::apextrace::RenderFormat;

use super::types::{FileSummary, LocalMixSummary, OverrideCandidate, WindowEvalRow};

pub(crate) fn render_body(
    format: RenderFormat,
    summary: &LocalMixSummary,
    file_summaries: &[FileSummary],
    window_rows: &[WindowEvalRow],
    override_candidates: &[OverrideCandidate],
    override_selected: &[OverrideCandidate],
    top_rows: usize,
) -> String {
    match format {
        RenderFormat::Txt => render_txt(
            summary,
            file_summaries,
            window_rows,
            override_candidates,
            override_selected,
            top_rows,
        ),
        RenderFormat::Csv => render_csv(
            summary,
            file_summaries,
            window_rows,
            override_candidates,
            override_selected,
        ),
    }
}

fn render_txt(
    summary: &LocalMixSummary,
    file_summaries: &[FileSummary],
    window_rows: &[WindowEvalRow],
    override_candidates: &[OverrideCandidate],
    override_selected: &[OverrideCandidate],
    top_rows: usize,
) -> String {
    let mut out = String::new();
    push_line(&mut out, "recipe", &summary.recipe);
    push_line(&mut out, "file_count", summary.file_count);
    push_line(&mut out, "honest_file_count", summary.honest_file_count);
    push_line(&mut out, "union_law_count", summary.union_law_count);
    push_line(&mut out, "target_global_law_id", &summary.target_global_law_id);
    push_line(&mut out, "target_global_law_path_hits", summary.target_global_law_path_hits);
    push_line(&mut out, "target_global_law_file_count", summary.target_global_law_file_count);
    push_line(&mut out, "target_global_law_total_window_count", summary.target_global_law_total_window_count);
    push_line(&mut out, "target_global_law_total_segment_count", summary.target_global_law_total_segment_count);
    push_line(&mut out, "target_global_law_total_covered_bytes", summary.target_global_law_total_covered_bytes);
    push_line(
        &mut out,
        "target_global_law_dominant_knob_signature",
        summary.target_global_law_dominant_knob_signature.replace(' ', "|"),
    );
    push_line(&mut out, "eval_boundary_band", summary.eval_boundary_band);
    push_line(&mut out, "eval_field_margin", summary.eval_field_margin);
    push_line(&mut out, "eval_newline_demote_margin", summary.eval_newline_demote_margin);
    push_line(&mut out, "eval_chunk_search_objective", &summary.eval_chunk_search_objective);
    push_line(&mut out, "eval_chunk_raw_slack", summary.eval_chunk_raw_slack);
    push_line(&mut out, "eval_chunk_candidates", &summary.eval_chunk_candidates);
    push_line(&mut out, "eval_chunk_candidate_count", summary.eval_chunk_candidate_count);
    push_line(&mut out, "default_local_chunk_bytes", summary.default_local_chunk_bytes);
    push_line(&mut out, "default_local_chunk_window_wins", summary.default_local_chunk_window_wins);
    push_line(&mut out, "searched_total_piecewise_payload_exact", summary.searched_total_piecewise_payload_exact);
    push_line(&mut out, "projected_default_total_piecewise_payload_exact", summary.projected_default_total_piecewise_payload_exact);
    push_line(&mut out, "delta_default_total_piecewise_payload_exact", summary.delta_default_total_piecewise_payload_exact);
    push_line(&mut out, "projected_unpriced_best_mix_total_piecewise_payload_exact", summary.projected_unpriced_best_mix_total_piecewise_payload_exact);
    push_line(&mut out, "delta_unpriced_best_mix_total_piecewise_payload_exact", summary.delta_unpriced_best_mix_total_piecewise_payload_exact);
    push_line(&mut out, "selected_total_piecewise_payload_exact", summary.selected_total_piecewise_payload_exact);
    push_line(&mut out, "delta_selected_total_piecewise_payload_exact", summary.delta_selected_total_piecewise_payload_exact);
    push_line(&mut out, "target_window_count", summary.target_window_count);
    push_line(&mut out, "searched_target_window_payload_exact", summary.searched_target_window_payload_exact);
    push_line(&mut out, "default_target_window_payload_exact", summary.default_target_window_payload_exact);
    push_line(&mut out, "best_mix_target_window_payload_exact", summary.best_mix_target_window_payload_exact);
    push_line(&mut out, "selected_target_window_payload_exact", summary.selected_target_window_payload_exact);
    push_line(&mut out, "delta_selected_target_window_payload_exact", summary.delta_selected_target_window_payload_exact);
    push_line(&mut out, "override_path_bytes_exact", summary.override_path_bytes_exact);
    push_line(&mut out, "selected_override_window_count", summary.selected_override_window_count);
    push_line(&mut out, "improved_target_window_count", summary.improved_target_window_count);
    push_line(&mut out, "equal_target_window_count", summary.equal_target_window_count);
    push_line(&mut out, "worsened_target_window_count", summary.worsened_target_window_count);
    push_line(&mut out, "best_gain_input", &summary.best_gain_input);
    push_line(&mut out, "best_gain_window_idx", summary.best_gain_window_idx);
    push_line(&mut out, "best_gain_delta_payload_exact", summary.best_gain_delta_payload_exact);
    push_line(&mut out, "worst_loss_input", &summary.worst_loss_input);
    push_line(&mut out, "worst_loss_window_idx", summary.worst_loss_window_idx);
    push_line(&mut out, "worst_loss_delta_payload_exact", summary.worst_loss_delta_payload_exact);

    out.push_str("\n--- files ---\n");
    for file in file_summaries {
        out.push_str(&format!(
            "input={} searched_total_piecewise_payload_exact={} projected_default_total_piecewise_payload_exact={} delta_default_total_piecewise_payload_exact={} projected_unpriced_best_mix_total_piecewise_payload_exact={} delta_unpriced_best_mix_total_piecewise_payload_exact={} selected_total_piecewise_payload_exact={} delta_selected_total_piecewise_payload_exact={} target_window_count={} searched_target_window_payload_exact={} default_target_window_payload_exact={} best_mix_target_window_payload_exact={} selected_target_window_payload_exact={} delta_selected_target_window_payload_exact={} override_path_bytes_exact={} selected_override_window_count={} improved_target_window_count={} equal_target_window_count={} worsened_target_window_count={}\n",
            file.input,
            file.searched_total_piecewise_payload_exact,
            file.projected_default_total_piecewise_payload_exact,
            file.delta_default_total_piecewise_payload_exact,
            file.projected_unpriced_best_mix_total_piecewise_payload_exact,
            file.delta_unpriced_best_mix_total_piecewise_payload_exact,
            file.selected_total_piecewise_payload_exact,
            file.delta_selected_total_piecewise_payload_exact,
            file.target_window_count,
            file.searched_target_window_payload_exact,
            file.default_target_window_payload_exact,
            file.best_mix_target_window_payload_exact,
            file.selected_target_window_payload_exact,
            file.delta_selected_target_window_payload_exact,
            file.override_path_bytes_exact,
            file.selected_override_window_count,
            file.improved_target_window_count,
            file.equal_target_window_count,
            file.worsened_target_window_count,
        ));
    }

    let mut candidate_rows = override_candidates.to_vec();
    candidate_rows.sort_by_key(|row| std::cmp::Reverse(row.gain_exact));
    out.push_str("\n--- top-override-candidates ---\n");
    for row in candidate_rows.into_iter().take(top_rows) {
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

    let mut selected_rows = override_selected.to_vec();
    selected_rows.sort_by_key(|row| row.window_idx);
    out.push_str("\n--- selected-overrides ---\n");
    for row in selected_rows {
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

    let mut deltas = window_rows.to_vec();
    deltas.sort_by_key(|row| (std::cmp::Reverse(row.selected_gain_exact), row.window_idx));
    out.push_str("\n--- top-window-gains ---\n");
    for row in deltas.into_iter().filter(|row| row.selected_gain_exact > 0).take(top_rows) {
        out.push_str(&format!(
            "input={} window_idx={} target_ordinal={} searched_payload_exact={} default_payload_exact={} best_payload_exact={} selected_payload_exact={} searched_chunk_bytes={} default_chunk_bytes={} best_chunk_bytes={} selected_chunk_bytes={} selected_gain_exact={}\n",
            row.input,
            row.window_idx,
            row.target_ordinal,
            row.searched_payload_exact,
            row.default_payload_exact,
            row.best_payload_exact,
            row.selected_payload_exact,
            row.searched_chunk_bytes,
            summary.default_local_chunk_bytes,
            row.best_chunk_bytes,
            row.selected_chunk_bytes,
            row.selected_gain_exact,
        ));
    }

    out
}

fn render_csv(
    summary: &LocalMixSummary,
    file_summaries: &[FileSummary],
    window_rows: &[WindowEvalRow],
    override_candidates: &[OverrideCandidate],
    override_selected: &[OverrideCandidate],
) -> String {
    let mut out = String::new();
    push_csv_row(&mut out, &["section","recipe","file_count","honest_file_count","union_law_count","target_global_law_id","target_global_law_path_hits","target_global_law_file_count","target_global_law_total_window_count","target_global_law_total_segment_count","target_global_law_total_covered_bytes","target_global_law_dominant_knob_signature","eval_boundary_band","eval_field_margin","eval_newline_demote_margin","eval_chunk_search_objective","eval_chunk_raw_slack","eval_chunk_candidates","eval_chunk_candidate_count","default_local_chunk_bytes","default_local_chunk_window_wins","searched_total_piecewise_payload_exact","projected_default_total_piecewise_payload_exact","delta_default_total_piecewise_payload_exact","projected_unpriced_best_mix_total_piecewise_payload_exact","delta_unpriced_best_mix_total_piecewise_payload_exact","selected_total_piecewise_payload_exact","delta_selected_total_piecewise_payload_exact","target_window_count","searched_target_window_payload_exact","default_target_window_payload_exact","best_mix_target_window_payload_exact","selected_target_window_payload_exact","delta_selected_target_window_payload_exact","override_path_bytes_exact","selected_override_window_count","improved_target_window_count","equal_target_window_count","worsened_target_window_count","best_gain_input","best_gain_window_idx","best_gain_delta_payload_exact","worst_loss_input","worst_loss_window_idx","worst_loss_delta_payload_exact"]);
    push_csv_row(&mut out, &["summary",&summary.recipe,&summary.file_count.to_string(),&summary.honest_file_count.to_string(),&summary.union_law_count.to_string(),&summary.target_global_law_id,&summary.target_global_law_path_hits.to_string(),&summary.target_global_law_file_count.to_string(),&summary.target_global_law_total_window_count.to_string(),&summary.target_global_law_total_segment_count.to_string(),&summary.target_global_law_total_covered_bytes.to_string(),&summary.target_global_law_dominant_knob_signature,&summary.eval_boundary_band.to_string(),&summary.eval_field_margin.to_string(),&summary.eval_newline_demote_margin.to_string(),&summary.eval_chunk_search_objective,&summary.eval_chunk_raw_slack.to_string(),&summary.eval_chunk_candidates,&summary.eval_chunk_candidate_count.to_string(),&summary.default_local_chunk_bytes.to_string(),&summary.default_local_chunk_window_wins.to_string(),&summary.searched_total_piecewise_payload_exact.to_string(),&summary.projected_default_total_piecewise_payload_exact.to_string(),&summary.delta_default_total_piecewise_payload_exact.to_string(),&summary.projected_unpriced_best_mix_total_piecewise_payload_exact.to_string(),&summary.delta_unpriced_best_mix_total_piecewise_payload_exact.to_string(),&summary.selected_total_piecewise_payload_exact.to_string(),&summary.delta_selected_total_piecewise_payload_exact.to_string(),&summary.target_window_count.to_string(),&summary.searched_target_window_payload_exact.to_string(),&summary.default_target_window_payload_exact.to_string(),&summary.best_mix_target_window_payload_exact.to_string(),&summary.selected_target_window_payload_exact.to_string(),&summary.delta_selected_target_window_payload_exact.to_string(),&summary.override_path_bytes_exact.to_string(),&summary.selected_override_window_count.to_string(),&summary.improved_target_window_count.to_string(),&summary.equal_target_window_count.to_string(),&summary.worsened_target_window_count.to_string(),&summary.best_gain_input,&summary.best_gain_window_idx.to_string(),&summary.best_gain_delta_payload_exact.to_string(),&summary.worst_loss_input,&summary.worst_loss_window_idx.to_string(),&summary.worst_loss_delta_payload_exact.to_string()]);
    push_csv_row(&mut out, &["section","input","searched_total_piecewise_payload_exact","projected_default_total_piecewise_payload_exact","projected_unpriced_best_mix_total_piecewise_payload_exact","selected_total_piecewise_payload_exact","target_window_count","override_path_bytes_exact","selected_override_window_count"]);
    for file in file_summaries {
        push_csv_row(&mut out, &["file",&file.input,&file.searched_total_piecewise_payload_exact.to_string(),&file.projected_default_total_piecewise_payload_exact.to_string(),&file.projected_unpriced_best_mix_total_piecewise_payload_exact.to_string(),&file.selected_total_piecewise_payload_exact.to_string(),&file.target_window_count.to_string(),&file.override_path_bytes_exact.to_string(),&file.selected_override_window_count.to_string()]);
    }
    push_csv_row(&mut out, &["section","input","window_idx","target_ordinal","searched_payload_exact","default_payload_exact","best_payload_exact","selected_payload_exact","searched_chunk_bytes","best_chunk_bytes","selected_chunk_bytes","selected_gain_exact"]);
    for row in window_rows {
        push_csv_row(&mut out, &["window",&row.input,&row.window_idx.to_string(),&row.target_ordinal.to_string(),&row.searched_payload_exact.to_string(),&row.default_payload_exact.to_string(),&row.best_payload_exact.to_string(),&row.selected_payload_exact.to_string(),&row.searched_chunk_bytes.to_string(),&row.best_chunk_bytes.to_string(),&row.selected_chunk_bytes.to_string(),&row.selected_gain_exact.to_string()]);
    }
    push_csv_row(&mut out, &["section","input","window_idx","target_ordinal","best_chunk_bytes","default_payload_exact","best_payload_exact","gain_exact"]);
    for row in override_candidates {
        push_csv_row(&mut out, &["override_candidate",&row.input,&row.window_idx.to_string(),&row.target_ordinal.to_string(),&row.best_chunk_bytes.to_string(),&row.default_payload_exact.to_string(),&row.best_payload_exact.to_string(),&row.gain_exact.to_string()]);
    }
    for row in override_selected {
        push_csv_row(&mut out, &["override_selected",&row.input,&row.window_idx.to_string(),&row.target_ordinal.to_string(),&row.best_chunk_bytes.to_string(),&row.default_payload_exact.to_string(),&row.best_payload_exact.to_string(),&row.gain_exact.to_string()]);
    }
    out
}

fn push_line(out: &mut String, key: &str, value: impl ToString) {
    out.push_str(key);
    out.push('=');
    out.push_str(&value.to_string());
    out.push('\n');
}

fn push_csv_row(out: &mut String, cells: &[&str]) {
    let escaped = cells.iter().map(|s| csv_escape(s)).collect::<Vec<_>>();
    out.push_str(&escaped.join(","));
    out.push('\n');
}

fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}
