use super::ws_lane_types::{WsLaneChunkBest, WsLaneReport, WsLaneSweepRow};

pub fn render_ws_class_ascii(classes: &[u8]) -> String {
    let mut out = String::with_capacity(classes.len());
    for &c in classes {
        out.push(match c {
            0 => '.',
            1 => 's',
            2 => 'n',
            _ => '?',
        });
    }
    out
}

pub fn render_ws_lane_csv(row: &WsLaneReport) -> String {
    let mut out = String::new();
    out.push_str("input,recipe,normalized_len,class_len,other_len,baseline_artifact_bytes,baseline_max_ticks_used,baseline_class_mismatches,baseline_class_patch_entries,baseline_class_patch_bytes,apex_byte_len,apex_quat_len,apex_depth,apex_root_quadrant,apex_root_seed_hex,apex_recipe_seed_hex,apex_key_bytes_exact,apex_matches,apex_prefix,apex_total,apex_match_pct,apex_longest_run,apex_longest_run_start,apex_patch_entries,apex_patch_bytes,apex_total_payload_exact,delta_patch_bytes,delta_patch_entries,delta_total_payload_exact_vs_baseline,target_hist_other,target_hist_space,target_hist_newline,pred_hist_other,pred_hist_space,pred_hist_newline,chunk_bytes,chunk_count,chunk_key_bytes_exact,chunk_patch_entries,chunk_patch_bytes,chunk_total_payload_exact,chunk_unique_key_count,chunk_unique_seed_count,chunk_delta_patch_bytes_vs_baseline,chunk_delta_patch_bytes_vs_global,chunk_delta_total_payload_exact_vs_baseline,chunk_delta_total_payload_exact_vs_global,chunk_matches,chunk_prefix,chunk_total,chunk_match_pct,chunk_longest_run,chunk_longest_run_start,chunk_target_hist_other,chunk_target_hist_space,chunk_target_hist_newline,chunk_pred_hist_other,chunk_pred_hist_space,chunk_pred_hist_newline\n");
    let chunk_target_hist = row.chunk_target_hist.unwrap_or([0, 0, 0]);
    let chunk_pred_hist = row.chunk_pred_hist.unwrap_or([0, 0, 0]);
    let fields = [
        format!("\"{}\"", row.input.replace('"', "\"\"")),
        format!("\"{}\"", row.recipe.replace('"', "\"\"")),
        row.normalized_len.to_string(),
        row.class_len.to_string(),
        row.other_len.to_string(),
        row.baseline_artifact_bytes.to_string(),
        row.baseline_max_ticks_used.to_string(),
        row.baseline_class_mismatches.to_string(),
        row.baseline_class_patch_entries.to_string(),
        row.baseline_class_patch_bytes.to_string(),
        row.apex_byte_len.to_string(),
        row.apex_quat_len.to_string(),
        row.apex_depth.to_string(),
        row.apex_root_quadrant.to_string(),
        format!("0x{:016X}", row.apex_root_seed),
        format!("0x{:016X}", row.apex_recipe_seed),
        row.apex_key_bytes_exact.to_string(),
        row.apex_matches.to_string(),
        row.apex_prefix.to_string(),
        row.apex_total.to_string(),
        format!("{:.6}", row.apex_match_pct),
        row.apex_longest_run.to_string(),
        row.apex_longest_run_start.to_string(),
        row.apex_patch_entries.to_string(),
        row.apex_patch_bytes.to_string(),
        row.apex_total_payload_exact.to_string(),
        row.delta_patch_bytes.to_string(),
        row.delta_patch_entries.to_string(),
        row.delta_total_payload_exact_vs_baseline.to_string(),
        row.target_hist[0].to_string(),
        row.target_hist[1].to_string(),
        row.target_hist[2].to_string(),
        row.pred_hist[0].to_string(),
        row.pred_hist[1].to_string(),
        row.pred_hist[2].to_string(),
        row.chunk_bytes.map(|v| v.to_string()).unwrap_or_default(),
        row.chunk_count.map(|v| v.to_string()).unwrap_or_default(),
        row.chunk_key_bytes_exact.map(|v| v.to_string()).unwrap_or_default(),
        row.chunk_patch_entries.map(|v| v.to_string()).unwrap_or_default(),
        row.chunk_patch_bytes.map(|v| v.to_string()).unwrap_or_default(),
        row.chunk_total_payload_exact.map(|v| v.to_string()).unwrap_or_default(),
        row.chunk_unique_key_count.map(|v| v.to_string()).unwrap_or_default(),
        row.chunk_unique_seed_count.map(|v| v.to_string()).unwrap_or_default(),
        row.chunk_delta_patch_bytes_vs_baseline.map(|v| v.to_string()).unwrap_or_default(),
        row.chunk_delta_patch_bytes_vs_global.map(|v| v.to_string()).unwrap_or_default(),
        row.chunk_delta_total_payload_exact_vs_baseline.map(|v| v.to_string()).unwrap_or_default(),
        row.chunk_delta_total_payload_exact_vs_global.map(|v| v.to_string()).unwrap_or_default(),
        row.chunk_matches.map(|v| v.to_string()).unwrap_or_default(),
        row.chunk_prefix.map(|v| v.to_string()).unwrap_or_default(),
        row.chunk_total.map(|v| v.to_string()).unwrap_or_default(),
        row.chunk_match_pct.map(|v| format!("{:.6}", v)).unwrap_or_default(),
        row.chunk_longest_run.map(|v| v.to_string()).unwrap_or_default(),
        row.chunk_longest_run_start.map(|v| v.to_string()).unwrap_or_default(),
        row.chunk_target_hist.map(|_| chunk_target_hist[0].to_string()).unwrap_or_default(),
        row.chunk_target_hist.map(|_| chunk_target_hist[1].to_string()).unwrap_or_default(),
        row.chunk_target_hist.map(|_| chunk_target_hist[2].to_string()).unwrap_or_default(),
        row.chunk_pred_hist.map(|_| chunk_pred_hist[0].to_string()).unwrap_or_default(),
        row.chunk_pred_hist.map(|_| chunk_pred_hist[1].to_string()).unwrap_or_default(),
        row.chunk_pred_hist.map(|_| chunk_pred_hist[2].to_string()).unwrap_or_default(),
    ];
    out.push_str(&fields.join(","));
    out.push('\n');
    out
}

pub fn render_ws_lane_txt(row: &WsLaneReport) -> String {
    let mut out = String::new();
    macro_rules! line {
        ($k:expr, $v:expr) => {{
            out.push_str($k);
            out.push('=');
            out.push_str(&$v.to_string());
            out.push('\n');
        }};
    }
    line!("input", row.input.clone());
    line!("recipe", row.recipe.clone());
    line!("normalized_len", row.normalized_len);
    line!("class_len", row.class_len);
    line!("other_len", row.other_len);
    line!("baseline_artifact_bytes", row.baseline_artifact_bytes);
    line!("baseline_max_ticks_used", row.baseline_max_ticks_used);
    line!("baseline_class_mismatches", row.baseline_class_mismatches);
    line!("baseline_class_patch_entries", row.baseline_class_patch_entries);
    line!("baseline_class_patch_bytes", row.baseline_class_patch_bytes);
    line!("apex_byte_len", row.apex_byte_len);
    line!("apex_quat_len", row.apex_quat_len);
    line!("apex_depth", row.apex_depth);
    line!("apex_root_quadrant", row.apex_root_quadrant);
    line!("apex_root_seed", format!("0x{:016X}", row.apex_root_seed));
    line!("apex_recipe_seed", format!("0x{:016X}", row.apex_recipe_seed));
    line!("apex_key_bytes_exact", row.apex_key_bytes_exact);
    line!("apex_matches", row.apex_matches);
    line!("apex_prefix", row.apex_prefix);
    line!("apex_total", row.apex_total);
    line!("apex_match_pct", format!("{:.6}", row.apex_match_pct));
    line!("apex_longest_run", row.apex_longest_run);
    line!("apex_longest_run_start", row.apex_longest_run_start);
    line!("apex_patch_entries", row.apex_patch_entries);
    line!("apex_patch_bytes", row.apex_patch_bytes);
    line!("apex_total_payload_exact", row.apex_total_payload_exact);
    line!("delta_patch_bytes", row.delta_patch_bytes);
    line!("delta_patch_entries", row.delta_patch_entries);
    line!("delta_total_payload_exact_vs_baseline", row.delta_total_payload_exact_vs_baseline);
    line!("target_hist_other", row.target_hist[0]);
    line!("target_hist_space", row.target_hist[1]);
    line!("target_hist_newline", row.target_hist[2]);
    line!("pred_hist_other", row.pred_hist[0]);
    line!("pred_hist_space", row.pred_hist[1]);
    line!("pred_hist_newline", row.pred_hist[2]);
    if let Some(v) = row.chunk_bytes { line!("chunk_bytes", v); }
    if let Some(v) = row.chunk_count { line!("chunk_count", v); }
    if let Some(v) = row.chunk_key_bytes_exact { line!("chunk_key_bytes_exact", v); }
    if let Some(v) = row.chunk_patch_entries { line!("chunk_patch_entries", v); }
    if let Some(v) = row.chunk_patch_bytes { line!("chunk_patch_bytes", v); }
    if let Some(v) = row.chunk_total_payload_exact { line!("chunk_total_payload_exact", v); }
    if let Some(v) = row.chunk_unique_key_count { line!("chunk_unique_key_count", v); }
    if let Some(v) = row.chunk_unique_seed_count { line!("chunk_unique_seed_count", v); }
    if let Some(v) = row.chunk_delta_patch_bytes_vs_baseline { line!("chunk_delta_patch_bytes_vs_baseline", v); }
    if let Some(v) = row.chunk_delta_patch_bytes_vs_global { line!("chunk_delta_patch_bytes_vs_global", v); }
    if let Some(v) = row.chunk_delta_total_payload_exact_vs_baseline { line!("chunk_delta_total_payload_exact_vs_baseline", v); }
    if let Some(v) = row.chunk_delta_total_payload_exact_vs_global { line!("chunk_delta_total_payload_exact_vs_global", v); }
    if let Some(v) = row.chunk_matches { line!("chunk_matches", v); }
    if let Some(v) = row.chunk_prefix { line!("chunk_prefix", v); }
    if let Some(v) = row.chunk_total { line!("chunk_total", v); }
    if let Some(v) = row.chunk_match_pct { line!("chunk_match_pct", format!("{:.6}", v)); }
    if let Some(v) = row.chunk_longest_run { line!("chunk_longest_run", v); }
    if let Some(v) = row.chunk_longest_run_start { line!("chunk_longest_run_start", v); }
    if let Some(hist) = row.chunk_target_hist {
        line!("chunk_target_hist_other", hist[0]);
        line!("chunk_target_hist_space", hist[1]);
        line!("chunk_target_hist_newline", hist[2]);
    }
    if let Some(hist) = row.chunk_pred_hist {
        line!("chunk_pred_hist_other", hist[0]);
        line!("chunk_pred_hist_space", hist[1]);
        line!("chunk_pred_hist_newline", hist[2]);
    }
    for chunk in &row.chunk_reports {
        line!(&format!("chunk_{}_range", chunk.chunk_index), format!("{}..{}", chunk.start, chunk.end));
        line!(&format!("chunk_{}_len", chunk.chunk_index), chunk.len);
        line!(&format!("chunk_{}_root_quadrant", chunk.chunk_index), chunk.root_quadrant);
        line!(&format!("chunk_{}_root_seed", chunk.chunk_index), format!("0x{:016X}", chunk.root_seed));
        line!(&format!("chunk_{}_recipe_seed", chunk.chunk_index), format!("0x{:016X}", chunk.recipe_seed));
        line!(&format!("chunk_{}_matches", chunk.chunk_index), chunk.matches);
        line!(&format!("chunk_{}_prefix", chunk.chunk_index), chunk.prefix);
        line!(&format!("chunk_{}_total", chunk.chunk_index), chunk.total);
        line!(&format!("chunk_{}_match_pct", chunk.chunk_index), format!("{:.6}", chunk.match_pct));
        line!(&format!("chunk_{}_longest_run", chunk.chunk_index), chunk.longest_run);
        line!(&format!("chunk_{}_longest_run_start", chunk.chunk_index), chunk.longest_run_start);
        line!(&format!("chunk_{}_patch_entries", chunk.chunk_index), chunk.patch_entries);
        line!(&format!("chunk_{}_patch_bytes", chunk.chunk_index), chunk.patch_bytes);
    }
    out
}

pub fn render_ws_lane_chunk_keys_csv(chunk_bytes: usize, chunks: &[WsLaneChunkBest]) -> String {
    let mut out = String::new();
    out.push_str("chunk_bytes,chunk_index,start,end,len,root_quadrant,root_seed_hex,recipe_seed_hex,matches,prefix,total,match_pct,longest_run,longest_run_start,patch_entries,patch_bytes\n");
    for chunk in chunks {
        let fields = [
            chunk_bytes.to_string(),
            chunk.chunk_index.to_string(),
            chunk.start.to_string(),
            chunk.end.to_string(),
            chunk.end.saturating_sub(chunk.start).to_string(),
            chunk.key.root_quadrant.to_string(),
            format!("0x{:016X}", chunk.key.root_seed),
            format!("0x{:016X}", chunk.key.recipe_seed),
            chunk.diag.score.matches.to_string(),
            chunk.diag.score.prefix.to_string(),
            chunk.diag.score.total.to_string(),
            format!("{:.6}", (chunk.diag.score.matches as f64) * 100.0 / (chunk.diag.score.total.max(1) as f64)),
            chunk.diag.score.longest_run.to_string(),
            chunk.diag.score.longest_run_start.to_string(),
            chunk.patch_entries.to_string(),
            chunk.patch_bytes.to_string(),
        ];
        out.push_str(&fields.join(","));
        out.push('\n');
    }
    out
}

pub fn render_ws_lane_sweep_csv(rows: &[WsLaneSweepRow]) -> String {
    let mut out = String::from("chunk_bytes,chunk_count,key_bytes_exact,patch_entries,patch_bytes,total_payload_exact,unique_key_count,unique_seed_count,matches,total,match_pct,longest_run,delta_patch_vs_baseline,delta_patch_vs_global,delta_total_vs_baseline,delta_total_vs_global\n");
    for row in rows {
        out.push_str(&format!(
            "{},{},{},{},{},{},{},{},{},{},{:.6},{},{},{},{},{}\n",
            row.chunk_bytes,
            row.chunk_count,
            row.key_bytes_exact,
            row.patch_entries,
            row.patch_bytes,
            row.total_payload_exact,
            row.unique_key_count,
            row.unique_seed_count,
            row.matches,
            row.total,
            row.match_pct,
            row.longest_run,
            row.delta_patch_vs_baseline,
            row.delta_patch_vs_global,
            row.delta_total_vs_baseline,
            row.delta_total_vs_global,
        ));
    }
    out
}

pub fn render_ws_lane_sweep_txt(rows: &[WsLaneSweepRow]) -> String {
    let mut out = String::new();
    for row in rows {
        out.push_str(&format!(
            "chunk_bytes={} chunk_count={} key_bytes_exact={} patch_entries={} patch_bytes={} total_payload_exact={} unique_key_count={} unique_seed_count={} matches={} total={} match_pct={:.6} longest_run={} delta_patch_vs_baseline={} delta_patch_vs_global={} delta_total_vs_baseline={} delta_total_vs_global={}\n",
            row.chunk_bytes,
            row.chunk_count,
            row.key_bytes_exact,
            row.patch_entries,
            row.patch_bytes,
            row.total_payload_exact,
            row.unique_key_count,
            row.unique_seed_count,
            row.matches,
            row.total,
            row.match_pct,
            row.longest_run,
            row.delta_patch_vs_baseline,
            row.delta_patch_vs_global,
            row.delta_total_vs_baseline,
            row.delta_total_vs_global,
        ));
    }
    out
}
