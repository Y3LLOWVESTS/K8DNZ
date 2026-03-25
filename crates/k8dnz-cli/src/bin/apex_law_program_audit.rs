use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use clap::{Parser, ValueEnum};

#[derive(Parser, Debug)]
#[command(name = "apex_law_program_audit")]
#[command(about = "Audit apex_law_program build/replay reports for codec-best vs closure-best")]
struct Args {
    #[arg(
        long = "case",
        required = true,
        help = "Case spec: label|build_report_path|replay_report_path"
    )]
    cases: Vec<String>,

    #[arg(long, value_enum, default_value_t = OutputFormat::Txt)]
    format: OutputFormat,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum OutputFormat {
    Txt,
    Csv,
}

#[derive(Clone, Debug)]
struct CaseInput {
    label: String,
    build_report: PathBuf,
    replay_report: PathBuf,
}

#[derive(Clone, Debug)]
struct CaseSummary {
    label: String,
    artifact_path: String,
    target_global_law_id: String,
    body_select_objective: String,
    default_local_chunk_bytes: usize,
    override_path_mode: String,
    artifact_selected_total_piecewise_payload_exact: usize,
    projected_default_total_piecewise_payload_exact: usize,
    default_gain_exact: i64,
    target_window_count: usize,
    override_count: usize,
    bridge_segment_count: usize,
    bridge_window_count: usize,
    override_run_count: usize,
    max_override_run_length: usize,
    override_density_pct: f64,
    untouched_window_count: usize,
    untouched_window_pct: f64,
    override_path_bytes_exact: usize,
    drift_exact: i64,
    collapse_90_failures: usize,
    newline_extinct_failures: usize,
    replay_selected_total_piecewise_payload_exact: i64,
    replay_gap_exact: i64,
    best_surface: String,
    best_total_piecewise_payload_exact: i64,
    best_delta_vs_artifact_exact: i64,
    stability_failures: usize,
    stable: bool,
    closure_penalty_exact: usize,
    closure_total_exact: usize,
}

impl CaseSummary {
    fn codec_total_exact(&self) -> usize {
        self.artifact_selected_total_piecewise_payload_exact
    }

    fn surface_beats_artifact(&self) -> bool {
        self.best_delta_vs_artifact_exact < 0
    }

    fn closure_grade(&self) -> &'static str {
        if !self.stable {
            return "UNSTABLE";
        }
        if self.override_count == 0 {
            return "PURE_BODY";
        }
        if self.override_density_pct <= 15.0 && self.override_run_count <= 3 {
            return "SPARSE";
        }
        if self.override_density_pct <= 40.0 {
            return "MIXED";
        }
        "DENSE"
    }
}

fn main() -> Result<()> {
    let args = Args::parse();

    let mut rows = Vec::with_capacity(args.cases.len());
    for raw in &args.cases {
        let spec = parse_case_spec(raw)?;
        rows.push(load_case_summary(&spec)?);
    }

    let body = match args.format {
        OutputFormat::Txt => render_txt(&rows),
        OutputFormat::Csv => render_csv(&rows),
    };
    print!("{body}");
    Ok(())
}

fn parse_case_spec(raw: &str) -> Result<CaseInput> {
    let parts = raw.split('|').map(str::trim).collect::<Vec<_>>();
    if parts.len() != 3 {
        return Err(anyhow!(
            "invalid --case spec {:?}; expected label|build_report|replay_report",
            raw
        ));
    }
    if parts.iter().any(|s| s.is_empty()) {
        return Err(anyhow!(
            "invalid --case spec {:?}; label/build/replay must be non-empty",
            raw
        ));
    }

    Ok(CaseInput {
        label: parts[0].to_string(),
        build_report: PathBuf::from(parts[1]),
        replay_report: PathBuf::from(parts[2]),
    })
}

fn load_case_summary(spec: &CaseInput) -> Result<CaseSummary> {
    let build = parse_key_value_report(&spec.build_report)
        .with_context(|| format!("parse build report {}", spec.build_report.display()))?;
    let replay = parse_key_value_report(&spec.replay_report)
        .with_context(|| format!("parse replay report {}", spec.replay_report.display()))?;

    let artifact_path = get_string(&build, "artifact_path");
    let target_global_law_id = get_string(&build, "target_global_law_id");
    let body_select_objective = get_string(&build, "body_select_objective");
    let default_local_chunk_bytes = get_usize(&build, "default_local_chunk_bytes");
    let artifact_selected_total_piecewise_payload_exact =
        get_usize(&build, "selected_total_piecewise_payload_exact");
    let projected_default_total_piecewise_payload_exact =
        get_usize(&build, "projected_default_total_piecewise_payload_exact");
    let override_path_mode = get_string(&build, "override_path_mode");
    let override_path_bytes_exact = get_usize(&build, "override_path_bytes_exact");
    let target_window_count = get_usize_fallback(&build, &["target_window_count", "window_count"]);
    let override_count = get_usize(&build, "override_count");
    let bridge_segment_count = get_usize_fallback(
        &build,
        &["bridge_segment_count", "artifact_bridge_segment_count"],
    );
    let bridge_window_count = get_usize_fallback(
        &build,
        &["bridge_window_count", "artifact_bridge_window_count"],
    );
    let override_run_count = get_usize(&build, "override_run_count");
    let max_override_run_length = get_usize(&build, "max_override_run_length");
    let untouched_window_count = get_usize(&build, "untouched_window_count");

    let override_density_pct =
        get_f64_fallback(&build, &["override_density_pct"], &["override_density_ppm"]);
    let untouched_window_pct =
        get_f64_fallback(&build, &["untouched_window_pct"], &["untouched_window_pct_ppm"]);

    let replay_selected_total_piecewise_payload_exact =
        get_i64(&replay, "replay_selected_total_piecewise_payload_exact");
    let drift_exact = get_i64(&replay, "drift_exact");
    let collapse_90_failures = get_usize(&replay, "collapse_90_failures");
    let newline_extinct_failures = get_usize(&replay, "newline_extinct_failures");

    let default_gain_exact = projected_default_total_piecewise_payload_exact as i64
        - artifact_selected_total_piecewise_payload_exact as i64;

    let replay_gap_exact = replay_selected_total_piecewise_payload_exact
        - artifact_selected_total_piecewise_payload_exact as i64;

    let (best_surface, best_total_piecewise_payload_exact, best_delta_vs_artifact_exact) =
        select_best_surface_from_report(
            &build,
            &replay,
            artifact_selected_total_piecewise_payload_exact as i64,
            replay_selected_total_piecewise_payload_exact,
        );

    let stability_failures =
        usize::from(drift_exact != 0) + collapse_90_failures + newline_extinct_failures;
    let stable = stability_failures == 0;

    let closure_penalty_exact = {
        let parsed = get_usize(&build, "closure_penalty_exact");
        if parsed != 0 || override_count == 0 {
            parsed
        } else {
            let mode_penalty_exact = match override_path_mode.as_str() {
                "none" => 0usize,
                "delta" => 0usize,
                "runs" => 0usize,
                "bridge" => 0usize,
                "ordinals" => 64usize,
                _ => 64usize,
            };
            override_count.saturating_mul(48)
                + override_run_count.saturating_mul(96)
                + override_path_bytes_exact.saturating_mul(2)
                + mode_penalty_exact
        }
    };

    let closure_total_exact = {
        let parsed = get_usize(&build, "closure_total_exact");
        if parsed != 0 || artifact_selected_total_piecewise_payload_exact == 0 {
            parsed
        } else {
            artifact_selected_total_piecewise_payload_exact.saturating_add(closure_penalty_exact)
        }
    };

    Ok(CaseSummary {
        label: spec.label.clone(),
        artifact_path,
        target_global_law_id,
        body_select_objective,
        default_local_chunk_bytes,
        override_path_mode,
        artifact_selected_total_piecewise_payload_exact,
        projected_default_total_piecewise_payload_exact,
        default_gain_exact,
        target_window_count,
        override_count,
        bridge_segment_count,
        bridge_window_count,
        override_run_count,
        max_override_run_length,
        override_density_pct,
        untouched_window_count,
        untouched_window_pct,
        override_path_bytes_exact,
        drift_exact,
        collapse_90_failures,
        newline_extinct_failures,
        replay_selected_total_piecewise_payload_exact,
        replay_gap_exact,
        best_surface,
        best_total_piecewise_payload_exact,
        best_delta_vs_artifact_exact,
        stability_failures,
        stable,
        closure_penalty_exact,
        closure_total_exact,
    })
}

fn parse_key_value_report(path: &PathBuf) -> Result<BTreeMap<String, String>> {
    let body = fs::read_to_string(path).with_context(|| format!("read report {}", path.display()))?;
    let mut out = BTreeMap::<String, String>::new();

    for raw in body.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with("--- ") {
            continue;
        }
        let Some((k, v)) = line.split_once('=') else {
            continue;
        };
        let key = k.trim().to_string();
        let value = v.trim().to_string();
        out.entry(key).or_insert(value);
    }

    Ok(out)
}

fn get_string(map: &BTreeMap<String, String>, key: &str) -> String {
    map.get(key).cloned().unwrap_or_default()
}

fn get_usize(map: &BTreeMap<String, String>, key: &str) -> usize {
    map.get(key)
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0)
}

fn get_i64(map: &BTreeMap<String, String>, key: &str) -> i64 {
    map.get(key)
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0)
}

fn select_best_surface_from_report(
    build: &BTreeMap<String, String>,
    replay: &BTreeMap<String, String>,
    artifact_selected_total_piecewise_payload_exact: i64,
    replay_selected_total_piecewise_payload_exact: i64,
) -> (String, i64, i64) {
    if replay.contains_key("best_surface") && replay.contains_key("best_total_piecewise_payload_exact")
    {
        let best_surface = normalize_surface_label(&get_string(replay, "best_surface"));
        let best_total = get_i64(replay, "best_total_piecewise_payload_exact");
        let best_delta = if replay.contains_key("best_delta_vs_artifact_exact") {
            get_i64(replay, "best_delta_vs_artifact_exact")
        } else {
            best_total - artifact_selected_total_piecewise_payload_exact
        };
        return (best_surface, best_total, best_delta);
    }

    if build.contains_key("surface_best_surface")
        && build.contains_key("surface_best_total_piecewise_payload_exact")
    {
        let best_surface = normalize_surface_label(&get_string(build, "surface_best_surface"));
        let best_total = get_i64(build, "surface_best_total_piecewise_payload_exact");
        let best_delta = if build.contains_key("surface_gap_vs_codec_exact") {
            get_i64(build, "surface_gap_vs_codec_exact")
        } else if build.contains_key("build_frontier_best_delta_vs_piecewise_exact") {
            get_i64(build, "build_frontier_best_delta_vs_piecewise_exact")
        } else {
            best_total - artifact_selected_total_piecewise_payload_exact
        };
        return (best_surface, best_total, best_delta);
    }

    if build.contains_key("build_frontier_best_surface")
        && build.contains_key("build_frontier_best_total_piecewise_payload_exact")
    {
        let best_surface =
            normalize_surface_label(&get_string(build, "build_frontier_best_surface"));
        let best_total = get_i64(build, "build_frontier_best_total_piecewise_payload_exact");
        let best_delta = if build.contains_key("build_frontier_best_delta_vs_piecewise_exact") {
            get_i64(build, "build_frontier_best_delta_vs_piecewise_exact")
        } else {
            best_total - artifact_selected_total_piecewise_payload_exact
        };
        return (best_surface, best_total, best_delta);
    }

    let mut candidates = vec![
        (
            "artifact".to_string(),
            artifact_selected_total_piecewise_payload_exact,
        ),
        (
            "replay".to_string(),
            replay_selected_total_piecewise_payload_exact,
        ),
    ];

    for (key, label) in [
        ("frozen_total_piecewise_payload_exact", "freeze"),
        ("split_total_piecewise_payload_exact", "split-freeze"),
        ("bridge_total_piecewise_payload_exact", "bridge-freeze"),
    ] {
        if replay.contains_key(key) {
            candidates.push((label.to_string(), get_i64(replay, key)));
        }
    }

    candidates.sort_by_key(|(label, total)| (*total, label.clone()));
    let (best_surface, best_total) = candidates.into_iter().next().unwrap_or_else(|| {
        (
            "artifact".to_string(),
            artifact_selected_total_piecewise_payload_exact,
        )
    });
    let best_delta = best_total - artifact_selected_total_piecewise_payload_exact;
    (best_surface, best_total, best_delta)
}

fn normalize_surface_label(raw: &str) -> String {
    match raw {
        "bridge" => "bridge-freeze".to_string(),
        "split" => "split-freeze".to_string(),
        other => other.to_string(),
    }
}

fn get_usize_fallback(map: &BTreeMap<String, String>, keys: &[&str]) -> usize {
    for key in keys {
        let value = get_usize(map, key);
        if value != 0 {
            return value;
        }
    }
    0
}

fn get_f64_fallback(
    map: &BTreeMap<String, String>,
    raw_float_keys: &[&str],
    ppm_keys: &[&str],
) -> f64 {
    for key in raw_float_keys {
        if let Some(v) = map.get(*key).and_then(|v| v.parse::<f64>().ok()) {
            return v;
        }
    }
    for key in ppm_keys {
        if let Some(v) = map.get(*key).and_then(|v| v.parse::<u32>().ok()) {
            return v as f64 / 10_000.0;
        }
    }
    0.0
}

fn render_txt(rows: &[CaseSummary]) -> String {
    let mut out = String::new();

    let mut codec_rank = rows.to_vec();
    codec_rank.sort_by_key(|r| {
        (
            r.codec_total_exact(),
            usize::from(!r.stable),
            r.closure_total_exact,
            r.override_count,
            r.override_run_count,
            r.override_path_bytes_exact,
            r.default_local_chunk_bytes,
        )
    });

    let mut closure_rank = rows.to_vec();
    closure_rank.sort_by_key(|r| {
        (
            usize::from(!r.stable),
            r.closure_total_exact,
            r.codec_total_exact(),
            r.override_count,
            r.override_run_count,
            r.override_path_bytes_exact,
            r.default_local_chunk_bytes,
        )
    });

    let mut surface_rank = rows.to_vec();
    surface_rank.sort_by_key(|r| {
        (
            r.best_total_piecewise_payload_exact,
            usize::from(!r.stable),
            r.codec_total_exact(),
            r.closure_total_exact,
            r.default_local_chunk_bytes,
        )
    });

    if let Some(best) = codec_rank.first() {
        out.push_str("codec_best_label=");
        out.push_str(&best.label);
        out.push('\n');
        out.push_str("codec_best_total_exact=");
        out.push_str(&best.codec_total_exact().to_string());
        out.push('\n');
    }
    if let Some(best) = closure_rank.first() {
        out.push_str("closure_best_label=");
        out.push_str(&best.label);
        out.push('\n');
        out.push_str("closure_best_total_exact=");
        out.push_str(&best.closure_total_exact.to_string());
        out.push('\n');
    }
    if let Some(best) = surface_rank.first() {
        out.push_str("surface_best_label=");
        out.push_str(&best.label);
        out.push('\n');
        out.push_str("surface_best_total_exact=");
        out.push_str(&best.best_total_piecewise_payload_exact.to_string());
        out.push('\n');
        out.push_str("surface_best_surface=");
        out.push_str(&best.best_surface);
        out.push('\n');
    }

    out.push_str("\n--- codec-ranking ---\n");
    for (idx, row) in codec_rank.iter().enumerate() {
        out.push_str(&format!(
            "rank={} label={} objective={} codec_total_exact={} closure_total_exact={} grade={} stable={} law={} chunk={} overrides={} bridge_segments={} bridge_windows={} runs={} max_run_len={} target_windows={} override_density_pct={:.6} override_path_mode={} override_path_bytes_exact={} default_gain_exact={} replay_gap_exact={} artifact_path={}\n",
            idx + 1,
            row.label,
            row.body_select_objective,
            row.codec_total_exact(),
            row.closure_total_exact,
            row.closure_grade(),
            row.stable,
            row.target_global_law_id,
            row.default_local_chunk_bytes,
            row.override_count,
            row.bridge_segment_count,
            row.bridge_window_count,
            row.override_run_count,
            row.max_override_run_length,
            row.target_window_count,
            row.override_density_pct,
            row.override_path_mode,
            row.override_path_bytes_exact,
            row.default_gain_exact,
            row.replay_gap_exact,
            row.artifact_path,
        ));
    }

    out.push_str("\n--- surface-ranking ---\n");
    for (idx, row) in surface_rank.iter().enumerate() {
        out.push_str(&format!(
            "rank={} label={} objective={} best_surface={} best_total_exact={} best_delta_vs_artifact_exact={} surface_beats_artifact={} codec_total_exact={} closure_total_exact={} stable={} law={} chunk={} overrides={} bridge_segments={} bridge_windows={} runs={} max_run_len={} override_path_mode={} artifact_path={}\n",
            idx + 1,
            row.label,
            row.body_select_objective,
            row.best_surface,
            row.best_total_piecewise_payload_exact,
            row.best_delta_vs_artifact_exact,
            row.surface_beats_artifact(),
            row.codec_total_exact(),
            row.closure_total_exact,
            row.stable,
            row.target_global_law_id,
            row.default_local_chunk_bytes,
            row.override_count,
            row.bridge_segment_count,
            row.bridge_window_count,
            row.override_run_count,
            row.max_override_run_length,
            row.override_path_mode,
            row.artifact_path,
        ));
    }

    out.push_str("\n--- closure-ranking ---\n");
    for (idx, row) in closure_rank.iter().enumerate() {
        out.push_str(&format!(
            "rank={} label={} objective={} closure_total_exact={} codec_total_exact={} closure_penalty_exact={} grade={} stable={} law={} chunk={} overrides={} bridge_segments={} bridge_windows={} runs={} max_run_len={} target_windows={} untouched_window_count={} untouched_window_pct={:.6} override_density_pct={:.6} override_path_mode={} override_path_bytes_exact={} drift_exact={} collapse_90_failures={} newline_extinct_failures={}\n",
            idx + 1,
            row.label,
            row.body_select_objective,
            row.closure_total_exact,
            row.codec_total_exact(),
            row.closure_penalty_exact,
            row.closure_grade(),
            row.stable,
            row.target_global_law_id,
            row.default_local_chunk_bytes,
            row.override_count,
            row.bridge_segment_count,
            row.bridge_window_count,
            row.override_run_count,
            row.max_override_run_length,
            row.target_window_count,
            row.untouched_window_count,
            row.untouched_window_pct,
            row.override_density_pct,
            row.override_path_mode,
            row.override_path_bytes_exact,
            row.drift_exact,
            row.collapse_90_failures,
            row.newline_extinct_failures,
        ));
    }

    out.push_str("\n--- case-details ---\n");
    for row in rows {
        out.push_str(&format!(
            "label={} artifact_path={} target_global_law_id={} objective={} default_local_chunk_bytes={} codec_total_exact={} projected_default_total_piecewise_payload_exact={} default_gain_exact={} closure_penalty_exact={} closure_total_exact={} override_count={} bridge_segment_count={} bridge_window_count={} override_run_count={} max_override_run_length={} target_window_count={} override_density_pct={:.6} untouched_window_pct={:.6} override_path_mode={} override_path_bytes_exact={} drift_exact={} replay_selected_total_piecewise_payload_exact={} replay_gap_exact={} best_surface={} best_total_exact={} best_delta_vs_artifact_exact={} surface_beats_artifact={} collapse_90_failures={} newline_extinct_failures={} stable={} grade={}\n",
            row.label,
            row.artifact_path,
            row.target_global_law_id,
            row.body_select_objective,
            row.default_local_chunk_bytes,
            row.codec_total_exact(),
            row.projected_default_total_piecewise_payload_exact,
            row.default_gain_exact,
            row.closure_penalty_exact,
            row.closure_total_exact,
            row.override_count,
            row.bridge_segment_count,
            row.bridge_window_count,
            row.override_run_count,
            row.max_override_run_length,
            row.target_window_count,
            row.override_density_pct,
            row.untouched_window_pct,
            row.override_path_mode,
            row.override_path_bytes_exact,
            row.drift_exact,
            row.replay_selected_total_piecewise_payload_exact,
            row.replay_gap_exact,
            row.best_surface,
            row.best_total_piecewise_payload_exact,
            row.best_delta_vs_artifact_exact,
            row.surface_beats_artifact(),
            row.collapse_90_failures,
            row.newline_extinct_failures,
            row.stable,
            row.closure_grade(),
        ));
    }

    out
}

fn render_csv(rows: &[CaseSummary]) -> String {
    let mut out = String::new();
    out.push_str(
        "label,artifact_path,target_global_law_id,body_select_objective,default_local_chunk_bytes,codec_total_exact,projected_default_total_piecewise_payload_exact,default_gain_exact,closure_penalty_exact,closure_total_exact,override_count,bridge_segment_count,bridge_window_count,override_run_count,max_override_run_length,target_window_count,override_density_pct,untouched_window_count,untouched_window_pct,override_path_mode,override_path_bytes_exact,drift_exact,replay_selected_total_piecewise_payload_exact,replay_gap_exact,best_surface,best_total_exact,best_delta_vs_artifact_exact,surface_beats_artifact,collapse_90_failures,newline_extinct_failures,stability_failures,stable,grade\n",
    );

    for row in rows {
        let cells = [
            csv_escape(&row.label),
            csv_escape(&row.artifact_path),
            csv_escape(&row.target_global_law_id),
            csv_escape(&row.body_select_objective),
            row.default_local_chunk_bytes.to_string(),
            row.codec_total_exact().to_string(),
            row.projected_default_total_piecewise_payload_exact.to_string(),
            row.default_gain_exact.to_string(),
            row.closure_penalty_exact.to_string(),
            row.closure_total_exact.to_string(),
            row.override_count.to_string(),
            row.bridge_segment_count.to_string(),
            row.bridge_window_count.to_string(),
            row.override_run_count.to_string(),
            row.max_override_run_length.to_string(),
            row.target_window_count.to_string(),
            format!("{:.6}", row.override_density_pct),
            row.untouched_window_count.to_string(),
            format!("{:.6}", row.untouched_window_pct),
            csv_escape(&row.override_path_mode),
            row.override_path_bytes_exact.to_string(),
            row.drift_exact.to_string(),
            row.replay_selected_total_piecewise_payload_exact.to_string(),
            row.replay_gap_exact.to_string(),
            csv_escape(&row.best_surface),
            row.best_total_piecewise_payload_exact.to_string(),
            row.best_delta_vs_artifact_exact.to_string(),
            row.surface_beats_artifact().to_string(),
            row.collapse_90_failures.to_string(),
            row.newline_extinct_failures.to_string(),
            row.stability_failures.to_string(),
            row.stable.to_string(),
            row.closure_grade().to_string(),
        ];
        out.push_str(&cells.join(","));
        out.push('\n');
    }

    out
}

fn csv_escape(raw: &str) -> String {
    if raw.contains(',') || raw.contains('"') || raw.contains('\n') {
        format!("\"{}\"", raw.replace('"', "\"\""))
    } else {
        raw.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_best_surface_from_report_prefers_explicit_summary() {
        let mut replay = BTreeMap::new();
        replay.insert("best_surface".to_string(), "bridge".to_string());
        replay.insert(
            "best_total_piecewise_payload_exact".to_string(),
            "4245".to_string(),
        );
        replay.insert(
            "best_delta_vs_artifact_exact".to_string(),
            "-21".to_string(),
        );

        let got = select_best_surface_from_report(&BTreeMap::new(), &replay, 4266, 4266);
        assert_eq!(got, ("bridge-freeze".to_string(), 4245, -21));
    }

    #[test]
    fn select_best_surface_from_report_falls_back_to_min_total() {
        let mut replay = BTreeMap::new();
        replay.insert(
            "frozen_total_piecewise_payload_exact".to_string(),
            "4259".to_string(),
        );
        replay.insert(
            "split_total_piecewise_payload_exact".to_string(),
            "4250".to_string(),
        );
        replay.insert(
            "bridge_total_piecewise_payload_exact".to_string(),
            "4245".to_string(),
        );

        let got = select_best_surface_from_report(&BTreeMap::new(), &replay, 4266, 4266);
        assert_eq!(got, ("bridge-freeze".to_string(), 4245, -21));
    }

    #[test]
    fn select_best_surface_from_report_uses_build_frontier_when_replay_lacks_scoreboard() {
        let mut build = BTreeMap::new();
        build.insert(
            "build_frontier_best_surface".to_string(),
            "bridge-freeze".to_string(),
        );
        build.insert(
            "build_frontier_best_total_piecewise_payload_exact".to_string(),
            "4245".to_string(),
        );
        build.insert(
            "build_frontier_best_delta_vs_piecewise_exact".to_string(),
            "-21".to_string(),
        );

        let replay = BTreeMap::new();
        let got = select_best_surface_from_report(&build, &replay, 4266, 4266);
        assert_eq!(got, ("bridge-freeze".to_string(), 4245, -21));
    }
}