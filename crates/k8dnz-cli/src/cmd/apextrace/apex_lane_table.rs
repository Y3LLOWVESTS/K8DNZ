use std::collections::BTreeMap;
use std::process::Command;

use anyhow::{anyhow, bail, Context, Result};

use crate::cmd::apextrace::{ApexLaneTableArgs, RenderFormat};

use super::common::write_or_print;

#[derive(Clone, Debug)]
struct StrategyMetrics {
    label: String,
    total_payload_exact: usize,
    raw_match_pct: f64,
    balanced_accuracy_pct: Option<f64>,
    macro_f1_pct: Option<f64>,
    minority_metric_name: &'static str,
    minority_metric_pct: Option<f64>,
    patch_entries: Option<usize>,
    patch_bytes: Option<usize>,
}

#[derive(Clone, Debug)]
struct LaneTableRow {
    text_id: String,
    lane_id: &'static str,
    target_len: usize,
    majority_class_label: String,
    majority_baseline_match_pct: f64,
    best_field_chunk_bytes: usize,
    best_field_boundary_band: usize,
    best_field_margin: u64,
    best_field: StrategyMetrics,
    codec: StrategyMetrics,
    frontier: StrategyMetrics,
}

pub fn run_apex_lane_table(args: ApexLaneTableArgs) -> Result<()> {
    use std::time::Instant;

    let started_all = Instant::now();
    let mut rows = Vec::new();
    for input in [&args.genesis1, &args.genesis2] {
        eprintln!("apex-lane-table: start text={} lane=whitespace-class", input);
        let t0 = Instant::now();
        rows.push(eval_ws_lane(&args, input)?);
        eprintln!(
            "apex-lane-table: done text={} lane=whitespace-class elapsed={:.2?}",
            input,
            t0.elapsed()
        );

        eprintln!("apex-lane-table: start text={} lane=case-anchor", input);
        let t0 = Instant::now();
        rows.push(eval_case_lane(&args, input)?);
        eprintln!(
            "apex-lane-table: done text={} lane=case-anchor elapsed={:.2?}",
            input,
            t0.elapsed()
        );

        eprintln!("apex-lane-table: start text={} lane=punct-kind", input);
        let t0 = Instant::now();
        rows.push(eval_punct_kind_lane(&args, input)?);
        eprintln!(
            "apex-lane-table: done text={} lane=punct-kind elapsed={:.2?}",
            input,
            t0.elapsed()
        );
    }

    let body = match args.format {
        RenderFormat::Txt => render_txt(&rows),
        RenderFormat::Csv => render_csv(&rows),
    };
    eprintln!(
        "apex-lane-table: render complete rows={} elapsed={:.2?}",
        rows.len(),
        started_all.elapsed()
    );
    write_or_print(args.out.as_deref(), &body)
}

fn eval_ws_lane(args: &ApexLaneTableArgs, input: &str) -> Result<LaneTableRow> {
    let stdout = run_child(
        "apex-map-lane",
        &[
            "--recipe".to_string(),
            args.recipe.clone(),
            "--in".to_string(),
            input.to_string(),
            "--seed-from".to_string(),
            args.seed_from.to_string(),
            "--seed-count".to_string(),
            args.seed_count.to_string(),
            "--seed-step".to_string(),
            args.seed_step.to_string(),
            "--recipe-seed".to_string(),
            args.recipe_seed.to_string(),
            "--chunk-sweep".to_string(),
            args.ws_chunk_sweep.clone(),
            "--boundary-band-sweep".to_string(),
            args.boundary_band_sweep.clone(),
            "--field-margin-sweep".to_string(),
            args.field_margin_sweep.clone(),
            "--format".to_string(),
            "txt".to_string(),
        ],
    )?;

    let runs = parse_ws_runs(&stdout)?;
    let best_idx = select_best_ws_run_index(&runs)?;
    let best = &runs[best_idx];

    let class_len = get_usize(best, "class_len")?;
    let baseline_mismatches = get_usize(best, "baseline_class_mismatches")?;
    let baseline_match_pct = if class_len == 0 {
        0.0
    } else {
        ((class_len.saturating_sub(baseline_mismatches)) as f64) * 100.0 / (class_len as f64)
    };

    let best_field = StrategyMetrics {
        label: "best-field".to_string(),
        total_payload_exact: get_usize(best, "compact_field_total_payload_exact")?,
        raw_match_pct: get_f64(best, "field_match_pct")?,
        balanced_accuracy_pct: Some(get_f64(best, "field_balanced_accuracy_pct")?),
        macro_f1_pct: Some(get_f64(best, "field_macro_f1_pct")?),
        minority_metric_name: "newline_f1",
        minority_metric_pct: Some(get_f64(best, "field_f1_newline_pct")?),
        patch_entries: Some(get_usize(best, "field_patch_entries")?),
        patch_bytes: Some(get_usize(best, "field_patch_bytes")?),
    };

    let baseline_codec = StrategyMetrics {
        label: "baseline-k8l1-class-patch".to_string(),
        total_payload_exact: get_usize(best, "baseline_class_patch_bytes")?,
        raw_match_pct: baseline_match_pct,
        balanced_accuracy_pct: None,
        macro_f1_pct: None,
        minority_metric_name: "newline_f1",
        minority_metric_pct: None,
        patch_entries: Some(get_usize(best, "baseline_class_patch_entries")?),
        patch_bytes: Some(get_usize(best, "baseline_class_patch_bytes")?),
    };

    let codec = if baseline_codec.total_payload_exact <= best_field.total_payload_exact {
        baseline_codec
    } else {
        best_field.clone()
    };

    Ok(LaneTableRow {
        text_id: text_id_from_path(input),
        lane_id: "whitespace-class",
        target_len: class_len,
        majority_class_label: get_string(best, "majority_class_label")?,
        majority_baseline_match_pct: get_f64(best, "majority_baseline_match_pct")?,
        best_field_chunk_bytes: get_usize(best, "chunk_bytes")?,
        best_field_boundary_band: get_usize(best, "boundary_band")?,
        best_field_margin: get_u64(best, "field_margin")?,
        best_field: best_field.clone(),
        codec,
        frontier: best_field,
    })
}

fn eval_case_lane(args: &ApexLaneTableArgs, input: &str) -> Result<LaneTableRow> {
    let stdout = run_child(
        "apex-map-case-anchor",
        &[
            "--in".to_string(),
            input.to_string(),
            "--seed-from".to_string(),
            args.seed_from.to_string(),
            "--seed-count".to_string(),
            args.seed_count.to_string(),
            "--seed-step".to_string(),
            args.seed_step.to_string(),
            "--recipe-seed".to_string(),
            args.recipe_seed.to_string(),
            "--chunk-sweep".to_string(),
            args.symbol_chunk_sweep.clone(),
            "--boundary-band-sweep".to_string(),
            args.boundary_band_sweep.clone(),
            "--field-margin-sweep".to_string(),
            args.field_margin_sweep.clone(),
            "--format".to_string(),
            "txt".to_string(),
        ],
    )?;

    let parsed = parse_multi_section_output(&stdout)?;
    if parsed.runs.is_empty() {
        bail!("apex-lane-table: no case-anchor runs parsed for {input}");
    }
    let best_idx = select_best_case_run_index(&parsed.runs)?;
    let best = &parsed.runs[best_idx];

    let codec_label = parsed
        .scalars
        .get("recommended_codec_strategy")
        .cloned()
        .ok_or_else(|| anyhow!("apex-lane-table: missing recommended_codec_strategy for {input}"))?;
    let frontier_label = parsed
        .scalars
        .get("recommended_north95_strategy")
        .cloned()
        .ok_or_else(|| anyhow!("apex-lane-table: missing recommended_north95_strategy for {input}"))?;

    let best_field = StrategyMetrics {
        label: "best-field".to_string(),
        total_payload_exact: get_usize(best, "field_total_payload_exact")?,
        raw_match_pct: get_f64(best, "field_match_pct")?,
        balanced_accuracy_pct: Some(get_f64(best, "field_balanced_accuracy_pct")?),
        macro_f1_pct: Some(get_f64(best, "field_macro_f1_pct")?),
        minority_metric_name: "upper_f1",
        minority_metric_pct: Some(get_f64(best, "field_f1_upper_pct")?),
        patch_entries: Some(get_usize(best, "field_patch_entries")?),
        patch_bytes: Some(get_usize(best, "field_patch_bytes")?),
    };

    let codec = resolve_case_strategy(&codec_label, best, &parsed.stability)?;
    let frontier = resolve_case_strategy(&frontier_label, best, &parsed.stability)?;

    Ok(LaneTableRow {
        text_id: text_id_from_path(input),
        lane_id: "case-anchor",
        target_len: get_usize(best, "case_len")?,
        majority_class_label: get_string(best, "majority_label")?,
        majority_baseline_match_pct: get_f64(best, "majority_baseline_match_pct")?,
        best_field_chunk_bytes: get_usize(best, "chunk_bytes")?,
        best_field_boundary_band: get_usize(best, "boundary_band")?,
        best_field_margin: get_u64(best, "field_margin")?,
        best_field,
        codec,
        frontier,
    })
}

fn eval_punct_kind_lane(args: &ApexLaneTableArgs, input: &str) -> Result<LaneTableRow> {
    let stdout = run_child(
        "apex-map-punct-kind",
        &[
            "--in".to_string(),
            input.to_string(),
            "--seed-from".to_string(),
            args.seed_from.to_string(),
            "--seed-count".to_string(),
            args.seed_count.to_string(),
            "--seed-step".to_string(),
            args.seed_step.to_string(),
            "--recipe-seed".to_string(),
            args.recipe_seed.to_string(),
            "--chunk-sweep".to_string(),
            args.symbol_chunk_sweep.clone(),
            "--boundary-band-sweep".to_string(),
            args.boundary_band_sweep.clone(),
            "--field-margin-sweep".to_string(),
            args.field_margin_sweep.clone(),
            "--format".to_string(),
            "txt".to_string(),
        ],
    )?;

    let parsed = parse_multi_section_output(&stdout)?;
    if parsed.runs.is_empty() {
        bail!("apex-lane-table: no punct-kind runs parsed for {input}");
    }
    let best_idx = select_best_punct_run_index(&parsed.runs)?;
    let best = &parsed.runs[best_idx];

    let codec_label = parsed
        .scalars
        .get("recommended_codec_strategy")
        .cloned()
        .ok_or_else(|| anyhow!("apex-lane-table: missing recommended_codec_strategy for {input}"))?;
    let frontier_label = parsed
        .scalars
        .get("recommended_frontier_strategy")
        .cloned()
        .ok_or_else(|| anyhow!("apex-lane-table: missing recommended_frontier_strategy for {input}"))?;

    let best_field = StrategyMetrics {
        label: "best-field".to_string(),
        total_payload_exact: get_usize(best, "field_total_payload_exact")?,
        raw_match_pct: get_f64(best, "field_match_pct")?,
        balanced_accuracy_pct: Some(get_f64(best, "field_balanced_accuracy_pct")?),
        macro_f1_pct: Some(get_f64(best, "field_macro_f1_pct")?),
        minority_metric_name: "non_majority_macro_f1",
        minority_metric_pct: Some(get_f64(best, "field_non_majority_macro_f1_pct")?),
        patch_entries: Some(get_usize(best, "field_patch_entries")?),
        patch_bytes: Some(get_usize(best, "field_patch_bytes")?),
    };

    let codec = resolve_punct_strategy(&codec_label, best, &parsed.stability)?;
    let frontier = resolve_punct_strategy(&frontier_label, best, &parsed.stability)?;

    Ok(LaneTableRow {
        text_id: text_id_from_path(input),
        lane_id: "punct-kind",
        target_len: get_usize(best, "kind_len")?,
        majority_class_label: get_string(best, "majority_class_label")?,
        majority_baseline_match_pct: get_f64(best, "majority_baseline_match_pct")?,
        best_field_chunk_bytes: get_usize(best, "chunk_bytes")?,
        best_field_boundary_band: get_usize(best, "boundary_band")?,
        best_field_margin: get_u64(best, "field_margin")?,
        best_field,
        codec,
        frontier,
    })
}

fn resolve_case_strategy(
    label: &str,
    best_run: &BTreeMap<String, String>,
    stability_rows: &[BTreeMap<String, String>],
) -> Result<StrategyMetrics> {
    if label == "best-field" {
        return Ok(StrategyMetrics {
            label: label.to_string(),
            total_payload_exact: get_usize(best_run, "field_total_payload_exact")?,
            raw_match_pct: get_f64(best_run, "field_match_pct")?,
            balanced_accuracy_pct: Some(get_f64(best_run, "field_balanced_accuracy_pct")?),
            macro_f1_pct: Some(get_f64(best_run, "field_macro_f1_pct")?),
            minority_metric_name: "upper_f1",
            minority_metric_pct: Some(get_f64(best_run, "field_f1_upper_pct")?),
            patch_entries: Some(get_usize(best_run, "field_patch_entries")?),
            patch_bytes: Some(get_usize(best_run, "field_patch_bytes")?),
        });
    }
    if label.starts_with("baseline") {
        return Ok(StrategyMetrics {
            label: label.to_string(),
            total_payload_exact: get_usize(best_run, "baseline_total_payload_exact")?,
            raw_match_pct: get_f64(best_run, "baseline_match_pct")?,
            balanced_accuracy_pct: Some(get_f64(best_run, "baseline_balanced_accuracy_pct")?),
            macro_f1_pct: Some(get_f64(best_run, "baseline_macro_f1_pct")?),
            minority_metric_name: "upper_f1",
            minority_metric_pct: Some(get_f64(best_run, "baseline_f1_upper_pct")?),
            patch_entries: Some(get_usize(best_run, "baseline_patch_entries")?),
            patch_bytes: Some(get_usize(best_run, "baseline_patch_bytes")?),
        });
    }
    if label.starts_with("hybrid") || label == "best-hybrid" {
        return Ok(StrategyMetrics {
            label: label.to_string(),
            total_payload_exact: get_usize(best_run, "hybrid_total_payload_exact")?,
            raw_match_pct: get_f64(best_run, "hybrid_match_pct")?,
            balanced_accuracy_pct: Some(get_f64(best_run, "hybrid_balanced_accuracy_pct")?),
            macro_f1_pct: Some(get_f64(best_run, "hybrid_macro_f1_pct")?),
            minority_metric_name: "upper_f1",
            minority_metric_pct: Some(get_f64(best_run, "hybrid_f1_upper_pct")?),
            patch_entries: Some(get_usize(best_run, "hybrid_patch_entries")?),
            patch_bytes: Some(get_usize(best_run, "hybrid_patch_bytes")?),
        });
    }

    let stab = stability_rows
        .iter()
        .find(|row| row.get("stability_label").map(|s| s.as_str()) == Some(label))
        .ok_or_else(|| anyhow!("apex-lane-table: case strategy '{label}' not found in stability rows"))?;

    Ok(StrategyMetrics {
        label: label.to_string(),
        total_payload_exact: get_usize(stab, "total_payload_exact")?,
        raw_match_pct: get_f64(stab, "raw_match_pct")?,
        balanced_accuracy_pct: Some(get_f64(stab, "balanced_accuracy_pct")?),
        macro_f1_pct: Some(get_f64(stab, "macro_f1_pct")?),
        minority_metric_name: "upper_f1",
        minority_metric_pct: Some(get_f64(stab, "upper_f1_pct")?),
        patch_entries: Some(get_usize(stab, "patch_entries")?),
        patch_bytes: Some(get_usize(stab, "patch_bytes")?),
    })
}

fn resolve_punct_strategy(
    label: &str,
    best_run: &BTreeMap<String, String>,
    stability_rows: &[BTreeMap<String, String>],
) -> Result<StrategyMetrics> {
    if label == "best-field" {
        return Ok(StrategyMetrics {
            label: label.to_string(),
            total_payload_exact: get_usize(best_run, "field_total_payload_exact")?,
            raw_match_pct: get_f64(best_run, "field_match_pct")?,
            balanced_accuracy_pct: Some(get_f64(best_run, "field_balanced_accuracy_pct")?),
            macro_f1_pct: Some(get_f64(best_run, "field_macro_f1_pct")?),
            minority_metric_name: "non_majority_macro_f1",
            minority_metric_pct: Some(get_f64(best_run, "field_non_majority_macro_f1_pct")?),
            patch_entries: Some(get_usize(best_run, "field_patch_entries")?),
            patch_bytes: Some(get_usize(best_run, "field_patch_bytes")?),
        });
    }
    if label.starts_with("baseline") {
        return Ok(StrategyMetrics {
            label: label.to_string(),
            total_payload_exact: get_usize(best_run, "baseline_total_payload_exact")?,
            raw_match_pct: get_f64(best_run, "baseline_match_pct")?,
            balanced_accuracy_pct: Some(get_f64(best_run, "baseline_balanced_accuracy_pct")?),
            macro_f1_pct: Some(get_f64(best_run, "baseline_macro_f1_pct")?),
            minority_metric_name: "non_majority_macro_f1",
            minority_metric_pct: Some(get_f64(best_run, "baseline_non_majority_macro_f1_pct")?),
            patch_entries: Some(get_usize(best_run, "baseline_patch_entries")?),
            patch_bytes: Some(get_usize(best_run, "baseline_patch_bytes")?),
        });
    }

    let stab = stability_rows
        .iter()
        .find(|row| row.get("stability_label").map(|s| s.as_str()) == Some(label))
        .ok_or_else(|| anyhow!("apex-lane-table: punct strategy '{label}' not found in stability rows"))?;

    Ok(StrategyMetrics {
        label: label.to_string(),
        total_payload_exact: get_usize(stab, "total_payload_exact")?,
        raw_match_pct: get_f64(stab, "raw_match_pct")?,
        balanced_accuracy_pct: Some(get_f64(stab, "balanced_accuracy_pct")?),
        macro_f1_pct: Some(get_f64(stab, "macro_f1_pct")?),
        minority_metric_name: "non_majority_macro_f1",
        minority_metric_pct: Some(get_f64(stab, "non_majority_macro_f1_pct")?),
        patch_entries: Some(get_usize(stab, "patch_entries")?),
        patch_bytes: Some(get_usize(stab, "patch_bytes")?),
    })
}

fn render_txt(rows: &[LaneTableRow]) -> String {
    let mut out = String::new();
    out.push_str("minority metric by lane: whitespace-class=newline_f1_pct, case-anchor=upper_f1_pct, punct-kind=non_majority_macro_f1_pct\n\n");
    out.push_str(&format!(
        "{:<10} {:<18} {:>8} {:<10} {:>11} {:>7} {:>6} {:>7} {:>12} {:>10} {:>11} {:>12} {:<24} {:>10} {:>11} {:<24} {:>10} {:>11}\n",
        "text",
        "lane",
        "target",
        "majority",
        "baseline%",
        "chunk",
        "band",
        "margin",
        "best_bytes",
        "best_raw%",
        "best_bal%",
        "best_minor%",
        "codec_strategy",
        "codec_bytes",
        "codec_raw%",
        "frontier_strategy",
        "frontier_bytes",
        "frontier_raw%",
    ));

    for row in rows {
        out.push_str(&format!(
            "{:<10} {:<18} {:>8} {:<10} {:>11} {:>7} {:>6} {:>7} {:>12} {:>10} {:>11} {:>12} {:<24} {:>10} {:>11} {:<24} {:>10} {:>11}\n",
            row.text_id,
            row.lane_id,
            row.target_len,
            row.majority_class_label,
            fmt6(row.majority_baseline_match_pct),
            row.best_field_chunk_bytes,
            row.best_field_boundary_band,
            row.best_field_margin,
            row.best_field.total_payload_exact,
            fmt6(row.best_field.raw_match_pct),
            fmt_opt(row.best_field.balanced_accuracy_pct),
            fmt_opt(row.best_field.minority_metric_pct),
            truncate_label(&row.codec.label, 24),
            row.codec.total_payload_exact,
            fmt6(row.codec.raw_match_pct),
            truncate_label(&row.frontier.label, 24),
            row.frontier.total_payload_exact,
            fmt6(row.frontier.raw_match_pct),
        ));
    }

    out.push('\n');
    for (idx, row) in rows.iter().enumerate() {
        if idx > 0 {
            out.push('\n');
        }
        out.push_str(&format!(
            "text={} lane={} target_len={} majority_class={} majority_baseline_match_pct={} minority_metric={}\n",
            row.text_id,
            row.lane_id,
            row.target_len,
            row.majority_class_label,
            fmt6(row.majority_baseline_match_pct),
            row.frontier.minority_metric_name,
        ));
        out.push_str(&format!(
            "best_field_chunk_bytes={} best_field_boundary_band={} best_field_margin={}\n",
            row.best_field_chunk_bytes,
            row.best_field_boundary_band,
            row.best_field_margin,
        ));
        out.push_str(&render_strategy_block("best_field", &row.best_field));
        out.push_str(&render_strategy_block("codec", &row.codec));
        out.push_str(&render_strategy_block("frontier", &row.frontier));
    }

    out
}

fn render_csv(rows: &[LaneTableRow]) -> String {
    let mut out = String::new();
    out.push_str("text_id,lane_id,target_len,majority_class_label,majority_baseline_match_pct,minority_metric_name,best_field_chunk_bytes,best_field_boundary_band,best_field_margin,best_field_label,best_field_total_payload_exact,best_field_raw_match_pct,best_field_balanced_accuracy_pct,best_field_macro_f1_pct,best_field_minority_metric_pct,best_field_patch_entries,best_field_patch_bytes,codec_label,codec_total_payload_exact,codec_raw_match_pct,codec_balanced_accuracy_pct,codec_macro_f1_pct,codec_minority_metric_pct,codec_patch_entries,codec_patch_bytes,frontier_label,frontier_total_payload_exact,frontier_raw_match_pct,frontier_balanced_accuracy_pct,frontier_macro_f1_pct,frontier_minority_metric_pct,frontier_patch_entries,frontier_patch_bytes\n");
    for row in rows {
        let cells = vec![
            row.text_id.clone(),
            row.lane_id.to_string(),
            row.target_len.to_string(),
            row.majority_class_label.clone(),
            fmt6(row.majority_baseline_match_pct),
            row.frontier.minority_metric_name.to_string(),
            row.best_field_chunk_bytes.to_string(),
            row.best_field_boundary_band.to_string(),
            row.best_field_margin.to_string(),
            row.best_field.label.clone(),
            row.best_field.total_payload_exact.to_string(),
            fmt6(row.best_field.raw_match_pct),
            fmt_opt_csv(row.best_field.balanced_accuracy_pct),
            fmt_opt_csv(row.best_field.macro_f1_pct),
            fmt_opt_csv(row.best_field.minority_metric_pct),
            fmt_opt_usize(row.best_field.patch_entries),
            fmt_opt_usize(row.best_field.patch_bytes),
            row.codec.label.clone(),
            row.codec.total_payload_exact.to_string(),
            fmt6(row.codec.raw_match_pct),
            fmt_opt_csv(row.codec.balanced_accuracy_pct),
            fmt_opt_csv(row.codec.macro_f1_pct),
            fmt_opt_csv(row.codec.minority_metric_pct),
            fmt_opt_usize(row.codec.patch_entries),
            fmt_opt_usize(row.codec.patch_bytes),
            row.frontier.label.clone(),
            row.frontier.total_payload_exact.to_string(),
            fmt6(row.frontier.raw_match_pct),
            fmt_opt_csv(row.frontier.balanced_accuracy_pct),
            fmt_opt_csv(row.frontier.macro_f1_pct),
            fmt_opt_csv(row.frontier.minority_metric_pct),
            fmt_opt_usize(row.frontier.patch_entries),
            fmt_opt_usize(row.frontier.patch_bytes),
        ];
        out.push_str(&cells.into_iter().map(csv_escape).collect::<Vec<_>>().join(","));
        out.push('\n');
    }
    out
}

fn render_strategy_block(prefix: &str, strategy: &StrategyMetrics) -> String {
    format!(
        "{}_label={} {}_total_payload_exact={} {}_raw_match_pct={} {}_balanced_accuracy_pct={} {}_macro_f1_pct={} {}_{}_pct={} {}_patch_entries={} {}_patch_bytes={}\n",
        prefix,
        strategy.label,
        prefix,
        strategy.total_payload_exact,
        prefix,
        fmt6(strategy.raw_match_pct),
        prefix,
        fmt_opt(strategy.balanced_accuracy_pct),
        prefix,
        fmt_opt(strategy.macro_f1_pct),
        prefix,
        strategy.minority_metric_name,
        fmt_opt(strategy.minority_metric_pct),
        prefix,
        fmt_opt_usize_display(strategy.patch_entries),
        prefix,
        fmt_opt_usize_display(strategy.patch_bytes),
    )
}

fn parse_ws_runs(stdout: &str) -> Result<Vec<BTreeMap<String, String>>> {
    let mut runs = Vec::new();
    let mut cur = BTreeMap::new();

    for raw_line in stdout.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line == "---" {
            if !cur.is_empty() {
                runs.push(cur);
                cur = BTreeMap::new();
            }
            continue;
        }
        if line.starts_with("input=") && !cur.is_empty() {
            runs.push(cur);
            cur = BTreeMap::new();
        }
        for (k, v) in parse_line_pairs(line)? {
            cur.insert(k, v);
        }
    }

    if !cur.is_empty() {
        runs.push(cur);
    }
    Ok(runs)
}

#[derive(Default)]
struct ParsedSections {
    runs: Vec<BTreeMap<String, String>>,
    stability: Vec<BTreeMap<String, String>>,
    scalars: BTreeMap<String, String>,
}

fn parse_multi_section_output(stdout: &str) -> Result<ParsedSections> {
    let mut parsed = ParsedSections::default();
    let mut cur = BTreeMap::new();
    let mut in_tail = false;

    for raw_line in stdout.lines() {
        let line = raw_line.trim();
        if line.is_empty() {
            if !in_tail && !cur.is_empty() {
                parsed.runs.push(cur);
                cur = BTreeMap::new();
            }
            continue;
        }

        if line.starts_with("stability_source=") {
            if !cur.is_empty() {
                parsed.runs.push(cur);
                cur = BTreeMap::new();
            }
            in_tail = true;
            for (k, v) in parse_line_pairs(line)? {
                parsed.scalars.insert(k, v);
            }
            continue;
        }

        if !in_tail {
            if line.starts_with("input=") && !cur.is_empty() {
                parsed.runs.push(cur);
                cur = BTreeMap::new();
            }
            for (k, v) in parse_line_pairs(line)? {
                cur.insert(k, v);
            }
            continue;
        }

        if line.starts_with("stability_label=") {
            let mut stab = BTreeMap::new();
            for (k, v) in parse_line_pairs(line)? {
                stab.insert(k, v);
            }
            parsed.stability.push(stab);
            continue;
        }

        for (k, v) in parse_line_pairs(line)? {
            parsed.scalars.insert(k, v);
        }
    }

    if !cur.is_empty() {
        parsed.runs.push(cur);
    }

    Ok(parsed)
}

fn parse_line_pairs(line: &str) -> Result<Vec<(String, String)>> {
    let mut pairs = Vec::new();
    for token in line.split_whitespace() {
        let (key, value) = token
            .split_once('=')
            .ok_or_else(|| anyhow!("apex-lane-table: malformed token '{token}' in line '{line}'"))?;
        pairs.push((key.to_string(), value.to_string()));
    }
    Ok(pairs)
}

fn run_child(subcmd: &str, args: &[String]) -> Result<String> {
    let exe = std::env::current_exe().context("apex-lane-table: resolve current executable")?;
    let output = Command::new(&exe)
        .arg("apextrace")
        .arg(subcmd)
        .args(args)
        .output()
        .with_context(|| format!("apex-lane-table: run child command {subcmd}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        bail!(
            "apex-lane-table: child command '{}' failed with status {}\nstdout:\n{}\nstderr:\n{}",
            subcmd,
            output.status,
            stdout.trim(),
            stderr.trim(),
        );
    }

    String::from_utf8(output.stdout).context("apex-lane-table: child stdout was not utf-8")
}

fn select_best_ws_run_index(runs: &[BTreeMap<String, String>]) -> Result<usize> {
    let mut best_idx = 0usize;
    for idx in 1..runs.len() {
        let best = &runs[best_idx];
        let cur = &runs[idx];
        let better = (
            get_bool(cur, "field_newline_extinct_flag")?,
            get_bool(cur, "field_pred_collapse_90_flag")?,
            get_usize(cur, "compact_field_total_payload_exact")?,
            get_usize(cur, "field_patch_bytes")?,
            get_i64_abs_delta(cur, "field_pred_newline_delta")?,
            -scaled_f64(cur, "field_f1_newline_pct")?,
            -scaled_f64(cur, "field_balanced_accuracy_pct")?,
            -scaled_f64(cur, "field_macro_f1_pct")?,
        ) < (
            get_bool(best, "field_newline_extinct_flag")?,
            get_bool(best, "field_pred_collapse_90_flag")?,
            get_usize(best, "compact_field_total_payload_exact")?,
            get_usize(best, "field_patch_bytes")?,
            get_i64_abs_delta(best, "field_pred_newline_delta")?,
            -scaled_f64(best, "field_f1_newline_pct")?,
            -scaled_f64(best, "field_balanced_accuracy_pct")?,
            -scaled_f64(best, "field_macro_f1_pct")?,
        );
        if better {
            best_idx = idx;
        }
    }
    Ok(best_idx)
}

fn select_best_case_run_index(runs: &[BTreeMap<String, String>]) -> Result<usize> {
    let mut best_idx = 0usize;
    for idx in 1..runs.len() {
        let best = &runs[best_idx];
        let cur = &runs[idx];
        let cur_key = (
            -scaled_f64(cur, "field_match_pct")?,
            -scaled_f64(cur, "field_f1_upper_pct")?,
            -scaled_f64(cur, "field_balanced_accuracy_pct")?,
            get_usize(cur, "field_total_payload_exact")?,
            get_usize(cur, "boundary_band")?,
            get_usize(cur, "chunk_bytes")?,
        );
        let best_key = (
            -scaled_f64(best, "field_match_pct")?,
            -scaled_f64(best, "field_f1_upper_pct")?,
            -scaled_f64(best, "field_balanced_accuracy_pct")?,
            get_usize(best, "field_total_payload_exact")?,
            get_usize(best, "boundary_band")?,
            get_usize(best, "chunk_bytes")?,
        );
        if cur_key < best_key {
            best_idx = idx;
        }
    }
    Ok(best_idx)
}

fn select_best_punct_run_index(runs: &[BTreeMap<String, String>]) -> Result<usize> {
    let mut best_idx = 0usize;
    for idx in 1..runs.len() {
        let best = &runs[best_idx];
        let cur = &runs[idx];
        let cur_key = (
            -scaled_f64(cur, "field_match_pct")?,
            -scaled_f64(cur, "field_non_majority_macro_f1_pct")?,
            get_usize(cur, "field_total_payload_exact")?,
            get_usize(cur, "boundary_band")?,
            get_usize(cur, "chunk_bytes")?,
        );
        let best_key = (
            -scaled_f64(best, "field_match_pct")?,
            -scaled_f64(best, "field_non_majority_macro_f1_pct")?,
            get_usize(best, "field_total_payload_exact")?,
            get_usize(best, "boundary_band")?,
            get_usize(best, "chunk_bytes")?,
        );
        if cur_key < best_key {
            best_idx = idx;
        }
    }
    Ok(best_idx)
}

fn text_id_from_path(path: &str) -> String {
    std::path::Path::new(path)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or(path)
        .to_string()
}

fn get_string(map: &BTreeMap<String, String>, key: &str) -> Result<String> {
    map.get(key)
        .cloned()
        .ok_or_else(|| anyhow!("apex-lane-table: missing key '{key}'"))
}

fn get_usize(map: &BTreeMap<String, String>, key: &str) -> Result<usize> {
    get_string(map, key)?
        .parse::<usize>()
        .with_context(|| format!("apex-lane-table: parse usize for key '{key}'"))
}

fn get_u64(map: &BTreeMap<String, String>, key: &str) -> Result<u64> {
    get_string(map, key)?
        .parse::<u64>()
        .with_context(|| format!("apex-lane-table: parse u64 for key '{key}'"))
}

fn get_f64(map: &BTreeMap<String, String>, key: &str) -> Result<f64> {
    get_string(map, key)?
        .parse::<f64>()
        .with_context(|| format!("apex-lane-table: parse f64 for key '{key}'"))
}

fn get_bool(map: &BTreeMap<String, String>, key: &str) -> Result<bool> {
    get_string(map, key)?
        .parse::<bool>()
        .with_context(|| format!("apex-lane-table: parse bool for key '{key}'"))
}

fn get_i64_abs_delta(map: &BTreeMap<String, String>, key: &str) -> Result<i64> {
    Ok(get_string(map, key)?
        .parse::<i64>()
        .with_context(|| format!("apex-lane-table: parse i64 for key '{key}'"))?
        .abs())
}

fn scaled_f64(map: &BTreeMap<String, String>, key: &str) -> Result<i64> {
    Ok((get_f64(map, key)? * 1_000_000.0) as i64)
}

fn fmt6(v: f64) -> String {
    format!("{v:.6}")
}

fn fmt_opt(v: Option<f64>) -> String {
    match v {
        Some(v) => fmt6(v),
        None => "-".to_string(),
    }
}

fn fmt_opt_csv(v: Option<f64>) -> String {
    match v {
        Some(v) => fmt6(v),
        None => String::new(),
    }
}

fn fmt_opt_usize(v: Option<usize>) -> String {
    match v {
        Some(v) => v.to_string(),
        None => String::new(),
    }
}

fn fmt_opt_usize_display(v: Option<usize>) -> String {
    match v {
        Some(v) => v.to_string(),
        None => "-".to_string(),
    }
}

fn truncate_label(label: &str, width: usize) -> String {
    if label.len() <= width {
        label.to_string()
    } else if width <= 1 {
        label[..width].to_string()
    } else {
        format!("{}…", &label[..width - 1])
    }
}

fn csv_escape(cell: String) -> String {
    if cell.contains(',') || cell.contains('"') || cell.contains('\n') {
        format!("\"{}\"", cell.replace('"', "\"\""))
    } else {
        cell
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_line_pairs_handles_multi_key_lines() {
        let pairs = parse_line_pairs(
            "recommended_codec_strategy=baseline-pause total_payload_exact=45 raw_match_pct=71.052632",
        )
        .expect("pairs");
        assert_eq!(pairs[0].0, "recommended_codec_strategy");
        assert_eq!(pairs[0].1, "baseline-pause");
        assert_eq!(pairs[1].0, "total_payload_exact");
        assert_eq!(pairs[1].1, "45");
    }

    #[test]
    fn parse_multi_section_output_splits_runs_and_stability() {
        let sample = "input=text/Genesis1.txt normalized_len=4201 case_len=3167\nmajority_label=lower majority_baseline_match_pct=97.284496\n\ninput=text/Genesis2.txt normalized_len=1234 case_len=900\nmajority_label=lower majority_baseline_match_pct=95.000000\n\nstability_source=field-best-chunk best_field_chunk_bytes=64 raw_guardrail_pct=95.000000\nstability_label=field-best-chunk-votes2 total_payload_exact=177 raw_match_pct=97.0 balanced_accuracy_pct=50.0 macro_f1_pct=40.0 upper_f1_pct=12.0 patch_entries=10 patch_bytes=20\nrecommended_codec_strategy=baseline-all-lower total_payload_exact=177 raw_match_pct=97.284496 upper_f1_pct=0.0\n";
        let parsed = parse_multi_section_output(sample).expect("parsed");
        assert_eq!(parsed.runs.len(), 2);
        assert_eq!(parsed.stability.len(), 1);
        assert_eq!(parsed.scalars.get("recommended_codec_strategy"), Some(&"baseline-all-lower".to_string()));
    }
}
