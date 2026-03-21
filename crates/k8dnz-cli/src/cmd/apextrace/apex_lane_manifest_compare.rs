use anyhow::{anyhow, Context, Result};
use std::collections::BTreeMap;
use std::env;
use std::process::Command;

use crate::cmd::apextrace::{ApexLaneManifestCompareArgs, ChunkSearchObjective, RenderFormat};

use super::common::write_or_print;

const LAW_MAGIC: &[u8; 4] = b"AKML";
const VERSION: u8 = 1;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ReplayLawTuple {
    boundary_band: usize,
    field_margin: u64,
    newline_demote_margin: u64,
}

#[derive(Clone, Debug)]
struct LawRow {
    local_law_id: String,
    law: ReplayLawTuple,
    window_count: usize,
    segment_count: usize,
    covered_bytes: usize,
    mean_compact_field_total_payload_exact: f64,
    mean_field_match_pct: f64,
    mean_field_match_vs_majority_pct: f64,
    mean_field_balanced_accuracy_pct: f64,
    mean_field_macro_f1_pct: f64,
    mean_field_f1_newline_pct: f64,
}

#[derive(Clone, Debug)]
struct FileReport {
    input: String,
    recipe: String,
    input_bytes: usize,
    window_bytes: usize,
    step_bytes: usize,
    windows_analyzed: usize,
    total_window_span_bytes: usize,
    coverage_bytes: usize,
    overlap_bytes: usize,
    honest_non_overlapping: bool,
    allow_overlap_scout: bool,
    distinct_law_count: usize,
    segment_count: usize,
    law_switch_count: usize,
    local_compact_payload_bytes_exact: usize,
    shared_header_bytes_exact: usize,
    law_dictionary_bytes_exact: usize,
    window_path_bytes_exact: usize,
    segment_path_bytes_exact: usize,
    selected_path_mode: String,
    selected_path_bytes_exact: usize,
    total_piecewise_payload_exact: usize,
    law_path: Vec<String>,
    laws: Vec<LawRow>,
}

#[derive(Clone, Debug)]
struct FileSharedView {
    input: String,
    total_piecewise_payload_exact: usize,
    law_dictionary_bytes_exact: usize,
    local_compact_payload_bytes_exact: usize,
    selected_path_mode: String,
    selected_path_bytes_exact: usize,
    distinct_law_count: usize,
    shared_law_path: Vec<String>,
}

#[derive(Clone, Debug)]
struct CompareSummary {
    recipe: String,
    file_count: usize,
    honest_file_count: usize,
    union_law_count: usize,
    shared_law_dictionary_bytes_exact: usize,
    separate_total_piecewise_payload_exact: usize,
    separate_total_law_dictionary_bytes_exact: usize,
    shared_total_piecewise_payload_exact: usize,
    shared_dictionary_savings_exact: isize,
}

pub fn run_apex_lane_manifest_compare(args: ApexLaneManifestCompareArgs) -> Result<()> {
    if args.inputs.len() < 2 {
        return Err(anyhow!(
            "apex-lane-manifest-compare requires at least two --in inputs"
        ));
    }

    let exe =
        env::current_exe().context("resolve current executable for apex-lane-manifest-compare")?;
    let mut reports = Vec::with_capacity(args.inputs.len());
    for input in &args.inputs {
        let output = run_child_apex_lane_manifest(&exe, &args, input)?;
        let report = parse_manifest_txt(&output)
            .with_context(|| format!("parse apex-lane-manifest output for {}", input))?;
        reports.push(report);
    }

    let shared_law_ids = build_shared_law_ids(&reports);
    let shared_laws = shared_law_ids.keys().cloned().collect::<Vec<_>>();
    let shared_law_dictionary_bytes_exact = encode_law_dictionary(&shared_laws).len();

    let file_views = reports
        .iter()
        .map(|report| FileSharedView {
            input: report.input.clone(),
            total_piecewise_payload_exact: report.total_piecewise_payload_exact,
            law_dictionary_bytes_exact: report.law_dictionary_bytes_exact,
            local_compact_payload_bytes_exact: report.local_compact_payload_bytes_exact,
            selected_path_mode: report.selected_path_mode.clone(),
            selected_path_bytes_exact: report.selected_path_bytes_exact,
            distinct_law_count: report.distinct_law_count,
            shared_law_path: remap_law_path(report, &shared_law_ids),
        })
        .collect::<Vec<_>>();

    let separate_total_piecewise_payload_exact = reports
        .iter()
        .map(|r| r.total_piecewise_payload_exact)
        .sum::<usize>();
    let separate_total_law_dictionary_bytes_exact = reports
        .iter()
        .map(|r| r.law_dictionary_bytes_exact)
        .sum::<usize>();
    let shared_total_piecewise_payload_exact = separate_total_piecewise_payload_exact
        .saturating_sub(separate_total_law_dictionary_bytes_exact)
        .saturating_add(shared_law_dictionary_bytes_exact);

    let summary = CompareSummary {
        recipe: reports
            .first()
            .map(|r| r.recipe.clone())
            .unwrap_or_else(|| args.recipe.clone()),
        file_count: reports.len(),
        honest_file_count: reports.iter().filter(|r| r.honest_non_overlapping).count(),
        union_law_count: shared_laws.len(),
        shared_law_dictionary_bytes_exact,
        separate_total_piecewise_payload_exact,
        separate_total_law_dictionary_bytes_exact,
        shared_total_piecewise_payload_exact,
        shared_dictionary_savings_exact: separate_total_piecewise_payload_exact as isize
            - shared_total_piecewise_payload_exact as isize,
    };

    let body = match args.format {
        RenderFormat::Txt => render_txt(&summary, &reports, &file_views, &shared_law_ids),
        RenderFormat::Csv => render_csv(&summary, &reports, &file_views, &shared_law_ids),
    };

    write_or_print(args.out.as_deref(), &body)?;

    if let Some(path) = args.out.as_deref() {
        eprintln!(
            "apex-lane-manifest-compare: out={} files={} union_laws={} shared_dict_bytes={} savings={}",
            path,
            summary.file_count,
            summary.union_law_count,
            summary.shared_law_dictionary_bytes_exact,
            summary.shared_dictionary_savings_exact,
        );
    } else {
        eprintln!(
            "apex-lane-manifest-compare: files={} union_laws={} shared_dict_bytes={} savings={}",
            summary.file_count,
            summary.union_law_count,
            summary.shared_law_dictionary_bytes_exact,
            summary.shared_dictionary_savings_exact,
        );
    }

    Ok(())
}

fn run_child_apex_lane_manifest(
    exe: &std::path::Path,
    args: &ApexLaneManifestCompareArgs,
    input: &str,
) -> Result<String> {
    let mut cmd = Command::new(exe);
    cmd.arg("apextrace")
        .arg("apex-lane-manifest")
        .arg("--recipe")
        .arg(&args.recipe)
        .arg("--in")
        .arg(input)
        .arg("--max-ticks")
        .arg(args.max_ticks.to_string())
        .arg("--window-bytes")
        .arg(args.window_bytes.to_string())
        .arg("--step-bytes")
        .arg(args.step_bytes.to_string())
        .arg("--max-windows")
        .arg(args.max_windows.to_string())
        .arg("--seed-from")
        .arg(args.seed_from.to_string())
        .arg("--seed-count")
        .arg(args.seed_count.to_string())
        .arg("--seed-step")
        .arg(args.seed_step.to_string())
        .arg("--recipe-seed")
        .arg(args.recipe_seed.to_string())
        .arg("--chunk-sweep")
        .arg(&args.chunk_sweep)
        .arg("--chunk-search-objective")
        .arg(chunk_search_objective_name(args.chunk_search_objective))
        .arg("--chunk-raw-slack")
        .arg(args.chunk_raw_slack.to_string())
        .arg("--map-max-depth")
        .arg(args.map_max_depth.to_string())
        .arg("--map-depth-shift")
        .arg(args.map_depth_shift.to_string())
        .arg("--boundary-band-sweep")
        .arg(&args.boundary_band_sweep)
        .arg("--boundary-delta")
        .arg(args.boundary_delta.to_string())
        .arg("--field-margin-sweep")
        .arg(&args.field_margin_sweep)
        .arg("--newline-margin-add")
        .arg(args.newline_margin_add.to_string())
        .arg("--space-to-newline-margin-add")
        .arg(args.space_to_newline_margin_add.to_string())
        .arg("--newline-share-ppm-min")
        .arg(args.newline_share_ppm_min.to_string())
        .arg("--newline-override-budget")
        .arg(args.newline_override_budget.to_string())
        .arg("--newline-demote-margin-sweep")
        .arg(&args.newline_demote_margin_sweep)
        .arg("--newline-demote-keep-ppm-min")
        .arg(args.newline_demote_keep_ppm_min.to_string())
        .arg("--newline-demote-keep-min")
        .arg(args.newline_demote_keep_min.to_string())
        .arg(format!(
            "--newline-only-from-spacelike={}",
            if args.newline_only_from_spacelike {
                "true"
            } else {
                "false"
            }
        ))
        .arg("--merge-gap-bytes")
        .arg(args.merge_gap_bytes.to_string())
        .arg("--format")
        .arg("txt");

    if args.field_from_global {
        cmd.arg("--field-from-global");
    }

    if args.allow_overlap_scout {
        cmd.arg("--allow-overlap-scout");
    }

    let output = cmd
        .output()
        .with_context(|| format!("spawn child apex-lane-manifest for {}", input))?;

    if !output.status.success() {
        return Err(anyhow!(
            "child apex-lane-manifest failed input={} status={} stderr={} stdout={}",
            input,
            output.status,
            String::from_utf8_lossy(&output.stderr),
            String::from_utf8_lossy(&output.stdout),
        ));
    }

    String::from_utf8(output.stdout).context("child apex-lane-manifest stdout was not valid UTF-8")
}

fn parse_manifest_txt(raw: &str) -> Result<FileReport> {
    let mut summary = BTreeMap::<String, String>::new();
    let mut section = "summary";
    let mut laws = Vec::new();

    for line in raw.lines().map(str::trim).filter(|line| !line.is_empty()) {
        match line {
            "--- laws ---" => {
                section = "laws";
                continue;
            }
            "--- segments ---" => {
                section = "segments";
                continue;
            }
            "--- windows ---" => {
                section = "windows";
                continue;
            }
            _ => {}
        }

        match section {
            "summary" => {
                if let Some((k, v)) = line.split_once('=') {
                    summary.insert(k.to_string(), v.to_string());
                }
            }
            "laws" => laws.push(parse_law_row(line)?),
            _ => {}
        }
    }

    Ok(FileReport {
        input: parse_required_string(&summary, "input")?,
        recipe: parse_required_string(&summary, "recipe")?,
        input_bytes: parse_required_usize(&summary, "input_bytes")?,
        window_bytes: parse_required_usize(&summary, "window_bytes")?,
        step_bytes: parse_required_usize(&summary, "step_bytes")?,
        windows_analyzed: parse_required_usize(&summary, "windows_analyzed")?,
        total_window_span_bytes: parse_required_usize(&summary, "total_window_span_bytes")?,
        coverage_bytes: parse_required_usize(&summary, "coverage_bytes")?,
        overlap_bytes: parse_required_usize(&summary, "overlap_bytes")?,
        honest_non_overlapping: parse_required_bool(&summary, "honest_non_overlapping")?,
        allow_overlap_scout: parse_required_bool(&summary, "allow_overlap_scout")?,
        distinct_law_count: parse_required_usize(&summary, "distinct_law_count")?,
        segment_count: parse_required_usize(&summary, "segment_count")?,
        law_switch_count: parse_required_usize(&summary, "law_switch_count")?,
        local_compact_payload_bytes_exact: parse_required_usize(
            &summary,
            "local_compact_payload_bytes_exact",
        )?,
        shared_header_bytes_exact: parse_required_usize(&summary, "shared_header_bytes_exact")?,
        law_dictionary_bytes_exact: parse_required_usize(&summary, "law_dictionary_bytes_exact")?,
        window_path_bytes_exact: parse_required_usize(&summary, "window_path_bytes_exact")?,
        segment_path_bytes_exact: parse_required_usize(&summary, "segment_path_bytes_exact")?,
        selected_path_mode: parse_required_string(&summary, "selected_path_mode")?,
        selected_path_bytes_exact: parse_required_usize(&summary, "selected_path_bytes_exact")?,
        total_piecewise_payload_exact: parse_required_usize(
            &summary,
            "total_piecewise_payload_exact",
        )?,
        law_path: parse_required_string(&summary, "law_path")?
            .split(',')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect(),
        laws,
    })
}

fn parse_law_row(line: &str) -> Result<LawRow> {
    let tokens = tokenize_kv_line(line);
    Ok(LawRow {
        local_law_id: parse_required_string(&tokens, "law_id")?,
        law: ReplayLawTuple {
            boundary_band: parse_required_usize(&tokens, "boundary_band")?,
            field_margin: parse_required_u64(&tokens, "field_margin")?,
            newline_demote_margin: parse_required_u64(&tokens, "newline_demote_margin")?,
        },
        window_count: parse_required_usize(&tokens, "window_count")?,
        segment_count: parse_required_usize(&tokens, "segment_count")?,
        covered_bytes: parse_required_usize(&tokens, "covered_bytes")?,
        mean_compact_field_total_payload_exact: parse_required_f64(
            &tokens,
            "mean_compact_field_total_payload_exact",
        )?,
        mean_field_match_pct: parse_required_f64(&tokens, "mean_field_match_pct")?,
        mean_field_match_vs_majority_pct: parse_required_f64(
            &tokens,
            "mean_field_match_vs_majority_pct",
        )?,
        mean_field_balanced_accuracy_pct: parse_required_f64(
            &tokens,
            "mean_field_balanced_accuracy_pct",
        )?,
        mean_field_macro_f1_pct: parse_required_f64(&tokens, "mean_field_macro_f1_pct")?,
        mean_field_f1_newline_pct: parse_required_f64(&tokens, "mean_field_f1_newline_pct")?,
    })
}

fn tokenize_kv_line(line: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for token in line.split_whitespace() {
        if let Some((k, v)) = token.split_once('=') {
            out.insert(k.to_string(), v.to_string());
        }
    }
    out
}

fn build_shared_law_ids(reports: &[FileReport]) -> BTreeMap<ReplayLawTuple, String> {
    let mut tuples = Vec::<ReplayLawTuple>::new();
    for report in reports {
        for law in &report.laws {
            tuples.push(law.law.clone());
        }
    }
    tuples.sort();
    tuples.dedup();

    let mut out = BTreeMap::<ReplayLawTuple, String>::new();
    for (idx, law) in tuples.into_iter().enumerate() {
        out.insert(law, format!("G{}", idx));
    }
    out
}

fn remap_law_path(
    report: &FileReport,
    shared_law_ids: &BTreeMap<ReplayLawTuple, String>,
) -> Vec<String> {
    let local_to_global = report
        .laws
        .iter()
        .map(|law| {
            let global = shared_law_ids
                .get(&law.law)
                .cloned()
                .unwrap_or_else(|| "G?".to_string());
            (law.local_law_id.clone(), global)
        })
        .collect::<BTreeMap<_, _>>();

    report
        .law_path
        .iter()
        .map(|local| {
            local_to_global
                .get(local)
                .cloned()
                .unwrap_or_else(|| "G?".to_string())
        })
        .collect()
}

fn encode_law_dictionary(laws: &[ReplayLawTuple]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(LAW_MAGIC);
    out.push(VERSION);
    out.push(0);
    put_varint(laws.len() as u64, &mut out);
    for law in laws {
        put_varint(law.boundary_band as u64, &mut out);
        put_varint(law.field_margin, &mut out);
        put_varint(law.newline_demote_margin, &mut out);
    }
    out
}

fn put_varint(mut v: u64, out: &mut Vec<u8>) {
    loop {
        let byte = (v & 0x7F) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            break;
        } else {
            out.push(byte | 0x80);
        }
    }
}

fn render_txt(
    summary: &CompareSummary,
    reports: &[FileReport],
    file_views: &[FileSharedView],
    shared_law_ids: &BTreeMap<ReplayLawTuple, String>,
) -> String {
    let mut out = String::new();
    push_line(&mut out, "recipe", &summary.recipe);
    push_line(&mut out, "file_count", summary.file_count);
    push_line(&mut out, "honest_file_count", summary.honest_file_count);
    push_line(&mut out, "union_law_count", summary.union_law_count);
    push_line(
        &mut out,
        "shared_law_dictionary_bytes_exact",
        summary.shared_law_dictionary_bytes_exact,
    );
    push_line(
        &mut out,
        "separate_total_piecewise_payload_exact",
        summary.separate_total_piecewise_payload_exact,
    );
    push_line(
        &mut out,
        "separate_total_law_dictionary_bytes_exact",
        summary.separate_total_law_dictionary_bytes_exact,
    );
    push_line(
        &mut out,
        "shared_total_piecewise_payload_exact",
        summary.shared_total_piecewise_payload_exact,
    );
    push_line(
        &mut out,
        "shared_dictionary_savings_exact",
        summary.shared_dictionary_savings_exact,
    );

    out.push_str("\n--- shared-laws ---\n");
    for (law, global_id) in shared_law_ids {
        out.push_str(&format!(
            "global_law_id={} boundary_band={} field_margin={} newline_demote_margin={}\n",
            global_id, law.boundary_band, law.field_margin, law.newline_demote_margin,
        ));
    }

    out.push_str("\n--- files ---\n");
    for (report, view) in reports.iter().zip(file_views.iter()) {
        out.push_str(&format!(
            "input={} honest_non_overlapping={} input_bytes={} windows_analyzed={} total_window_span_bytes={} coverage_bytes={} overlap_bytes={} distinct_law_count={} segment_count={} law_switch_count={} local_compact_payload_bytes_exact={} shared_header_bytes_exact={} law_dictionary_bytes_exact={} selected_path_mode={} selected_path_bytes_exact={} total_piecewise_payload_exact={} shared_law_path={}\n",
            report.input,
            report.honest_non_overlapping,
            report.input_bytes,
            report.windows_analyzed,
            report.total_window_span_bytes,
            report.coverage_bytes,
            report.overlap_bytes,
            view.distinct_law_count,
            report.segment_count,
            report.law_switch_count,
            view.local_compact_payload_bytes_exact,
            report.shared_header_bytes_exact,
            view.law_dictionary_bytes_exact,
            view.selected_path_mode,
            view.selected_path_bytes_exact,
            view.total_piecewise_payload_exact,
            view.shared_law_path.join(","),
        ));
    }

    out
}

fn render_csv(
    summary: &CompareSummary,
    reports: &[FileReport],
    file_views: &[FileSharedView],
    shared_law_ids: &BTreeMap<ReplayLawTuple, String>,
) -> String {
    let mut out = String::new();
    push_csv_row(
        &mut out,
        &[
            "row_kind",
            "id",
            "input",
            "honest_non_overlapping",
            "union_law_count",
            "global_law_id",
            "boundary_band",
            "field_margin",
            "newline_demote_margin",
            "shared_law_dictionary_bytes_exact",
            "separate_total_piecewise_payload_exact",
            "separate_total_law_dictionary_bytes_exact",
            "shared_total_piecewise_payload_exact",
            "shared_dictionary_savings_exact",
            "local_compact_payload_bytes_exact",
            "law_dictionary_bytes_exact",
            "selected_path_mode",
            "selected_path_bytes_exact",
            "total_piecewise_payload_exact",
            "shared_law_path",
        ],
    );

    push_csv_row(
        &mut out,
        &[
            "summary",
            "summary",
            "",
            "",
            &summary.union_law_count.to_string(),
            "",
            "",
            "",
            "",
            &summary.shared_law_dictionary_bytes_exact.to_string(),
            &summary.separate_total_piecewise_payload_exact.to_string(),
            &summary.separate_total_law_dictionary_bytes_exact.to_string(),
            &summary.shared_total_piecewise_payload_exact.to_string(),
            &summary.shared_dictionary_savings_exact.to_string(),
            "",
            "",
            "",
            "",
            "",
            "",
        ],
    );

    for (law, global_id) in shared_law_ids {
        push_csv_row(
            &mut out,
            &[
                "shared-law",
                global_id,
                "",
                "",
                "",
                global_id,
                &law.boundary_band.to_string(),
                &law.field_margin.to_string(),
                &law.newline_demote_margin.to_string(),
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
            ],
        );
    }

    for (report, view) in reports.iter().zip(file_views.iter()) {
        push_csv_row(
            &mut out,
            &[
                "file",
                &view.input,
                &view.input,
                &report.honest_non_overlapping.to_string(),
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                &view.local_compact_payload_bytes_exact.to_string(),
                &view.law_dictionary_bytes_exact.to_string(),
                &view.selected_path_mode,
                &view.selected_path_bytes_exact.to_string(),
                &view.total_piecewise_payload_exact.to_string(),
                &view.shared_law_path.join(","),
            ],
        );
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

fn chunk_search_objective_name(value: ChunkSearchObjective) -> &'static str {
    match value {
        ChunkSearchObjective::Raw => "raw",
        ChunkSearchObjective::RawGuarded => "raw-guarded",
        ChunkSearchObjective::Honest => "honest",
        ChunkSearchObjective::Newline => "newline",
    }
}

fn parse_required_string(map: &BTreeMap<String, String>, key: &str) -> Result<String> {
    map.get(key)
        .cloned()
        .ok_or_else(|| anyhow!("missing key {}", key))
}

fn parse_required_usize(map: &BTreeMap<String, String>, key: &str) -> Result<usize> {
    let raw = parse_required_string(map, key)?;
    raw.parse::<usize>()
        .with_context(|| format!("parse usize key {} from {}", key, raw))
}

fn parse_required_u64(map: &BTreeMap<String, String>, key: &str) -> Result<u64> {
    let raw = parse_required_string(map, key)?;
    raw.parse::<u64>()
        .with_context(|| format!("parse u64 key {} from {}", key, raw))
}

fn parse_required_f64(map: &BTreeMap<String, String>, key: &str) -> Result<f64> {
    let raw = parse_required_string(map, key)?;
    raw.parse::<f64>()
        .with_context(|| format!("parse f64 key {} from {}", key, raw))
}

fn parse_required_bool(map: &BTreeMap<String, String>, key: &str) -> Result<bool> {
    let raw = parse_required_string(map, key)?;
    raw.parse::<bool>()
        .with_context(|| format!("parse bool key {} from {}", key, raw))
}

#[cfg(test)]
mod tests {
    use super::{
        build_shared_law_ids, encode_law_dictionary, parse_manifest_txt, remap_law_path,
        ReplayLawTuple,
    };

    fn sample_manifest(input: &str, law_path: &str, laws: &[(&str, usize, u64, u64)]) -> String {
        let mut out = String::new();
        out.push_str(&format!("input={}\n", input));
        out.push_str("recipe=configs/tuned_validated.k8r\n");
        out.push_str("input_bytes=3072\n");
        out.push_str("window_bytes=256\n");
        out.push_str("step_bytes=256\n");
        out.push_str("windows_analyzed=12\n");
        out.push_str("total_window_span_bytes=3072\n");
        out.push_str("coverage_bytes=3072\n");
        out.push_str("overlap_bytes=0\n");
        out.push_str("honest_non_overlapping=true\n");
        out.push_str("allow_overlap_scout=false\n");
        out.push_str(&format!("distinct_law_count={}\n", laws.len()));
        out.push_str("segment_count=3\n");
        out.push_str("law_switch_count=2\n");
        out.push_str("boundary_delta=1\n");
        out.push_str("map_max_depth=0\n");
        out.push_str("map_depth_shift=1\n");
        out.push_str("newline_margin_add=96\n");
        out.push_str("space_to_newline_margin_add=64\n");
        out.push_str("newline_share_ppm_min=550000\n");
        out.push_str("newline_override_budget=0\n");
        out.push_str("newline_demote_keep_ppm_min=150000\n");
        out.push_str("newline_demote_keep_min=1\n");
        out.push_str("newline_only_from_spacelike=true\n");
        out.push_str("local_compact_payload_bytes_exact=2142\n");
        out.push_str("shared_header_bytes_exact=24\n");
        out.push_str("law_dictionary_bytes_exact=13\n");
        out.push_str("window_path_bytes_exact=30\n");
        out.push_str("segment_path_bytes_exact=21\n");
        out.push_str("selected_path_mode=segment\n");
        out.push_str("selected_path_bytes_exact=21\n");
        out.push_str("total_piecewise_payload_exact=2200\n");
        out.push_str(&format!("law_path={}\n", law_path));
        out.push_str("\n--- laws ---\n");
        for (id, band, margin, demote) in laws {
            out.push_str(&format!(
                "law_id={} boundary_band={} field_margin={} newline_demote_margin={} window_count=6 segment_count=1 covered_bytes=1536 mean_compact_field_total_payload_exact=178.500 mean_field_match_pct=70.000000 mean_field_match_vs_majority_pct=-20.000000 mean_field_balanced_accuracy_pct=50.000000 mean_field_macro_f1_pct=45.000000 mean_field_f1_newline_pct=10.000000\n",
                id, band, margin, demote,
            ));
        }
        out.push_str("\n--- segments ---\nsegment_idx=0 law_id=L0 start=0 end=1536 span_bytes=1536 window_count=6 first_window_idx=0 last_window_idx=5 mean_compact_field_total_payload_exact=178.500 mean_field_match_pct=70.000000 mean_field_balanced_accuracy_pct=50.000000 mean_field_macro_f1_pct=45.000000 mean_field_f1_newline_pct=10.000000\n");
        out.push_str("\n--- windows ---\nwindow_idx=0 law_id=L0 start=0 end=256 span_bytes=256 chunk_bytes=64 boundary_band=12 field_margin=4 newline_demote_margin=4 chunk_search_objective=raw chunk_raw_slack=1 compact_field_total_payload_exact=177 field_patch_bytes=90 field_match_pct=70.312500 majority_baseline_match_pct=89.000000 field_match_vs_majority_pct=-18.687500 field_balanced_accuracy_pct=50.000000 field_macro_f1_pct=45.000000 field_f1_newline_pct=10.000000 field_pred_dominant_label=space field_pred_dominant_share_pct=90.000000 field_pred_collapse_90_flag=true field_pred_newline_delta=-3 field_newline_demoted=1 field_newline_after_demote=5 field_newline_floor_used=1 field_newline_extinct_flag=false\n");
        out
    }

    #[test]
    fn parse_manifest_extracts_summary_and_laws() {
        let raw = sample_manifest(
            "text/Genesis1.txt",
            "L0,L0,L1",
            &[("L0", 12, 4, 4), ("L1", 8, 4, 4)],
        );
        let report = parse_manifest_txt(&raw).expect("parse manifest txt");
        assert_eq!(report.input, "text/Genesis1.txt");
        assert_eq!(report.distinct_law_count, 2);
        assert_eq!(report.law_path, vec!["L0", "L0", "L1"]);
        assert_eq!(report.laws.len(), 2);
        assert_eq!(report.laws[0].law.boundary_band, 12);
    }

    #[test]
    fn shared_law_ids_are_stable_by_tuple_order() {
        let a = parse_manifest_txt(&sample_manifest(
            "text/Genesis1.txt",
            "L0,L1",
            &[("L0", 12, 4, 4), ("L1", 8, 4, 4)],
        ))
        .expect("parse A");
        let b = parse_manifest_txt(&sample_manifest(
            "text/Genesis2.txt",
            "L0,L0",
            &[("L0", 12, 4, 4)],
        ))
        .expect("parse B");
        let ids = build_shared_law_ids(&[a.clone(), b.clone()]);
        assert_eq!(ids.len(), 2);
        assert_eq!(
            ids.get(&ReplayLawTuple {
                boundary_band: 8,
                field_margin: 4,
                newline_demote_margin: 4,
            })
            .cloned(),
            Some("G0".to_string())
        );
        assert_eq!(
            ids.get(&ReplayLawTuple {
                boundary_band: 12,
                field_margin: 4,
                newline_demote_margin: 4,
            })
            .cloned(),
            Some("G1".to_string())
        );

        let remapped = remap_law_path(&a, &ids);
        assert_eq!(remapped, vec!["G1", "G0"]);
    }

    #[test]
    fn shared_dictionary_is_smaller_than_two_identical_dicts() {
        let laws = vec![
            ReplayLawTuple {
                boundary_band: 8,
                field_margin: 4,
                newline_demote_margin: 4,
            },
            ReplayLawTuple {
                boundary_band: 12,
                field_margin: 4,
                newline_demote_margin: 4,
            },
        ];
        let shared = encode_law_dictionary(&laws).len();
        let separate = shared * 2;
        assert!(shared < separate);
    }
}