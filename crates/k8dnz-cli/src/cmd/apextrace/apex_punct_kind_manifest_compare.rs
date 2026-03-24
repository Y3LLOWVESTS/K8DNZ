use anyhow::{anyhow, Context, Result};
use std::collections::BTreeMap;
use std::env;
use std::process::Command;

use crate::cmd::apextrace::{ApexPunctKindManifestCompareArgs, ChunkSearchObjective, RenderFormat};

use super::common::write_or_print;

const LAW_MAGIC: &[u8; 4] = b"PKML";
const VERSION: u8 = 1;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ReplayLawTuple {
    boundary_band: usize,
    field_margin: u64,
}

#[derive(Clone, Debug)]
struct LawRow {
    local_law_id: String,
    law: ReplayLawTuple,
    window_count: usize,
    segment_count: usize,
    covered_bytes: usize,
    mean_field_total_payload_exact: f64,
    mean_field_match_pct: f64,
    mean_field_balanced_accuracy_pct: f64,
    mean_field_macro_f1_pct: f64,
    mean_field_non_majority_macro_f1_pct: f64,
}

#[derive(Clone, Debug)]
struct FileReport {
    input: String,
    input_bytes: usize,
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
    file_count: usize,
    honest_file_count: usize,
    union_law_count: usize,
    shared_law_dictionary_bytes_exact: usize,
    separate_total_piecewise_payload_exact: usize,
    separate_total_law_dictionary_bytes_exact: usize,
    shared_total_piecewise_payload_exact: usize,
    shared_dictionary_savings_exact: isize,
}

pub fn run_apex_punct_kind_manifest_compare(args: ApexPunctKindManifestCompareArgs) -> Result<()> {
    if args.inputs.len() < 2 {
        return Err(anyhow!("apex-punct-kind-manifest-compare requires at least two --in inputs"));
    }
    let exe = env::current_exe().context("resolve current executable for apex-punct-kind-manifest-compare")?;
    let mut reports = Vec::with_capacity(args.inputs.len());
    for input in &args.inputs {
        let output = run_child_apex_punct_kind_manifest(&exe, &args, input)?;
        let report = parse_manifest_txt(&output).with_context(|| format!("parse apex-punct-kind-manifest output for {}", input))?;
        reports.push(report);
    }

    let shared_law_ids = build_shared_law_ids(&reports);
    let shared_laws = shared_law_ids.keys().cloned().collect::<Vec<_>>();
    let shared_law_dictionary_bytes_exact = encode_law_dictionary(&shared_laws).len();
    let file_views = reports.iter().map(|report| FileSharedView {
        input: report.input.clone(),
        total_piecewise_payload_exact: report.total_piecewise_payload_exact,
        law_dictionary_bytes_exact: report.law_dictionary_bytes_exact,
        local_compact_payload_bytes_exact: report.local_compact_payload_bytes_exact,
        selected_path_mode: report.selected_path_mode.clone(),
        selected_path_bytes_exact: report.selected_path_bytes_exact,
        distinct_law_count: report.distinct_law_count,
        shared_law_path: remap_law_path(report, &shared_law_ids),
    }).collect::<Vec<_>>();

    let separate_total_piecewise_payload_exact = reports.iter().map(|r| r.total_piecewise_payload_exact).sum::<usize>();
    let separate_total_law_dictionary_bytes_exact = reports.iter().map(|r| r.law_dictionary_bytes_exact).sum::<usize>();
    let shared_total_piecewise_payload_exact = separate_total_piecewise_payload_exact
        .saturating_sub(separate_total_law_dictionary_bytes_exact)
        .saturating_add(shared_law_dictionary_bytes_exact);

    let summary = CompareSummary {
        file_count: reports.len(),
        honest_file_count: reports.iter().filter(|r| r.honest_non_overlapping).count(),
        union_law_count: shared_laws.len(),
        shared_law_dictionary_bytes_exact,
        separate_total_piecewise_payload_exact,
        separate_total_law_dictionary_bytes_exact,
        shared_total_piecewise_payload_exact,
        shared_dictionary_savings_exact: separate_total_piecewise_payload_exact as isize - shared_total_piecewise_payload_exact as isize,
    };

    let body = match args.format {
        RenderFormat::Txt => render_txt(&summary, &reports, &file_views, &shared_law_ids),
        RenderFormat::Csv => render_csv(&summary, &reports, &file_views, &shared_law_ids),
    };
    write_or_print(args.out.as_deref(), &body)?;
    Ok(())
}

fn run_child_apex_punct_kind_manifest(exe: &std::path::Path, args: &ApexPunctKindManifestCompareArgs, input: &str) -> Result<String> {
    let mut cmd = Command::new(exe);
    cmd.arg("apextrace")
        .arg("apex-punct-kind-manifest")
        .arg("--in").arg(input)
        .arg("--window-bytes").arg(args.window_bytes.to_string())
        .arg("--step-bytes").arg(args.step_bytes.to_string())
        .arg("--max-windows").arg(args.max_windows.to_string())
        .arg("--seed-from").arg(args.seed_from.to_string())
        .arg("--seed-count").arg(args.seed_count.to_string())
        .arg("--seed-step").arg(args.seed_step.to_string())
        .arg("--recipe-seed").arg(args.recipe_seed.to_string())
        .arg("--chunk-sweep").arg(&args.chunk_sweep)
        .arg("--chunk-search-objective").arg(chunk_search_objective_name(args.chunk_search_objective))
        .arg("--chunk-raw-slack").arg(args.chunk_raw_slack.to_string())
        .arg("--map-max-depth").arg(args.map_max_depth.to_string())
        .arg("--map-depth-shift").arg(args.map_depth_shift.to_string())
        .arg("--boundary-band-sweep").arg(&args.boundary_band_sweep)
        .arg("--boundary-delta").arg(args.boundary_delta.to_string())
        .arg("--field-margin-sweep").arg(&args.field_margin_sweep)
        .arg("--term-margin-add").arg(args.term_margin_add.to_string())
        .arg("--pause-margin-add").arg(args.pause_margin_add.to_string())
        .arg("--wrap-margin-add").arg(args.wrap_margin_add.to_string())
        .arg("--term-share-ppm-min").arg(args.term_share_ppm_min.to_string())
        .arg("--pause-share-ppm-min").arg(args.pause_share_ppm_min.to_string())
        .arg("--wrap-share-ppm-min").arg(args.wrap_share_ppm_min.to_string())
        .arg("--merge-gap-bytes").arg(args.merge_gap_bytes.to_string())
        .arg("--format").arg("txt");
    if args.allow_overlap_scout { cmd.arg("--allow-overlap-scout"); }
    let output = cmd.output().with_context(|| format!("spawn child apex-punct-kind-manifest for {}", input))?;
    if !output.status.success() {
        return Err(anyhow!("child apex-punct-kind-manifest failed input={} status={} stderr={} stdout={}", input, output.status, String::from_utf8_lossy(&output.stderr), String::from_utf8_lossy(&output.stdout)));
    }
    String::from_utf8(output.stdout).context("child apex-punct-kind-manifest stdout was not valid UTF-8")
}

fn parse_manifest_txt(raw: &str) -> Result<FileReport> {
    let mut summary = BTreeMap::<String, String>::new();
    let mut section = "summary";
    let mut laws = Vec::new();
    for line in raw.lines().map(str::trim).filter(|line| !line.is_empty()) {
        match line {
            "--- laws ---" => { section = "laws"; continue; }
            "--- segments ---" => { section = "segments"; continue; }
            "--- windows ---" => { section = "windows"; continue; }
            _ => {}
        }
        match section {
            "summary" => if let Some((k,v)) = line.split_once('=') { summary.insert(k.to_string(), v.to_string()); },
            "laws" => laws.push(parse_law_row(line)?),
            _ => {}
        }
    }
    Ok(FileReport {
        input: parse_required_string(&summary, "input")?,
        input_bytes: parse_required_usize(&summary, "input_bytes")?,
        windows_analyzed: parse_required_usize(&summary, "windows_analyzed")?,
        total_window_span_bytes: parse_required_usize(&summary, "total_window_span_bytes")?,
        coverage_bytes: parse_required_usize(&summary, "coverage_bytes")?,
        overlap_bytes: parse_required_usize(&summary, "overlap_bytes")?,
        honest_non_overlapping: parse_required_bool(&summary, "honest_non_overlapping")?,
        allow_overlap_scout: parse_required_bool(&summary, "allow_overlap_scout")?,
        distinct_law_count: parse_required_usize(&summary, "distinct_law_count")?,
        segment_count: parse_required_usize(&summary, "segment_count")?,
        law_switch_count: parse_required_usize(&summary, "law_switch_count")?,
        local_compact_payload_bytes_exact: parse_required_usize(&summary, "local_compact_payload_bytes_exact")?,
        shared_header_bytes_exact: parse_required_usize(&summary, "shared_header_bytes_exact")?,
        law_dictionary_bytes_exact: parse_required_usize(&summary, "law_dictionary_bytes_exact")?,
        window_path_bytes_exact: parse_required_usize(&summary, "window_path_bytes_exact")?,
        segment_path_bytes_exact: parse_required_usize(&summary, "segment_path_bytes_exact")?,
        selected_path_mode: parse_required_string(&summary, "selected_path_mode")?,
        selected_path_bytes_exact: parse_required_usize(&summary, "selected_path_bytes_exact")?,
        total_piecewise_payload_exact: parse_required_usize(&summary, "total_piecewise_payload_exact")?,
        law_path: parse_required_string(&summary, "law_path")?.split(',').filter(|s| !s.is_empty()).map(|s| s.to_string()).collect(),
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
        },
        window_count: parse_required_usize(&tokens, "window_count")?,
        segment_count: parse_required_usize(&tokens, "segment_count")?,
        covered_bytes: parse_required_usize(&tokens, "covered_bytes")?,
        mean_field_total_payload_exact: parse_required_f64(&tokens, "mean_field_total_payload_exact")?,
        mean_field_match_pct: parse_required_f64(&tokens, "mean_field_match_pct")?,
        mean_field_balanced_accuracy_pct: parse_required_f64(&tokens, "mean_field_balanced_accuracy_pct")?,
        mean_field_macro_f1_pct: parse_required_f64(&tokens, "mean_field_macro_f1_pct")?,
        mean_field_non_majority_macro_f1_pct: parse_required_f64(&tokens, "mean_field_non_majority_macro_f1_pct")?,
    })
}

fn build_shared_law_ids(reports: &[FileReport]) -> BTreeMap<ReplayLawTuple, String> {
    let mut tuples = Vec::<ReplayLawTuple>::new();
    for report in reports { for law in &report.laws { tuples.push(law.law.clone()); } }
    tuples.sort(); tuples.dedup();
    let mut out = BTreeMap::<ReplayLawTuple, String>::new();
    for (idx, law) in tuples.into_iter().enumerate() { out.insert(law, format!("PG{}", idx)); }
    out
}

fn remap_law_path(report: &FileReport, shared_law_ids: &BTreeMap<ReplayLawTuple, String>) -> Vec<String> {
    let local_to_shared = report.laws.iter().map(|law| (law.local_law_id.clone(), shared_law_ids.get(&law.law).cloned().unwrap_or_else(|| "PG?".to_string()))).collect::<BTreeMap<_,_>>();
    report.law_path.iter().map(|local| local_to_shared.get(local).cloned().unwrap_or_else(|| "PG?".to_string())).collect()
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
    }
    out
}

fn put_varint(mut v: u64, out: &mut Vec<u8>) {
    loop {
        let byte = (v & 0x7F) as u8;
        v >>= 7;
        if v == 0 { out.push(byte); break; } else { out.push(byte | 0x80); }
    }
}

fn tokenize_kv_line(line: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for token in line.split_whitespace() { if let Some((k,v)) = token.split_once('=') { out.insert(k.to_string(), v.to_string()); } }
    out
}
fn parse_required_string(map: &BTreeMap<String, String>, key: &str) -> Result<String> { map.get(key).cloned().ok_or_else(|| anyhow!("missing key {}", key)) }
fn parse_required_usize(map: &BTreeMap<String, String>, key: &str) -> Result<usize> { parse_required_string(map,key)?.parse::<usize>().with_context(|| format!("parse usize {}", key)) }
fn parse_required_u64(map: &BTreeMap<String, String>, key: &str) -> Result<u64> { parse_required_string(map,key)?.parse::<u64>().with_context(|| format!("parse u64 {}", key)) }
fn parse_required_f64(map: &BTreeMap<String, String>, key: &str) -> Result<f64> { parse_required_string(map,key)?.parse::<f64>().with_context(|| format!("parse f64 {}", key)) }
fn parse_required_bool(map: &BTreeMap<String, String>, key: &str) -> Result<bool> { parse_required_string(map,key)?.parse::<bool>().with_context(|| format!("parse bool {}", key)) }

fn chunk_search_objective_name(value: ChunkSearchObjective) -> &'static str {
    match value {
        ChunkSearchObjective::Raw => "raw",
        ChunkSearchObjective::RawGuarded => "raw-guarded",
        ChunkSearchObjective::Honest => "honest",
        ChunkSearchObjective::Newline => "newline",
    }
}

fn render_txt(summary: &CompareSummary, reports: &[FileReport], file_views: &[FileSharedView], shared_law_ids: &BTreeMap<ReplayLawTuple, String>) -> String {
    let mut out = String::new();
    macro_rules! line { ($k:expr, $v:expr) => {{ out.push_str($k); out.push('='); out.push_str(&$v.to_string()); out.push('\n'); }}; }
    line!("file_count", summary.file_count);
    line!("honest_file_count", summary.honest_file_count);
    line!("union_law_count", summary.union_law_count);
    line!("shared_law_dictionary_bytes_exact", summary.shared_law_dictionary_bytes_exact);
    line!("separate_total_piecewise_payload_exact", summary.separate_total_piecewise_payload_exact);
    line!("separate_total_law_dictionary_bytes_exact", summary.separate_total_law_dictionary_bytes_exact);
    line!("shared_total_piecewise_payload_exact", summary.shared_total_piecewise_payload_exact);
    line!("shared_dictionary_savings_exact", summary.shared_dictionary_savings_exact);
    out.push_str("\n--- shared_laws ---\n");
    for (law, gid) in shared_law_ids {
        out.push_str(&format!("global_law_id={} boundary_band={} field_margin={}\n", gid, law.boundary_band, law.field_margin));
    }
    out.push_str("\n--- files ---\n");
    for (report, view) in reports.iter().zip(file_views.iter()) {
        out.push_str(&format!(
            "input={} total_piecewise_payload_exact={} law_dictionary_bytes_exact={} local_compact_payload_bytes_exact={} selected_path_mode={} selected_path_bytes_exact={} distinct_law_count={} shared_law_path={} honest_non_overlapping={} overlap_bytes={} coverage_bytes={} windows_analyzed={}\n",
            report.input,
            view.total_piecewise_payload_exact,
            view.law_dictionary_bytes_exact,
            view.local_compact_payload_bytes_exact,
            view.selected_path_mode,
            view.selected_path_bytes_exact,
            view.distinct_law_count,
            view.shared_law_path.join(","),
            report.honest_non_overlapping,
            report.overlap_bytes,
            report.coverage_bytes,
            report.windows_analyzed,
        ));
    }
    out
}

fn render_csv(summary: &CompareSummary, _reports: &[FileReport], file_views: &[FileSharedView], shared_law_ids: &BTreeMap<ReplayLawTuple, String>) -> String {
    let mut out = String::new();
    push_csv_row(&mut out, &[
        "row_kind","id","input","boundary_band","field_margin","file_count","honest_file_count","union_law_count","shared_law_dictionary_bytes_exact","separate_total_piecewise_payload_exact","separate_total_law_dictionary_bytes_exact","shared_total_piecewise_payload_exact","shared_dictionary_savings_exact","total_piecewise_payload_exact","law_dictionary_bytes_exact","local_compact_payload_bytes_exact","selected_path_mode","selected_path_bytes_exact","distinct_law_count","shared_law_path"
    ]);
    push_csv_row(&mut out, &[
        "summary".to_string(),"summary".to_string(),String::new(),String::new(),String::new(),summary.file_count.to_string(),summary.honest_file_count.to_string(),summary.union_law_count.to_string(),summary.shared_law_dictionary_bytes_exact.to_string(),summary.separate_total_piecewise_payload_exact.to_string(),summary.separate_total_law_dictionary_bytes_exact.to_string(),summary.shared_total_piecewise_payload_exact.to_string(),summary.shared_dictionary_savings_exact.to_string(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new()
    ]);
    for (law, gid) in shared_law_ids {
        push_csv_row(&mut out, &[
            "shared_law".to_string(),gid.clone(),String::new(),law.boundary_band.to_string(),law.field_margin.to_string(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new()
        ]);
    }
    for view in file_views {
        push_csv_row(&mut out, &[
            "file".to_string(),String::new(),view.input.clone(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),String::new(),view.total_piecewise_payload_exact.to_string(),view.law_dictionary_bytes_exact.to_string(),view.local_compact_payload_bytes_exact.to_string(),view.selected_path_mode.clone(),view.selected_path_bytes_exact.to_string(),view.distinct_law_count.to_string(),view.shared_law_path.join("|")
        ]);
    }
    out
}

fn push_csv_row(out: &mut String, cells: &[impl AsRef<str>]) {
    let escaped = cells.iter().map(|s| csv_escape(s.as_ref())).collect::<Vec<_>>();
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
