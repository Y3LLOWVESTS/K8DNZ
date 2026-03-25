use anyhow::{anyhow, Context, Result};
use std::collections::BTreeMap;

use super::types::{
    FileReport, FrozenEvalRow, LawRow, ManifestWindowRow, ReplayLawTuple, SearchKnobTuple,
};

pub(crate) fn parse_manifest_txt(raw: &str) -> Result<FileReport> {
    let mut summary = BTreeMap::<String, String>::new();
    let mut section = "summary";
    let mut laws = Vec::new();
    let mut windows = Vec::new();

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
            "windows" => windows.push(parse_window_row(line)?),
            _ => {}
        }
    }

    Ok(FileReport {
        input: parse_required_string(&summary, "input")?,
        recipe: parse_required_string(&summary, "recipe")?,
        input_bytes: parse_required_usize(&summary, "input_bytes")?,
        windows_analyzed: parse_required_usize(&summary, "windows_analyzed")?,
        honest_non_overlapping: parse_required_bool(&summary, "honest_non_overlapping")?,
        shared_header_bytes_exact: parse_required_usize(&summary, "shared_header_bytes_exact")?,
        total_piecewise_payload_exact: parse_required_usize(&summary, "total_piecewise_payload_exact")?,
        law_path: parse_required_string(&summary, "law_path")?
            .split(',')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect(),
        laws,
        windows,
    })
}

pub(crate) fn parse_law_row(line: &str) -> Result<LawRow> {
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

pub(crate) fn parse_window_row(line: &str) -> Result<ManifestWindowRow> {
    let tokens = tokenize_kv_line(line);
    Ok(ManifestWindowRow {
        window_idx: parse_required_usize(&tokens, "window_idx")?,
        local_law_id: parse_required_string(&tokens, "law_id")?,
        start: parse_required_usize(&tokens, "start")?,
        end: parse_required_usize(&tokens, "end")?,
        span_bytes: parse_required_usize(&tokens, "span_bytes")?,
        chunk_bytes: parse_required_usize(&tokens, "chunk_bytes")?,
        chunk_search_objective: parse_required_string(&tokens, "chunk_search_objective")?,
        chunk_raw_slack: parse_required_u64(&tokens, "chunk_raw_slack")?,
        compact_field_total_payload_exact: parse_required_usize(
            &tokens,
            "compact_field_total_payload_exact",
        )?,
        field_match_pct: parse_required_f64(&tokens, "field_match_pct")?,
    })
}

pub(crate) fn tokenize_kv_line(line: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for token in line.split_whitespace() {
        if let Some((k, v)) = token.split_once('=') {
            out.insert(k.to_string(), v.to_string());
        }
    }
    out
}

pub(crate) fn parse_best_line(stderr: &[u8]) -> Result<FrozenEvalRow> {
    let stderr = String::from_utf8_lossy(stderr);
    let prefix = "apextrace apex-map-lane best:";
    let line = stderr
        .lines()
        .rev()
        .find(|line| line.trim_start().starts_with(prefix))
        .ok_or_else(|| anyhow!("missing best line in apex-map-lane stderr"))?;
    let payload = line
        .trim_start()
        .strip_prefix(prefix)
        .ok_or_else(|| anyhow!("apex-map-lane best line missing prefix"))?
        .trim();

    let map = tokenize_kv_line(payload);
    let search = SearchKnobTuple {
        chunk_bytes: parse_required_usize(&map, "chunk_bytes")?,
        chunk_search_objective: parse_required_string(&map, "chunk_search_objective")?,
        chunk_raw_slack: parse_required_u64(&map, "chunk_raw_slack")?,
    };
    let law = ReplayLawTuple {
        boundary_band: parse_required_usize(&map, "boundary_band")?,
        field_margin: parse_required_u64(&map, "field_margin")?,
        newline_demote_margin: parse_required_u64(&map, "newline_demote_margin")?,
    };

    Ok(FrozenEvalRow {
        law,
        search,
        compact_field_total_payload_exact: parse_required_usize(
            &map,
            "compact_field_total_payload_exact",
        )?,
        field_patch_bytes: parse_required_usize(&map, "field_patch_bytes")?,
        field_match_pct: parse_required_f64(&map, "field_match_pct")?,
        field_match_vs_majority_pct: parse_required_f64(&map, "field_match_vs_majority_pct")?,
        field_balanced_accuracy_pct: parse_required_f64(&map, "field_balanced_accuracy_pct")?,
        field_macro_f1_pct: parse_required_f64(&map, "field_macro_f1_pct")?,
        field_f1_newline_pct: parse_required_f64(&map, "field_f1_newline_pct")?,
        field_pred_dominant_label: parse_required_string(&map, "field_pred_dominant_label")?,
        field_pred_dominant_share_pct: parse_required_f64(
            &map,
            "field_pred_dominant_share_pct",
        )?,
        field_pred_collapse_90_flag: parse_required_bool(&map, "field_pred_collapse_90_flag")?,
        field_pred_newline_delta: parse_required_i64(&map, "field_pred_newline_delta")?,
        field_newline_demoted: parse_required_usize(&map, "field_newline_demoted")?,
        field_newline_after_demote: parse_required_usize(&map, "field_newline_after_demote")?,
        field_newline_floor_used: parse_required_usize(&map, "field_newline_floor_used")?,
        field_newline_extinct_flag: parse_required_bool(&map, "field_newline_extinct_flag")?,
    })
}

pub(crate) fn parse_required_string(map: &BTreeMap<String, String>, key: &str) -> Result<String> {
    map.get(key)
        .cloned()
        .ok_or_else(|| anyhow!("missing key {}", key))
}

pub(crate) fn parse_required_usize(map: &BTreeMap<String, String>, key: &str) -> Result<usize> {
    let raw = parse_required_string(map, key)?;
    raw.parse::<usize>()
        .with_context(|| format!("parse usize key {} from {}", key, raw))
}

pub(crate) fn parse_required_u64(map: &BTreeMap<String, String>, key: &str) -> Result<u64> {
    let raw = parse_required_string(map, key)?;
    raw.parse::<u64>()
        .with_context(|| format!("parse u64 key {} from {}", key, raw))
}

pub(crate) fn parse_required_i64(map: &BTreeMap<String, String>, key: &str) -> Result<i64> {
    let raw = parse_required_string(map, key)?;
    raw.parse::<i64>()
        .with_context(|| format!("parse i64 key {} from {}", key, raw))
}

pub(crate) fn parse_required_f64(map: &BTreeMap<String, String>, key: &str) -> Result<f64> {
    let raw = parse_required_string(map, key)?;
    raw.parse::<f64>()
        .with_context(|| format!("parse f64 key {} from {}", key, raw))
}

pub(crate) fn parse_required_bool(map: &BTreeMap<String, String>, key: &str) -> Result<bool> {
    let raw = parse_required_string(map, key)?;
    raw.parse::<bool>()
        .with_context(|| format!("parse bool key {} from {}", key, raw))
}
