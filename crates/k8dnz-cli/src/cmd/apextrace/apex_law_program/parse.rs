use anyhow::{anyhow, bail, Context, Result};
use std::collections::BTreeMap;

use super::types::{FrozenEvalRow, ManifestWindowPos, ParsedCsvSections};

pub(crate) fn parse_manifest_positions(stdout: &[u8]) -> Result<BTreeMap<usize, ManifestWindowPos>> {
    let body = String::from_utf8_lossy(stdout);
    let mut out = BTreeMap::new();

    for line in body.lines() {
        let line = line.trim();
        if !line.starts_with("window_idx=") {
            continue;
        }
        let map = tokenize_kv_line(line);
        let window_idx = parse_required_usize(&map, "window_idx")?;
        let pos = ManifestWindowPos {
            start: parse_required_usize(&map, "start")?,
            end: parse_required_usize(&map, "end")?,
            span_bytes: parse_required_usize(&map, "span_bytes")?,
        };
        out.insert(window_idx, pos);
    }

    Ok(out)
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
    Ok(FrozenEvalRow {
        compact_field_total_payload_exact: parse_required_usize(
            &map,
            "compact_field_total_payload_exact",
        )?,
        field_match_pct: parse_required_f64(&map, "field_match_pct")?,
        field_pred_collapse_90_flag: parse_required_bool(&map, "field_pred_collapse_90_flag")?,
        field_newline_extinct_flag: parse_required_bool(&map, "field_newline_extinct_flag")?,
        field_newline_floor_used: parse_required_usize(&map, "field_newline_floor_used")?,
    })
}

pub(crate) fn parse_txt_summary(stdout: &[u8]) -> Result<BTreeMap<String, String>> {
    let mut map = BTreeMap::new();
    let body = String::from_utf8_lossy(stdout);

    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if line.starts_with("---") {
            break;
        }
        if let Some((k, v)) = line.split_once('=') {
            map.insert(k.trim().to_string(), v.trim().to_string());
        }
    }

    Ok(map)
}

pub(crate) fn summary_row_map(sections: &ParsedCsvSections) -> Result<BTreeMap<String, String>> {
    if sections.summary_rows.len() != 1 {
        bail!(
            "expected exactly one summary row in local mix csv, got {}",
            sections.summary_rows.len()
        );
    }

    Ok(sections.summary_rows[0].clone())
}

pub(crate) fn parse_csv_sections(stdout: &[u8]) -> Result<ParsedCsvSections> {
    let body = String::from_utf8_lossy(stdout);
    let mut header = Vec::<String>::new();
    let mut sections = ParsedCsvSections {
        summary_rows: Vec::new(),
        file_rows: Vec::new(),
        window_rows: Vec::new(),
        override_selected_rows: Vec::new(),
    };

    for raw_line in body.lines() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        let cells = parse_csv_line(line)?;
        if cells.is_empty() {
            continue;
        }
        if cells[0] == "section" {
            header = cells;
            continue;
        }
        if header.is_empty() {
            bail!("csv row seen before header: {}", line);
        }
        if header.len() != cells.len() {
            bail!(
                "csv row/header length mismatch header_len={} row_len={} line={}",
                header.len(),
                cells.len(),
                line
            );
        }

        let row = header
            .iter()
            .cloned()
            .zip(cells.into_iter())
            .collect::<BTreeMap<_, _>>();

        match row.get("section").map(String::as_str) {
            Some("summary") => sections.summary_rows.push(row),
            Some("file") => sections.file_rows.push(row),
            Some("window") => sections.window_rows.push(row),
            Some("override_selected") => sections.override_selected_rows.push(row),
            _ => {}
        }
    }

    Ok(sections)
}

pub(crate) fn parse_csv_line(line: &str) -> Result<Vec<String>> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut chars = line.chars().peekable();
    let mut in_quotes = false;

    while let Some(ch) = chars.next() {
        match ch {
            '"' => {
                if in_quotes {
                    if matches!(chars.peek(), Some('"')) {
                        cur.push('"');
                        chars.next();
                    } else {
                        in_quotes = false;
                    }
                } else if cur.is_empty() {
                    in_quotes = true;
                } else {
                    cur.push(ch);
                }
            }
            ',' if !in_quotes => {
                out.push(cur);
                cur = String::new();
            }
            _ => cur.push(ch),
        }
    }

    if in_quotes {
        bail!("unterminated quoted csv line: {}", line);
    }

    out.push(cur);
    Ok(out)
}

pub(crate) fn parse_csv_usize_list(source: &str) -> Result<Vec<usize>> {
    let mut out = Vec::new();
    for raw in source.split(',') {
        let token = raw.trim();
        if token.is_empty() {
            continue;
        }
        let value = token
            .parse::<usize>()
            .with_context(|| format!("parse usize list element from {}", token))?;
        out.push(value);
    }
    if out.is_empty() {
        bail!("usize csv list resolved to empty set from {}", source);
    }
    Ok(out)
}

pub(crate) fn tokenize_kv_line(payload: &str) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    for token in payload.split_whitespace() {
        if let Some((k, v)) = token.split_once('=') {
            map.insert(k.to_string(), v.to_string());
        }
    }
    map
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
    match raw.as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => bail!("parse bool key {} from {}", key, raw),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn csv_line_roundtrip_quotes() {
        let row = parse_csv_line("summary,\"a,b\",42").expect("parse csv");
        assert_eq!(row, vec!["summary", "a,b", "42"]);
    }

    #[test]
    fn txt_summary_stops_at_sections() {
        let raw = b"recipe=configs/tuned_validated.k8r\nfile_count=2\n\n--- files ---\ninput=x\n";
        let map = parse_txt_summary(raw).expect("parse txt summary");
        assert_eq!(map.get("recipe").unwrap(), "configs/tuned_validated.k8r");
        assert_eq!(map.get("file_count").unwrap(), "2");
        assert!(map.get("input").is_none());
    }

    #[test]
    fn summary_row_map_requires_exactly_one_row() {
        let sections = ParsedCsvSections {
            summary_rows: vec![BTreeMap::new(), BTreeMap::new()],
            file_rows: Vec::new(),
            window_rows: Vec::new(),
            override_selected_rows: Vec::new(),
        };
        assert!(summary_row_map(&sections).is_err());
    }
}
