use anyhow::{anyhow, Context, Result};
use std::collections::BTreeMap;
use std::env;
use std::path::Path;
use std::process::Command;

use crate::cmd::apextrace::{ApexLaneLawProfileArgs, ChunkSearchObjective, RenderFormat};

use super::common::write_or_print;

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
struct WindowRow {
    window_idx: usize,
    local_law_id: String,
    chunk_bytes: usize,
    chunk_search_objective: String,
    chunk_raw_slack: u64,
    compact_field_total_payload_exact: usize,
    field_match_pct: f64,
}

#[derive(Clone, Debug)]
struct FileReport {
    input: String,
    recipe: String,
    windows_analyzed: usize,
    honest_non_overlapping: bool,
    total_piecewise_payload_exact: usize,
    law_path: Vec<String>,
    laws: Vec<LawRow>,
    windows: Vec<WindowRow>,
}

#[derive(Clone, Debug)]
struct LawProfile {
    global_law_id: String,
    law: ReplayLawTuple,
    file_count: usize,
    path_hits: usize,
    total_window_count: usize,
    total_segment_count: usize,
    total_covered_bytes: usize,
    weighted_mean_compact_field_total_payload_exact: f64,
    weighted_mean_field_match_pct: f64,
    weighted_mean_field_match_vs_majority_pct: f64,
    weighted_mean_field_balanced_accuracy_pct: f64,
    weighted_mean_field_macro_f1_pct: f64,
    weighted_mean_field_f1_newline_pct: f64,
    mean_window_payload_exact: f64,
    mean_window_match_pct: f64,
    best_window_payload_exact: usize,
    best_window_input: String,
    best_window_idx: usize,
    worst_window_payload_exact: usize,
    worst_window_input: String,
    worst_window_idx: usize,
    dominant_knob_signature: String,
    dominant_knob_count: usize,
}

#[derive(Clone, Debug)]
struct ProfileSummary {
    recipe: String,
    file_count: usize,
    honest_file_count: usize,
    union_law_count: usize,
    total_piecewise_payload_exact: usize,
    total_windows_analyzed: usize,
    dominant_global_law_id: String,
    dominant_global_law_path_hits: usize,
}

pub fn run_apex_lane_law_profile(args: ApexLaneLawProfileArgs) -> Result<()> {
    if args.inputs.is_empty() {
        return Err(anyhow!("apex-lane-law-profile requires at least one --in input"));
    }

    let exe = env::current_exe().context("resolve current executable for apex-lane-law-profile")?;
    let mut reports = Vec::with_capacity(args.inputs.len());
    for input in &args.inputs {
        let output = run_child_apex_lane_manifest(&exe, &args, input)?;
        let report = parse_manifest_txt(&output)
            .with_context(|| format!("parse apex-lane-manifest output for {}", input))?;
        reports.push(report);
    }

    let shared_law_ids = build_shared_law_ids(&reports);
    let profiles = build_profiles(&reports, &shared_law_ids);
    let dominant = profiles
        .iter()
        .max_by_key(|p| p.path_hits)
        .cloned();

    let summary = ProfileSummary {
        recipe: reports
            .first()
            .map(|r| r.recipe.clone())
            .unwrap_or_else(|| args.recipe.clone()),
        file_count: reports.len(),
        honest_file_count: reports.iter().filter(|r| r.honest_non_overlapping).count(),
        union_law_count: profiles.len(),
        total_piecewise_payload_exact: reports
            .iter()
            .map(|r| r.total_piecewise_payload_exact)
            .sum::<usize>(),
        total_windows_analyzed: reports.iter().map(|r| r.windows_analyzed).sum::<usize>(),
        dominant_global_law_id: dominant
            .as_ref()
            .map(|p| p.global_law_id.clone())
            .unwrap_or_else(|| "G?".to_string()),
        dominant_global_law_path_hits: dominant.as_ref().map(|p| p.path_hits).unwrap_or(0),
    };

    let body = match args.format {
        RenderFormat::Txt => render_txt(&summary, &reports, &profiles, &shared_law_ids),
        RenderFormat::Csv => render_csv(&summary, &reports, &profiles),
    };

    write_or_print(args.out.as_deref(), &body)?;

    if let Some(path) = args.out.as_deref() {
        eprintln!(
            "apex-lane-law-profile: out={} files={} union_laws={} dominant={} path_hits={}",
            path,
            summary.file_count,
            summary.union_law_count,
            summary.dominant_global_law_id,
            summary.dominant_global_law_path_hits,
        );
    } else {
        eprintln!(
            "apex-lane-law-profile: files={} union_laws={} dominant={} path_hits={}",
            summary.file_count,
            summary.union_law_count,
            summary.dominant_global_law_id,
            summary.dominant_global_law_path_hits,
        );
    }

    Ok(())
}

fn run_child_apex_lane_manifest(
    exe: &Path,
    args: &ApexLaneLawProfileArgs,
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
            if args.newline_only_from_spacelike { "true" } else { "false" }
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
    if args.keep_temp_dir {
        cmd.arg("--keep-temp-dir");
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
        windows_analyzed: parse_required_usize(&summary, "windows_analyzed")?,
        honest_non_overlapping: parse_required_bool(&summary, "honest_non_overlapping")?,
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
        windows,
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

fn parse_window_row(line: &str) -> Result<WindowRow> {
    let tokens = tokenize_kv_line(line);
    Ok(WindowRow {
        window_idx: parse_required_usize(&tokens, "window_idx")?,
        local_law_id: parse_required_string(&tokens, "law_id")?,
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

fn build_profiles(
    reports: &[FileReport],
    shared_law_ids: &BTreeMap<ReplayLawTuple, String>,
) -> Vec<LawProfile> {
    let mut per_law = Vec::<LawProfile>::new();

    for (law_tuple, global_id) in shared_law_ids {
        let mut file_count = 0usize;
        let mut path_hits = 0usize;
        let mut total_window_count = 0usize;
        let mut total_segment_count = 0usize;
        let mut total_covered_bytes = 0usize;
        let mut weighted_payload_sum = 0.0f64;
        let mut weighted_match_sum = 0.0f64;
        let mut weighted_match_vs_majority_sum = 0.0f64;
        let mut weighted_balanced_sum = 0.0f64;
        let mut weighted_macro_f1_sum = 0.0f64;
        let mut weighted_f1_newline_sum = 0.0f64;
        let mut weight_total = 0usize;
        let mut window_payload_sum = 0usize;
        let mut window_match_sum = 0.0f64;
        let mut window_seen = 0usize;
        let mut best_window_payload_exact = usize::MAX;
        let mut best_window_input = String::new();
        let mut best_window_idx = 0usize;
        let mut worst_window_payload_exact = 0usize;
        let mut worst_window_input = String::new();
        let mut worst_window_idx = 0usize;
        let mut knob_counts = BTreeMap::<String, usize>::new();

        for report in reports {
            let matching_laws = report
                .laws
                .iter()
                .filter(|law| &law.law == law_tuple)
                .collect::<Vec<_>>();
            if !matching_laws.is_empty() {
                file_count += 1;
            }
            for law in matching_laws {
                total_window_count += law.window_count;
                total_segment_count += law.segment_count;
                total_covered_bytes += law.covered_bytes;
                weighted_payload_sum += law.mean_compact_field_total_payload_exact * law.window_count as f64;
                weighted_match_sum += law.mean_field_match_pct * law.window_count as f64;
                weighted_match_vs_majority_sum +=
                    law.mean_field_match_vs_majority_pct * law.window_count as f64;
                weighted_balanced_sum +=
                    law.mean_field_balanced_accuracy_pct * law.window_count as f64;
                weighted_macro_f1_sum += law.mean_field_macro_f1_pct * law.window_count as f64;
                weighted_f1_newline_sum +=
                    law.mean_field_f1_newline_pct * law.window_count as f64;
                weight_total += law.window_count;
            }

            let local_to_global = report
                .laws
                .iter()
                .map(|law| {
                    let mapped = shared_law_ids
                        .get(&law.law)
                        .cloned()
                        .unwrap_or_else(|| "G?".to_string());
                    (law.local_law_id.clone(), mapped)
                })
                .collect::<BTreeMap<_, _>>();

            path_hits += report
                .law_path
                .iter()
                .filter(|local_id| {
                    local_to_global
                        .get(*local_id)
                        .map(|g| g == global_id)
                        .unwrap_or(false)
                })
                .count();

            for window in &report.windows {
                if let Some(mapped) = local_to_global.get(&window.local_law_id) {
                    if mapped == global_id {
                        window_seen += 1;
                        window_payload_sum += window.compact_field_total_payload_exact;
                        window_match_sum += window.field_match_pct;
                        let sig = format!(
                            "chunk_bytes={} chunk_search_objective={} chunk_raw_slack={}",
                            window.chunk_bytes, window.chunk_search_objective, window.chunk_raw_slack
                        );
                        *knob_counts.entry(sig).or_default() += 1;

                        if window.compact_field_total_payload_exact < best_window_payload_exact {
                            best_window_payload_exact = window.compact_field_total_payload_exact;
                            best_window_input = report.input.clone();
                            best_window_idx = window.window_idx;
                        }
                        if window.compact_field_total_payload_exact > worst_window_payload_exact {
                            worst_window_payload_exact = window.compact_field_total_payload_exact;
                            worst_window_input = report.input.clone();
                            worst_window_idx = window.window_idx;
                        }
                    }
                }
            }
        }

        let (dominant_knob_signature, dominant_knob_count) = knob_counts
            .into_iter()
            .max_by_key(|(_, count)| *count)
            .unwrap_or_else(|| ("unknown".to_string(), 0));

        per_law.push(LawProfile {
            global_law_id: global_id.clone(),
            law: law_tuple.clone(),
            file_count,
            path_hits,
            total_window_count,
            total_segment_count,
            total_covered_bytes,
            weighted_mean_compact_field_total_payload_exact: if weight_total == 0 {
                0.0
            } else {
                weighted_payload_sum / weight_total as f64
            },
            weighted_mean_field_match_pct: if weight_total == 0 {
                0.0
            } else {
                weighted_match_sum / weight_total as f64
            },
            weighted_mean_field_match_vs_majority_pct: if weight_total == 0 {
                0.0
            } else {
                weighted_match_vs_majority_sum / weight_total as f64
            },
            weighted_mean_field_balanced_accuracy_pct: if weight_total == 0 {
                0.0
            } else {
                weighted_balanced_sum / weight_total as f64
            },
            weighted_mean_field_macro_f1_pct: if weight_total == 0 {
                0.0
            } else {
                weighted_macro_f1_sum / weight_total as f64
            },
            weighted_mean_field_f1_newline_pct: if weight_total == 0 {
                0.0
            } else {
                weighted_f1_newline_sum / weight_total as f64
            },
            mean_window_payload_exact: if window_seen == 0 {
                0.0
            } else {
                window_payload_sum as f64 / window_seen as f64
            },
            mean_window_match_pct: if window_seen == 0 {
                0.0
            } else {
                window_match_sum / window_seen as f64
            },
            best_window_payload_exact: if best_window_payload_exact == usize::MAX {
                0
            } else {
                best_window_payload_exact
            },
            best_window_input,
            best_window_idx,
            worst_window_payload_exact,
            worst_window_input,
            worst_window_idx,
            dominant_knob_signature,
            dominant_knob_count,
        });
    }

    per_law
}

fn render_txt(
    summary: &ProfileSummary,
    reports: &[FileReport],
    profiles: &[LawProfile],
    shared_law_ids: &BTreeMap<ReplayLawTuple, String>,
) -> String {
    let mut out = String::new();
    push_line(&mut out, "recipe", &summary.recipe);
    push_line(&mut out, "file_count", summary.file_count);
    push_line(&mut out, "honest_file_count", summary.honest_file_count);
    push_line(&mut out, "union_law_count", summary.union_law_count);
    push_line(
        &mut out,
        "total_piecewise_payload_exact",
        summary.total_piecewise_payload_exact,
    );
    push_line(&mut out, "total_windows_analyzed", summary.total_windows_analyzed);
    push_line(&mut out, "dominant_global_law_id", &summary.dominant_global_law_id);
    push_line(
        &mut out,
        "dominant_global_law_path_hits",
        summary.dominant_global_law_path_hits,
    );

    out.push_str("\n--- shared-laws ---\n");
    for (law, global_id) in shared_law_ids {
        out.push_str(&format!(
            "global_law_id={} boundary_band={} field_margin={} newline_demote_margin={}\n",
            global_id, law.boundary_band, law.field_margin, law.newline_demote_margin,
        ));
    }

    out.push_str("\n--- law-profiles ---\n");
    for profile in profiles {
        out.push_str(&format!(
            "global_law_id={} boundary_band={} field_margin={} newline_demote_margin={} file_count={} path_hits={} total_window_count={} total_segment_count={} total_covered_bytes={} weighted_mean_compact_field_total_payload_exact={:.6} weighted_mean_field_match_pct={:.6} weighted_mean_field_match_vs_majority_pct={:.6} weighted_mean_field_balanced_accuracy_pct={:.6} weighted_mean_field_macro_f1_pct={:.6} weighted_mean_field_f1_newline_pct={:.6} mean_window_payload_exact={:.6} mean_window_match_pct={:.6} best_window_payload_exact={} best_window_input={} best_window_idx={} worst_window_payload_exact={} worst_window_input={} worst_window_idx={} dominant_knob_signature={} dominant_knob_count={}\n",
            profile.global_law_id,
            profile.law.boundary_band,
            profile.law.field_margin,
            profile.law.newline_demote_margin,
            profile.file_count,
            profile.path_hits,
            profile.total_window_count,
            profile.total_segment_count,
            profile.total_covered_bytes,
            profile.weighted_mean_compact_field_total_payload_exact,
            profile.weighted_mean_field_match_pct,
            profile.weighted_mean_field_match_vs_majority_pct,
            profile.weighted_mean_field_balanced_accuracy_pct,
            profile.weighted_mean_field_macro_f1_pct,
            profile.weighted_mean_field_f1_newline_pct,
            profile.mean_window_payload_exact,
            profile.mean_window_match_pct,
            profile.best_window_payload_exact,
            profile.best_window_input,
            profile.best_window_idx,
            profile.worst_window_payload_exact,
            profile.worst_window_input,
            profile.worst_window_idx,
            profile.dominant_knob_signature.replace(' ', "|"),
            profile.dominant_knob_count,
        ));
    }

    out.push_str("\n--- files ---\n");
    for report in reports {
        out.push_str(&format!(
            "input={} honest_non_overlapping={} windows_analyzed={} total_piecewise_payload_exact={} law_path={}\n",
            report.input,
            report.honest_non_overlapping,
            report.windows_analyzed,
            report.total_piecewise_payload_exact,
            report.law_path.join(","),
        ));
    }

    out
}

fn render_csv(summary: &ProfileSummary, reports: &[FileReport], profiles: &[LawProfile]) -> String {
    let mut out = String::new();
    push_csv_row(
        &mut out,
        &[
            "row_kind",
            "id",
            "input",
            "boundary_band",
            "field_margin",
            "newline_demote_margin",
            "file_count",
            "path_hits",
            "total_window_count",
            "total_segment_count",
            "total_covered_bytes",
            "weighted_mean_compact_field_total_payload_exact",
            "weighted_mean_field_match_pct",
            "weighted_mean_field_match_vs_majority_pct",
            "weighted_mean_field_balanced_accuracy_pct",
            "weighted_mean_field_macro_f1_pct",
            "weighted_mean_field_f1_newline_pct",
            "mean_window_payload_exact",
            "mean_window_match_pct",
            "best_window_payload_exact",
            "best_window_input",
            "best_window_idx",
            "worst_window_payload_exact",
            "worst_window_input",
            "worst_window_idx",
            "dominant_knob_signature",
            "dominant_knob_count",
            "total_piecewise_payload_exact",
            "law_path",
        ],
    );

    push_csv_row(
        &mut out,
        &[
            "summary",
            "summary",
            "",
            "",
            "",
            "",
            &summary.file_count.to_string(),
            &summary.dominant_global_law_path_hits.to_string(),
            &summary.total_windows_analyzed.to_string(),
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
            "",
            "",
            "",
            "",
            "",
            &summary.dominant_global_law_id,
            "",
            &summary.total_piecewise_payload_exact.to_string(),
            "",
        ],
    );

    for profile in profiles {
        push_csv_row(
            &mut out,
            &[
                "law-profile",
                &profile.global_law_id,
                "",
                &profile.law.boundary_band.to_string(),
                &profile.law.field_margin.to_string(),
                &profile.law.newline_demote_margin.to_string(),
                &profile.file_count.to_string(),
                &profile.path_hits.to_string(),
                &profile.total_window_count.to_string(),
                &profile.total_segment_count.to_string(),
                &profile.total_covered_bytes.to_string(),
                &format!("{:.6}", profile.weighted_mean_compact_field_total_payload_exact),
                &format!("{:.6}", profile.weighted_mean_field_match_pct),
                &format!("{:.6}", profile.weighted_mean_field_match_vs_majority_pct),
                &format!("{:.6}", profile.weighted_mean_field_balanced_accuracy_pct),
                &format!("{:.6}", profile.weighted_mean_field_macro_f1_pct),
                &format!("{:.6}", profile.weighted_mean_field_f1_newline_pct),
                &format!("{:.6}", profile.mean_window_payload_exact),
                &format!("{:.6}", profile.mean_window_match_pct),
                &profile.best_window_payload_exact.to_string(),
                &profile.best_window_input,
                &profile.best_window_idx.to_string(),
                &profile.worst_window_payload_exact.to_string(),
                &profile.worst_window_input,
                &profile.worst_window_idx.to_string(),
                &profile.dominant_knob_signature,
                &profile.dominant_knob_count.to_string(),
                "",
                "",
            ],
        );
    }

    for report in reports {
        push_csv_row(
            &mut out,
            &[
                "file",
                &report.input,
                &report.input,
                "",
                "",
                "",
                "",
                "",
                &report.windows_analyzed.to_string(),
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
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                &report.total_piecewise_payload_exact.to_string(),
                &report.law_path.join(","),
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
    use super::{build_profiles, build_shared_law_ids, parse_manifest_txt, ReplayLawTuple};

    fn sample_manifest(input: &str, law_path: &str, laws: &[(&str, usize, u64, u64)]) -> String {
        let mut out = String::new();
        out.push_str(&format!("input={}\n", input));
        out.push_str("recipe=configs/tuned_validated.k8r\n");
        out.push_str("input_bytes=3072\n");
        out.push_str("window_bytes=256\n");
        out.push_str("step_bytes=256\n");
        out.push_str("windows_analyzed=3\n");
        out.push_str("total_window_span_bytes=768\n");
        out.push_str("coverage_bytes=768\n");
        out.push_str("overlap_bytes=0\n");
        out.push_str("honest_non_overlapping=true\n");
        out.push_str("allow_overlap_scout=false\n");
        out.push_str(&format!("distinct_law_count={}\n", laws.len()));
        out.push_str("segment_count=2\n");
        out.push_str("law_switch_count=1\n");
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
        out.push_str("local_compact_payload_bytes_exact=530\n");
        out.push_str("shared_header_bytes_exact=24\n");
        out.push_str("law_dictionary_bytes_exact=13\n");
        out.push_str("window_path_bytes_exact=18\n");
        out.push_str("segment_path_bytes_exact=14\n");
        out.push_str("selected_path_mode=segment\n");
        out.push_str("selected_path_bytes_exact=14\n");
        out.push_str("total_piecewise_payload_exact=567\n");
        out.push_str(&format!("law_path={}\n", law_path));
        out.push_str("\n--- laws ---\n");
        for (id, band, margin, demote) in laws {
            out.push_str(&format!(
                "law_id={} boundary_band={} field_margin={} newline_demote_margin={} window_count=2 segment_count=1 covered_bytes=512 mean_compact_field_total_payload_exact=176.500 mean_field_match_pct=70.000000 mean_field_match_vs_majority_pct=-20.000000 mean_field_balanced_accuracy_pct=50.000000 mean_field_macro_f1_pct=45.000000 mean_field_f1_newline_pct=10.000000\n",
                id, band, margin, demote,
            ));
        }
        out.push_str("\n--- segments ---\nsegment_idx=0 law_id=L0 start=0 end=512 span_bytes=512 window_count=2 first_window_idx=0 last_window_idx=1 mean_compact_field_total_payload_exact=176.500 mean_field_match_pct=70.000000 mean_field_balanced_accuracy_pct=50.000000 mean_field_macro_f1_pct=45.000000 mean_field_f1_newline_pct=10.000000\n");
        out.push_str("\n--- windows ---\n");
        out.push_str("window_idx=0 law_id=L0 start=0 end=256 span_bytes=256 chunk_bytes=64 boundary_band=12 field_margin=4 newline_demote_margin=4 chunk_search_objective=raw chunk_raw_slack=1 compact_field_total_payload_exact=177 field_patch_bytes=90 field_match_pct=70.312500 majority_baseline_match_pct=89.000000 field_match_vs_majority_pct=-18.687500 field_balanced_accuracy_pct=50.000000 field_macro_f1_pct=45.000000 field_f1_newline_pct=10.000000 field_pred_dominant_label=space field_pred_dominant_share_pct=90.000000 field_pred_collapse_90_flag=true field_pred_newline_delta=-3 field_newline_demoted=1 field_newline_after_demote=5 field_newline_floor_used=1 field_newline_extinct_flag=false\n");
        out.push_str("window_idx=1 law_id=L0 start=256 end=512 span_bytes=256 chunk_bytes=64 boundary_band=12 field_margin=4 newline_demote_margin=4 chunk_search_objective=raw chunk_raw_slack=1 compact_field_total_payload_exact=176 field_patch_bytes=88 field_match_pct=71.000000 majority_baseline_match_pct=89.000000 field_match_vs_majority_pct=-18.000000 field_balanced_accuracy_pct=50.000000 field_macro_f1_pct=45.000000 field_f1_newline_pct=10.000000 field_pred_dominant_label=space field_pred_dominant_share_pct=90.000000 field_pred_collapse_90_flag=true field_pred_newline_delta=-3 field_newline_demoted=1 field_newline_after_demote=5 field_newline_floor_used=1 field_newline_extinct_flag=false\n");
        out.push_str("window_idx=2 law_id=L1 start=512 end=768 span_bytes=256 chunk_bytes=32 boundary_band=8 field_margin=4 newline_demote_margin=4 chunk_search_objective=raw chunk_raw_slack=1 compact_field_total_payload_exact=180 field_patch_bytes=96 field_match_pct=68.000000 majority_baseline_match_pct=89.000000 field_match_vs_majority_pct=-21.000000 field_balanced_accuracy_pct=50.000000 field_macro_f1_pct=45.000000 field_f1_newline_pct=10.000000 field_pred_dominant_label=space field_pred_dominant_share_pct=90.000000 field_pred_collapse_90_flag=true field_pred_newline_delta=-3 field_newline_demoted=1 field_newline_after_demote=5 field_newline_floor_used=1 field_newline_extinct_flag=false\n");
        out
    }

    #[test]
    fn parse_manifest_extracts_windows() {
        let raw = sample_manifest(
            "text/Genesis1.txt",
            "L0,L0,L1",
            &[("L0", 12, 4, 4), ("L1", 8, 4, 4)],
        );
        let report = parse_manifest_txt(&raw).expect("parse manifest txt");
        assert_eq!(report.windows_analyzed, 3);
        assert_eq!(report.windows.len(), 3);
        assert_eq!(report.windows[0].chunk_bytes, 64);
        assert_eq!(report.windows[2].local_law_id, "L1");
    }

    #[test]
    fn build_profiles_marks_dominant_knob() {
        let a = parse_manifest_txt(&sample_manifest(
            "text/Genesis1.txt",
            "L0,L0,L1",
            &[("L0", 12, 4, 4), ("L1", 8, 4, 4)],
        ))
        .expect("parse A");
        let b = parse_manifest_txt(&sample_manifest(
            "text/Genesis2.txt",
            "L0,L0,L0",
            &[("L0", 12, 4, 4)],
        ))
        .expect("parse B");
        let ids = build_shared_law_ids(&[a.clone(), b.clone()]);
        let profiles = build_profiles(&[a, b], &ids);
        let g1 = profiles
            .iter()
            .find(|p| p.law == ReplayLawTuple { boundary_band: 12, field_margin: 4, newline_demote_margin: 4 })
            .expect("find dominant law");
        assert_eq!(g1.file_count, 2);
        assert!(g1.path_hits >= 4);
        assert_eq!(g1.dominant_knob_signature, "chunk_bytes=64 chunk_search_objective=raw chunk_raw_slack=1");
    }
}
