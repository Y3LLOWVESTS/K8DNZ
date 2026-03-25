use anyhow::{anyhow, Context, Result};
use std::collections::BTreeMap;

use crate::cmd::apextrace::{ApexLaneLawLocalMixFreezeArgs, ChunkSearchObjective};

use super::types::{EvalConfig, FileReport, LawProfile, ReplayLawTuple, SearchKnobTuple};

pub(crate) fn select_target_profile<'a>(
    profiles: &'a [LawProfile],
    global_law_id: Option<&str>,
) -> Result<&'a LawProfile> {
    if profiles.is_empty() {
        return Err(anyhow!(
            "apex-lane-law-local-mix-freeze found no shared laws to evaluate"
        ));
    }
    if let Some(id) = global_law_id {
        return profiles
            .iter()
            .find(|profile| profile.global_law_id == id)
            .ok_or_else(|| anyhow!("requested --global-law-id {} was not present", id));
    }
    profiles
        .iter()
        .max_by_key(|profile| profile.path_hits)
        .ok_or_else(|| anyhow!("apex-lane-law-local-mix-freeze could not select dominant law"))
}

pub(crate) fn build_eval_config(
    args: &ApexLaneLawLocalMixFreezeArgs,
    target: &LawProfile,
    dominant: &SearchKnobTuple,
) -> Result<EvalConfig> {
    let search = SearchKnobTuple {
        chunk_bytes: dominant.chunk_bytes,
        chunk_search_objective: args
            .local_chunk_search_objective
            .map(chunk_search_objective_name)
            .unwrap_or(dominant.chunk_search_objective.as_str())
            .to_string(),
        chunk_raw_slack: args.local_chunk_raw_slack.unwrap_or(dominant.chunk_raw_slack),
    };
    let law = ReplayLawTuple {
        boundary_band: args.freeze_boundary_band.unwrap_or(target.law.boundary_band),
        field_margin: args.freeze_field_margin.unwrap_or(target.law.field_margin),
        newline_demote_margin: args
            .freeze_newline_demote_margin
            .unwrap_or(target.law.newline_demote_margin),
    };
    Ok(EvalConfig { law, search })
}

pub(crate) fn parse_usize_list(raw: &str) -> Result<Vec<usize>> {
    let mut out = raw
        .split(',')
        .filter_map(|part| {
            let t = part.trim();
            if t.is_empty() {
                None
            } else {
                Some(t)
            }
        })
        .map(|part| {
            part.parse::<usize>()
                .with_context(|| format!("parse usize from {}", part))
        })
        .collect::<Result<Vec<_>>>()?;
    out.sort_unstable();
    out.dedup();
    if out.is_empty() {
        return Err(anyhow!("empty usize list"));
    }
    Ok(out)
}

pub(crate) fn select_chunk_candidates(
    args: &ApexLaneLawLocalMixFreezeArgs,
    reports: &[FileReport],
    shared_law_ids: &BTreeMap<ReplayLawTuple, String>,
    target: &LawProfile,
    dominant_chunk_bytes: usize,
) -> Result<Vec<usize>> {
    let mut out = parse_usize_list(&args.local_chunk_sweep)?;
    out.push(dominant_chunk_bytes);
    for report in reports {
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
        for window in &report.windows {
            if local_to_global
                .get(&window.local_law_id)
                .map(|id| id == &target.global_law_id)
                .unwrap_or(false)
            {
                out.push(window.chunk_bytes);
            }
        }
    }
    out.sort_unstable();
    out.dedup();
    Ok(out)
}

pub(crate) fn chunk_search_objective_name(value: ChunkSearchObjective) -> &'static str {
    match value {
        ChunkSearchObjective::Raw => "raw",
        ChunkSearchObjective::RawGuarded => "raw-guarded",
        ChunkSearchObjective::Honest => "honest",
        ChunkSearchObjective::Newline => "newline",
    }
}
