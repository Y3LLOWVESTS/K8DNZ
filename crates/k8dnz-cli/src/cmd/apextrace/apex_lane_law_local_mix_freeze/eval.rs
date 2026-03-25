use anyhow::{anyhow, Context, Result};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use crate::cmd::apextrace::ApexLaneLawLocalMixFreezeArgs;

use super::children::run_child_frozen_apex_map_lane;
use super::types::{
    EvalConfig, FileReport, FrozenEvalRow, ManifestWindowRow,
};
use super::util::sanitize_file_stem;

pub(crate) fn eval_window(
    exe: &Path,
    args: &ApexLaneLawLocalMixFreezeArgs,
    eval_config: &EvalConfig,
    input_name: &str,
    input_bytes: &[u8],
    window: &ManifestWindowRow,
    chunk_bytes: usize,
    temp_dir: &Path,
    cache: &mut BTreeMap<(String, usize, usize), FrozenEvalRow>,
) -> Result<FrozenEvalRow> {
    let key = (input_name.to_string(), window.window_idx, chunk_bytes);
    if let Some(row) = cache.get(&key) {
        return Ok(row.clone());
    }

    let slice = &input_bytes[window.start..window.end];
    let window_path = temp_dir.join(format!(
        "local_mix_{}_window_{:04}_{:08}_{:08}_chunk_{}.bin",
        sanitize_file_stem(input_name),
        window.window_idx,
        window.start,
        window.end,
        chunk_bytes
    ));
    fs::write(&window_path, slice)
        .with_context(|| format!("write local mix slice {}", window_path.display()))?;

    let mut per_chunk = eval_config.clone();
    per_chunk.search.chunk_bytes = chunk_bytes;
    let row = run_child_frozen_apex_map_lane(exe, args, &per_chunk, &window_path).with_context(
        || {
            format!(
                "run local mix frozen apex-map-lane input={} window_idx={} chunk_bytes={}",
                input_name, window.window_idx, chunk_bytes
            )
        },
    )?;
    cache.insert(key, row.clone());
    Ok(row)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn choose_default_chunk(
    args: &ApexLaneLawLocalMixFreezeArgs,
    chunk_candidates: &[usize],
    best_chunk_counts: &BTreeMap<usize, usize>,
    reports: &[FileReport],
    local_to_global_maps: &[BTreeMap<String, String>],
    eval_cache: &BTreeMap<(String, usize, usize), FrozenEvalRow>,
    target_global_law_id: &str,
    temp_dir: &Path,
    exe: &Path,
    eval_config: &EvalConfig,
    eval_args: &ApexLaneLawLocalMixFreezeArgs,
) -> Result<usize> {
    if let Some(explicit) = args.default_local_chunk_bytes {
        return Ok(explicit);
    }

    let mut best_choice: Option<(usize, usize, usize, usize)> = None;
    for &chunk_bytes in chunk_candidates {
        let mut total_payload = 0usize;
        let mut improved = 0usize;
        let wins = *best_chunk_counts.get(&chunk_bytes).unwrap_or(&0);

        for (report_idx, report) in reports.iter().enumerate() {
            let input_bytes = fs::read(&report.input)
                .with_context(|| format!("read input for default chunk scan {}", report.input))?;
            let local_to_global = &local_to_global_maps[report_idx];
            for window in &report.windows {
                if local_to_global
                    .get(&window.local_law_id)
                    .map(|id| id == target_global_law_id)
                    .unwrap_or(false)
                {
                    let eval = if let Some(cached) =
                        eval_cache.get(&(report.input.clone(), window.window_idx, chunk_bytes))
                    {
                        cached.clone()
                    } else {
                        eval_window(
                            exe,
                            eval_args,
                            eval_config,
                            &report.input,
                            &input_bytes,
                            window,
                            chunk_bytes,
                            temp_dir,
                            &mut BTreeMap::new(),
                        )?
                    };
                    total_payload = total_payload.saturating_add(eval.compact_field_total_payload_exact);
                    if eval.compact_field_total_payload_exact < window.compact_field_total_payload_exact {
                        improved += 1;
                    }
                }
            }
        }

        match best_choice {
            None => best_choice = Some((chunk_bytes, wins, improved, total_payload)),
            Some((cur_chunk, cur_wins, cur_improved, cur_total)) => {
                if wins > cur_wins
                    || (wins == cur_wins && total_payload < cur_total)
                    || (wins == cur_wins && total_payload == cur_total && improved > cur_improved)
                    || (wins == cur_wins
                        && total_payload == cur_total
                        && improved == cur_improved
                        && chunk_bytes < cur_chunk)
                {
                    best_choice = Some((chunk_bytes, wins, improved, total_payload));
                }
            }
        }
    }

    best_choice
        .map(|v| v.0)
        .ok_or_else(|| anyhow!("could not choose default local chunk"))
}
