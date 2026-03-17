use crate::dibit::bytes_to_quats;
use crate::error::{ApexError, Result};
use crate::generator::generate_quats;
use crate::key::ApexKey;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FitScore {
    pub matches: u64,
    pub prefix: u64,
    pub total: u64,
}

impl FitScore {
    pub fn hamming(&self) -> u64 {
        self.total.saturating_sub(self.matches)
    }

    pub fn better_than(&self, other: &Self) -> bool {
        (self.matches, self.prefix) > (other.matches, other.prefix)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FitDiagnostics {
    pub score: FitScore,
    pub byte_matches: u64,
    pub longest_run: u64,
    pub longest_run_start: u64,
    pub target_hist: [u64; 4],
    pub pred_hist: [u64; 4],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FitCandidate {
    pub key: ApexKey,
    pub score: FitScore,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SearchCfg {
    pub seed_from: u64,
    pub seed_count: u64,
    pub seed_step: u64,
    pub recipe_seed: u64,
}

impl Default for SearchCfg {
    fn default() -> Self {
        Self {
            seed_from: 0,
            seed_count: 4096,
            seed_step: 1,
            recipe_seed: 1,
        }
    }
}

pub fn score_key_against_bytes(key: &ApexKey, target_bytes: &[u8]) -> Result<FitScore> {
    Ok(analyze_key_against_bytes(key, target_bytes)?.score)
}

pub fn analyze_key_against_bytes(key: &ApexKey, target_bytes: &[u8]) -> Result<FitDiagnostics> {
    let target_quats = bytes_to_quats(target_bytes)?;
    let pred_quats = generate_quats(key)?;
    analyze_quat_streams(&target_quats, &pred_quats)
}

pub fn analyze_quat_streams(target_quats: &[u8], pred_quats: &[u8]) -> Result<FitDiagnostics> {
    if target_quats.len() != pred_quats.len() {
        return Err(ApexError::Validation(format!(
            "target quat len {} != predicted quat len {}",
            target_quats.len(),
            pred_quats.len()
        )));
    }

    if target_quats.len() % 4 != 0 {
        return Err(ApexError::Validation(format!(
            "quaternary stream length {} is not divisible by 4",
            target_quats.len()
        )));
    }

    let mut matches = 0u64;
    let mut prefix = 0u64;
    let mut still_prefix = true;

    let mut current_run = 0u64;
    let mut current_run_start = 0u64;
    let mut longest_run = 0u64;
    let mut longest_run_start = 0u64;

    let mut target_hist = [0u64; 4];
    let mut pred_hist = [0u64; 4];

    for (idx, (&target, &pred)) in target_quats.iter().zip(pred_quats.iter()).enumerate() {
        let t_slot = quat_slot(target)?;
        let p_slot = quat_slot(pred)?;

        target_hist[t_slot] += 1;
        pred_hist[p_slot] += 1;

        if target == pred {
            matches += 1;
            if still_prefix {
                prefix += 1;
            }

            if current_run == 0 {
                current_run_start = idx as u64;
            }
            current_run += 1;

            if current_run > longest_run {
                longest_run = current_run;
                longest_run_start = current_run_start;
            }
        } else {
            still_prefix = false;
            current_run = 0;
        }
    }

    let mut byte_matches = 0u64;
    for (target, pred) in target_quats.chunks_exact(4).zip(pred_quats.chunks_exact(4)) {
        if target == pred {
            byte_matches += 1;
        }
    }

    Ok(FitDiagnostics {
        score: FitScore {
            matches,
            prefix,
            total: target_quats.len() as u64,
        },
        byte_matches,
        longest_run,
        longest_run_start,
        target_hist,
        pred_hist,
    })
}

pub fn brute_force_best(target_bytes: &[u8], cfg: SearchCfg) -> Result<FitCandidate> {
    if cfg.seed_step == 0 {
        return Err(ApexError::Validation("seed_step must be >= 1".into()));
    }

    let byte_len = target_bytes.len() as u64;
    let mut best: Option<FitCandidate> = None;

    for quadrant in 0u8..=3 {
        let mut i = 0u64;
        while i < cfg.seed_count {
            let seed = cfg.seed_from.saturating_add(i.saturating_mul(cfg.seed_step));
            let key = ApexKey::new_dibit_v1(byte_len, quadrant, seed, cfg.recipe_seed)?;
            let score = score_key_against_bytes(&key, target_bytes)?;
            let candidate = FitCandidate { key, score };

            match &best {
                None => best = Some(candidate),
                Some(current) => {
                    if candidate.score.better_than(&current.score) {
                        best = Some(candidate);
                    }
                }
            }

            i += 1;
        }
    }

    best.ok_or_else(|| ApexError::Validation("search produced no candidates".into()))
}

fn quat_slot(v: u8) -> Result<usize> {
    match v {
        1..=4 => Ok((v - 1) as usize),
        _ => Err(ApexError::Validation(format!(
            "invalid quaternary symbol {v}; expected 1..=4"
        ))),
    }
}
