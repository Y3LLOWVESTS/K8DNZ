use anyhow::{anyhow, Result};

pub fn baseline_symbol_lane(len: usize, symbol: u8, class_count: u8) -> Result<Vec<u8>> {
    if class_count == 0 || symbol >= class_count {
        return Err(anyhow!(
            "baseline symbol lane: invalid symbol {} for class_count {}",
            symbol,
            class_count
        ));
    }
    Ok(vec![symbol; len])
}

pub fn anchored_consensus_prediction(
    len: usize,
    anchor_symbol: u8,
    promote_symbol: u8,
    class_count: u8,
    predictors: &[&[u8]],
    min_votes: usize,
) -> Result<(Vec<u8>, usize)> {
    if class_count == 0 || anchor_symbol >= class_count || promote_symbol >= class_count {
        return Err(anyhow!(
            "anchored consensus prediction: invalid symbols anchor={} promote={} class_count={}",
            anchor_symbol,
            promote_symbol,
            class_count
        ));
    }
    if predictors.is_empty() {
        return Err(anyhow!("anchored consensus prediction: predictors cannot be empty"));
    }
    let min_votes = min_votes.max(1).min(predictors.len());

    let mut out = vec![anchor_symbol; len];
    let mut promoted = 0usize;

    for pos in 0..len {
        let mut votes = 0usize;
        for pred in predictors {
            if pred.len() != len {
                return Err(anyhow!(
                    "anchored consensus prediction: predictor len {} != expected {}",
                    pred.len(),
                    len
                ));
            }
            let sym = pred[pos];
            if sym >= class_count {
                return Err(anyhow!(
                    "anchored consensus prediction: predictor symbol {} out of range for class_count {}",
                    sym,
                    class_count
                ));
            }
            if sym == promote_symbol {
                votes += 1;
            }
        }
        if votes >= min_votes {
            out[pos] = promote_symbol;
            promoted += 1;
        }
    }

    Ok((out, promoted))
}