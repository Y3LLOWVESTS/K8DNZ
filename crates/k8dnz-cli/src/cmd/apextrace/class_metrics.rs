use anyhow::{anyhow, Result};

pub const LANE_CLASS_COUNT: usize = 3;

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct PerClassMetrics {
    pub support: u64,
    pub predicted: u64,
    pub tp: u64,
    pub fp: u64,
    pub fn_: u64,
    pub precision_pct: f64,
    pub recall_pct: f64,
    pub f1_pct: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LaneClassMetrics {
    pub total: u64,
    pub target_hist: [u64; LANE_CLASS_COUNT],
    pub pred_hist: [u64; LANE_CLASS_COUNT],
    pub confusion: [[u64; LANE_CLASS_COUNT]; LANE_CLASS_COUNT],

    pub raw_match_pct: f64,

    pub majority_class: u8,
    pub majority_count: u64,
    pub majority_baseline_match_pct: f64,
    pub raw_match_vs_majority_pct: f64,

    pub pred_dominant_class: u8,
    pub pred_dominant_count: u64,
    pub pred_dominant_share_ppm: u64,
    pub pred_dominant_share_pct: f64,
    pub pred_collapse_90_flag: bool,

    pub target_entropy_bits: f64,
    pub pred_entropy_bits: f64,
    pub hist_l1: u64,
    pub hist_l1_pct: f64,

    pub supported_recall_class_count: u8,
    pub active_macro_class_count: u8,

    pub balanced_accuracy_pct: f64,
    pub macro_precision_pct: f64,
    pub macro_recall_pct: f64,
    pub macro_f1_pct: f64,
    pub weighted_f1_pct: f64,

    pub per_class: [PerClassMetrics; LANE_CLASS_COUNT],
}

pub fn class_label(v: u8) -> &'static str {
    match v {
        0 => "other",
        1 => "space",
        2 => "newline",
        _ => "invalid",
    }
}

pub fn compute_lane_class_metrics(target: &[u8], predicted: &[u8]) -> Result<LaneClassMetrics> {
    if target.len() != predicted.len() {
        return Err(anyhow!(
            "lane class metrics: target len {} != predicted len {}",
            target.len(),
            predicted.len()
        ));
    }

    let mut target_hist = [0u64; LANE_CLASS_COUNT];
    let mut pred_hist = [0u64; LANE_CLASS_COUNT];
    let mut confusion = [[0u64; LANE_CLASS_COUNT]; LANE_CLASS_COUNT];
    let mut matches = 0u64;

    for (&t, &p) in target.iter().zip(predicted.iter()) {
        let ti = class_slot(t)?;
        let pi = class_slot(p)?;
        target_hist[ti] = target_hist[ti].saturating_add(1);
        pred_hist[pi] = pred_hist[pi].saturating_add(1);
        confusion[ti][pi] = confusion[ti][pi].saturating_add(1);
        if ti == pi {
            matches = matches.saturating_add(1);
        }
    }

    let total = target.len() as u64;
    let raw_match_pct = pct(matches, total);

    let (majority_class, majority_count) = argmax_hist(&target_hist);
    let majority_baseline_match_pct = pct(majority_count, total);
    let raw_match_vs_majority_pct = raw_match_pct - majority_baseline_match_pct;

    let (pred_dominant_class, pred_dominant_count) = argmax_hist(&pred_hist);
    let pred_dominant_share_ppm = ppm(pred_dominant_count, total);
    let pred_dominant_share_pct = pct(pred_dominant_count, total);
    let pred_collapse_90_flag = pred_dominant_share_ppm >= 900_000;

    let target_entropy_bits = entropy_bits(&target_hist, total);
    let pred_entropy_bits = entropy_bits(&pred_hist, total);

    let hist_l1 = target_hist
        .iter()
        .zip(pred_hist.iter())
        .map(|(&a, &b)| a.abs_diff(b))
        .sum::<u64>();
    let hist_l1_pct = if total == 0 {
        0.0
    } else {
        (hist_l1 as f64) * 100.0 / ((2 * total) as f64)
    };

    let mut per_class = [PerClassMetrics::default(); LANE_CLASS_COUNT];

    let mut supported_recall_class_count = 0u8;
    let mut active_macro_class_count = 0u8;

    let mut balanced_accuracy_sum = 0.0;
    let mut macro_precision_sum = 0.0;
    let mut macro_recall_sum = 0.0;
    let mut macro_f1_sum = 0.0;
    let mut weighted_f1_sum = 0.0;

    for cls in 0..LANE_CLASS_COUNT {
        let support = target_hist[cls];
        let predicted_count = pred_hist[cls];
        let tp = confusion[cls][cls];
        let fn_ = support.saturating_sub(tp);
        let fp = predicted_count.saturating_sub(tp);

        let precision_pct = pct(tp, predicted_count);
        let recall_pct = pct(tp, support);
        let f1_pct = f1_pct(precision_pct, recall_pct);

        per_class[cls] = PerClassMetrics {
            support,
            predicted: predicted_count,
            tp,
            fp,
            fn_,
            precision_pct,
            recall_pct,
            f1_pct,
        };

        if support > 0 {
            supported_recall_class_count = supported_recall_class_count.saturating_add(1);
            balanced_accuracy_sum += recall_pct;
            macro_recall_sum += recall_pct;
            weighted_f1_sum += f1_pct * (support as f64);
        }

        if support > 0 || predicted_count > 0 {
            active_macro_class_count = active_macro_class_count.saturating_add(1);
            macro_precision_sum += precision_pct;
            macro_f1_sum += f1_pct;
        }
    }

    let balanced_accuracy_pct = if supported_recall_class_count == 0 {
        0.0
    } else {
        balanced_accuracy_sum / (supported_recall_class_count as f64)
    };

    let macro_precision_pct = if active_macro_class_count == 0 {
        0.0
    } else {
        macro_precision_sum / (active_macro_class_count as f64)
    };

    let macro_recall_pct = if supported_recall_class_count == 0 {
        0.0
    } else {
        macro_recall_sum / (supported_recall_class_count as f64)
    };

    let macro_f1_pct = if active_macro_class_count == 0 {
        0.0
    } else {
        macro_f1_sum / (active_macro_class_count as f64)
    };

    let weighted_f1_pct = if total == 0 {
        0.0
    } else {
        weighted_f1_sum / (total as f64)
    };

    Ok(LaneClassMetrics {
        total,
        target_hist,
        pred_hist,
        confusion,
        raw_match_pct,
        majority_class,
        majority_count,
        majority_baseline_match_pct,
        raw_match_vs_majority_pct,
        pred_dominant_class,
        pred_dominant_count,
        pred_dominant_share_ppm,
        pred_dominant_share_pct,
        pred_collapse_90_flag,
        target_entropy_bits,
        pred_entropy_bits,
        hist_l1,
        hist_l1_pct,
        supported_recall_class_count,
        active_macro_class_count,
        balanced_accuracy_pct,
        macro_precision_pct,
        macro_recall_pct,
        macro_f1_pct,
        weighted_f1_pct,
        per_class,
    })
}

fn class_slot(v: u8) -> Result<usize> {
    match v {
        0..=2 => Ok(v as usize),
        _ => Err(anyhow!(
            "lane class metrics: invalid class symbol {}",
            v
        )),
    }
}

fn pct(num: u64, den: u64) -> f64 {
    if den == 0 {
        0.0
    } else {
        (num as f64) * 100.0 / (den as f64)
    }
}

fn ppm(num: u64, den: u64) -> u64 {
    if den == 0 {
        0
    } else {
        num.saturating_mul(1_000_000) / den
    }
}

fn f1_pct(precision_pct: f64, recall_pct: f64) -> f64 {
    if precision_pct <= 0.0 || recall_pct <= 0.0 {
        0.0
    } else {
        (2.0 * precision_pct * recall_pct) / (precision_pct + recall_pct)
    }
}

fn argmax_hist(hist: &[u64; LANE_CLASS_COUNT]) -> (u8, u64) {
    let mut best_idx = 0usize;
    let mut best_val = 0u64;
    for (idx, &val) in hist.iter().enumerate() {
        if val > best_val {
            best_idx = idx;
            best_val = val;
        }
    }
    (best_idx as u8, best_val)
}

fn entropy_bits(hist: &[u64; LANE_CLASS_COUNT], total: u64) -> f64 {
    if total == 0 {
        return 0.0;
    }

    let mut ent = 0.0;
    for &count in hist {
        if count == 0 {
            continue;
        }
        let p = (count as f64) / (total as f64);
        ent -= p * p.log2();
    }
    ent
}

#[cfg(test)]
mod tests {
    use super::{class_label, compute_lane_class_metrics};

    fn approx_eq(a: f64, b: f64, eps: f64) {
        assert!(
            (a - b).abs() <= eps,
            "expected {} ~= {} within {}",
            a,
            b,
            eps
        );
    }

    #[test]
    fn genesis_style_all_other_predictor_is_exact_majority_mirage() {
        let mut target = Vec::new();
        target.extend(std::iter::repeat_n(0u8, 3344));
        target.extend(std::iter::repeat_n(1u8, 797));
        target.extend(std::iter::repeat_n(2u8, 60));

        let predicted = vec![0u8; target.len()];
        let m = compute_lane_class_metrics(&target, &predicted).unwrap();

        assert_eq!(class_label(m.majority_class), "other");
        assert_eq!(m.majority_count, 3344);
        approx_eq(m.majority_baseline_match_pct, 79.60009521542585, 1e-9);
        approx_eq(m.raw_match_pct, m.majority_baseline_match_pct, 1e-9);
        approx_eq(m.raw_match_vs_majority_pct, 0.0, 1e-9);
        approx_eq(m.balanced_accuracy_pct, 33.333333333333336, 1e-9);
        assert_eq!(m.pred_dominant_class, 0);
        assert_eq!(m.pred_dominant_share_ppm, 1_000_000);
        assert!(m.pred_collapse_90_flag);
        assert_eq!(m.confusion[0][0], 3344);
        assert_eq!(m.confusion[1][0], 797);
        assert_eq!(m.confusion[2][0], 60);
    }

    #[test]
    fn noncollapsed_predictor_can_have_lower_raw_but_much_better_balance() {
        let target = vec![0u8, 0, 0, 0, 0, 0, 1, 2];
        let collapsed = vec![0u8, 0, 0, 0, 0, 0, 0, 0];
        let structured = vec![0u8, 0, 0, 1, 1, 1, 1, 2];

        let a = compute_lane_class_metrics(&target, &collapsed).unwrap();
        let b = compute_lane_class_metrics(&target, &structured).unwrap();

        assert!(a.raw_match_pct > b.raw_match_pct);
        assert!(b.balanced_accuracy_pct > a.balanced_accuracy_pct);
        assert!(b.macro_f1_pct > a.macro_f1_pct);
        assert!(!b.pred_collapse_90_flag);
    }
}