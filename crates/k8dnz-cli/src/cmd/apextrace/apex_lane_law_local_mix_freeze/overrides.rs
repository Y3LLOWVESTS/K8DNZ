use super::types::OverrideCandidate;

pub(crate) fn select_override_subset(
    candidates: &[OverrideCandidate],
    exact_subset_limit: usize,
    min_override_gain_exact: usize,
) -> Vec<usize> {
    let filtered = candidates
        .iter()
        .enumerate()
        .filter(|(_, row)| row.gain_exact >= min_override_gain_exact)
        .map(|(idx, _)| idx)
        .collect::<Vec<_>>();

    if filtered.len() <= exact_subset_limit && filtered.len() <= 20 {
        return select_override_subset_exact(candidates, &filtered);
    }
    select_override_subset_greedy(candidates, &filtered)
}

pub(crate) fn select_override_subset_exact(
    candidates: &[OverrideCandidate],
    filtered: &[usize],
) -> Vec<usize> {
    let mut best = Vec::<usize>::new();
    let mut best_total = usize::MAX;
    let total_masks = 1usize.checked_shl(filtered.len() as u32).unwrap_or(0);

    for mask in 0..total_masks {
        let mut subset = Vec::<usize>::new();
        let mut gain = 0usize;
        for (bit, idx) in filtered.iter().enumerate() {
            if ((mask >> bit) & 1) == 1 {
                subset.push(*idx);
                gain = gain.saturating_add(candidates[*idx].gain_exact);
            }
        }
        let chosen = subset
            .iter()
            .map(|idx| candidates[*idx].clone())
            .collect::<Vec<_>>();
        let path_bytes = override_path_bytes(&chosen);
        let default_payload = candidates.iter().map(|row| row.default_payload_exact).sum::<usize>();
        let total = default_payload.saturating_sub(gain).saturating_add(path_bytes);
        if total < best_total || (total == best_total && subset.len() < best.len()) {
            best_total = total;
            best = subset;
        }
    }

    best.sort_unstable();
    best
}

pub(crate) fn select_override_subset_greedy(
    candidates: &[OverrideCandidate],
    filtered: &[usize],
) -> Vec<usize> {
    let mut order = filtered.to_vec();
    order.sort_by_key(|idx| std::cmp::Reverse(candidates[*idx].gain_exact));

    let mut chosen = Vec::<usize>::new();
    let default_payload = candidates.iter().map(|row| row.default_payload_exact).sum::<usize>();
    let mut current_total = default_payload;

    for idx in order {
        let mut trial = chosen.clone();
        trial.push(idx);
        let chosen_rows = trial
            .iter()
            .map(|i| candidates[*i].clone())
            .collect::<Vec<_>>();
        let gain = trial.iter().map(|i| candidates[*i].gain_exact).sum::<usize>();
        let total = default_payload
            .saturating_sub(gain)
            .saturating_add(override_path_bytes(&chosen_rows));
        if total < current_total {
            chosen = trial;
            current_total = total;
        }
    }

    chosen.sort_unstable();
    chosen
}

pub(crate) fn override_path_bytes(rows: &[OverrideCandidate]) -> usize {
    if rows.is_empty() {
        return 0;
    }

    let mut bytes = Vec::new();
    let mut prev = 0usize;
    let mut ordered = rows.to_vec();
    ordered.sort_by_key(|row| row.window_idx);

    for row in ordered {
        let delta = row.window_idx.saturating_sub(prev);
        put_varint(delta as u64, &mut bytes);
        put_varint(row.best_chunk_bytes as u64, &mut bytes);
        prev = row.window_idx;
    }
    bytes.len()
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
