use anyhow::{anyhow, Context, Result};
use k8dnz_apextrace::{branch_name, render_lattice, render_paths, render_subtree_stats, ApexKey, SubtreeStats};
use k8dnz_core::symbol::{patch::PatchList, varint};

pub fn render_lattice_csv(key: &ApexKey, max_quats: Option<u64>, active_only: bool) -> Result<String> {
    let points = render_lattice(key, max_quats)?;
    let mut out = String::from("row,k,x,y,visits,leaf_span,active\n");
    for p in points {
        if active_only && p.visits == 0 {
            continue;
        }
        out.push_str(&format!(
            "{},{},{},{},{},{},{}\n",
            p.row,
            p.k,
            p.x,
            p.y,
            p.visits,
            p.leaf_span,
            if p.visits > 0 { 1 } else { 0 }
        ));
    }
    Ok(out)
}

pub fn render_lattice_txt(key: &ApexKey, max_quats: Option<u64>, active_only: bool) -> Result<String> {
    let points = render_lattice(key, max_quats)?;
    let mut out = String::new();
    for p in points {
        if active_only && p.visits == 0 {
            continue;
        }
        out.push_str(&format!(
            "row={} k={} x={} y={} visits={} leaf_span={}\n",
            p.row, p.k, p.x, p.y, p.visits, p.leaf_span
        ));
    }
    Ok(out)
}

pub fn render_paths_csv(key: &ApexKey, max_quats: Option<u64>) -> Result<String> {
    let (points, _) = render_paths(key, max_quats)?;
    let mut out = String::from("leaf,step,row,k,x,y,branch,q,u_hex,quat\n");
    for p in points {
        let quat = p.quat.map(|v| v.to_string()).unwrap_or_default();
        out.push_str(&format!(
            "{},{},{},{},{},{},{},{},0x{:016X},{}\n",
            p.leaf,
            p.step,
            p.row,
            p.k,
            p.x,
            p.y,
            branch_name(p.branch),
            p.q,
            p.u,
            quat
        ));
    }
    Ok(out)
}

pub fn render_paths_txt(key: &ApexKey, max_quats: Option<u64>) -> Result<String> {
    let (points, _) = render_paths(key, max_quats)?;
    let mut out = String::new();
    for p in points {
        match p.quat {
            Some(quat) => out.push_str(&format!(
                "leaf={} step={} row={} k={} x={} y={} branch={} q={} u=0x{:016X} quat={}\n",
                p.leaf, p.step, p.row, p.k, p.x, p.y, branch_name(p.branch), p.q, p.u, quat
            )),
            None => out.push_str(&format!(
                "leaf={} step={} row={} k={} x={} y={} branch={} q={} u=0x{:016X}\n",
                p.leaf, p.step, p.row, p.k, p.x, p.y, branch_name(p.branch), p.q, p.u
            )),
        }
    }
    Ok(out)
}

pub fn render_base_csv(key: &ApexKey, max_quats: Option<u64>) -> Result<String> {
    let (_, labels) = render_paths(key, max_quats)?;
    let mut out = String::from("leaf,row,k,x,y,quat\n");
    for b in labels {
        out.push_str(&format!("{},{},{},{},{},{}\n", b.leaf, b.row, b.k, b.x, b.y, b.quat));
    }
    Ok(out)
}

pub fn render_base_txt(key: &ApexKey, max_quats: Option<u64>) -> Result<String> {
    let (_, labels) = render_paths(key, max_quats)?;
    let mut out = String::new();
    for b in labels {
        out.push_str(&format!("leaf={} row={} k={} x={} y={} quat={}\n", b.leaf, b.row, b.k, b.x, b.y, b.quat));
    }
    Ok(out)
}

pub fn render_stats_csv(key: &ApexKey, target: &[u8], max_quats: Option<u64>, active_only: bool) -> Result<String> {
    let stats = render_subtree_stats(key, target, max_quats)?;
    let mut out = String::from(
        "row,k,x,y,subtree_size,leaf_range_start,leaf_range_end,target_hist_1,target_hist_2,target_hist_3,target_hist_4,pred_hist_1,pred_hist_2,pred_hist_3,pred_hist_4,matches,mismatches,match_rate,match_excess_vs_random,target_purity,pred_purity,target_entropy,pred_entropy,active\n",
    );
    for s in stats {
        if active_only && !s.active() {
            continue;
        }
        out.push_str(&format!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{}\n",
            s.row,
            s.k,
            s.x,
            s.y,
            s.subtree_size,
            s.leaf_range_start,
            s.leaf_range_end,
            s.target_hist[0],
            s.target_hist[1],
            s.target_hist[2],
            s.target_hist[3],
            s.pred_hist[0],
            s.pred_hist[1],
            s.pred_hist[2],
            s.pred_hist[3],
            s.matches,
            s.mismatches,
            percent_from_ppm(s.match_rate_ppm()),
            signed_percent_from_ppm(s.match_excess_ppm()),
            percent_from_ppm(s.target_purity_ppm()),
            percent_from_ppm(s.pred_purity_ppm()),
            s.target_entropy_bits(),
            s.pred_entropy_bits(),
            if s.active() { 1 } else { 0 }
        ));
    }
    Ok(out)
}

pub fn render_stats_txt(key: &ApexKey, target: &[u8], max_quats: Option<u64>, active_only: bool) -> Result<String> {
    let stats = render_subtree_stats(key, target, max_quats)?;
    let mut out = String::new();
    for s in stats {
        if active_only && !s.active() {
            continue;
        }
        out.push_str(&format!(
            "row={} k={} x={} y={} subtree_size={} leaf_range_start={} leaf_range_end={} target_hist=[{},{},{},{}] pred_hist=[{},{},{},{}] matches={} mismatches={} match_rate={:.6} match_excess_vs_random={:.6} target_purity={:.6} pred_purity={:.6} target_entropy={:.6} pred_entropy={:.6}\n",
            s.row,
            s.k,
            s.x,
            s.y,
            s.subtree_size,
            s.leaf_range_start,
            s.leaf_range_end,
            s.target_hist[0],
            s.target_hist[1],
            s.target_hist[2],
            s.target_hist[3],
            s.pred_hist[0],
            s.pred_hist[1],
            s.pred_hist[2],
            s.pred_hist[3],
            s.matches,
            s.mismatches,
            percent_from_ppm(s.match_rate_ppm()),
            signed_percent_from_ppm(s.match_excess_ppm()),
            percent_from_ppm(s.target_purity_ppm()),
            percent_from_ppm(s.pred_purity_ppm()),
            s.target_entropy_bits(),
            s.pred_entropy_bits(),
        ));
    }
    Ok(out)
}

pub fn pick_hot_node(stats: &[SubtreeStats]) -> Option<&SubtreeStats> {
    pick_hot_node_min(stats, 1)
}

pub fn pick_hot_node_min(stats: &[SubtreeStats], min_subtree: u64) -> Option<&SubtreeStats> {
    let mut best: Option<&SubtreeStats> = None;
    for stat in stats {
        if !stat.active() || stat.row == 0 || stat.subtree_size < min_subtree {
            continue;
        }
        match best {
            None => best = Some(stat),
            Some(current) => {
                if hot_node_sort_key(stat) > hot_node_sort_key(current) {
                    best = Some(stat);
                }
            }
        }
    }
    best
}

fn hot_node_sort_key(stat: &SubtreeStats) -> (i64, u64, u64, u16, std::cmp::Reverse<u16>) {
    (
        stat.match_excess_ppm(),
        stat.matches,
        stat.subtree_size,
        stat.row,
        std::cmp::Reverse(stat.k),
    )
}

pub fn render_quats_ascii(quats: &[u8]) -> String {
    let mut out = String::with_capacity(quats.len());
    for &q in quats {
        out.push(match q {
            1 => '1',
            2 => '2',
            3 => '3',
            4 => '4',
            _ => '?',
        });
    }
    out
}

pub fn write_or_print(out: Option<&str>, body: &str) -> Result<()> {
    match out {
        Some(path) => std::fs::write(path, body.as_bytes()).with_context(|| format!("write {}", path))?,
        None => print!("{body}"),
    }
    Ok(())
}

pub fn match_pct(matches: u64, total: u64) -> f64 {
    if total == 0 { 0.0 } else { (matches as f64) * 100.0 / (total as f64) }
}

pub fn match_pct_f64(matches: f64, total: u64) -> f64 {
    if total == 0 { 0.0 } else { matches * 100.0 / (total as f64) }
}

const MAGIC_K8L1_ANY: &[u8; 4] = b"K8L1";
const K8L1_VERSION_MIN_ANY: u8 = 1;
const K8L1_VERSION_MAX_ANY: u8 = 3;

#[derive(Clone, Debug)]
pub struct K8L1ViewAny {
    pub class_patch: Vec<u8>,
    #[allow(dead_code)]
    pub other_patch: Vec<u8>,
    #[allow(dead_code)]
    pub omega_len: usize,
    #[allow(dead_code)]
    pub trailing_len: usize,
}

pub fn decode_k8l1_view_any(bytes: &[u8]) -> Result<K8L1ViewAny> {
    let mut i = 0usize;
    if bytes.len() < 5 {
        return Err(anyhow!("k8l1: too short"));
    }
    if &bytes[0..4] != MAGIC_K8L1_ANY {
        return Err(anyhow!("k8l1: bad magic"));
    }
    i += 4;
    let ver = bytes[i];
    i += 1;
    if !(K8L1_VERSION_MIN_ANY..=K8L1_VERSION_MAX_ANY).contains(&ver) {
        return Err(anyhow!("k8l1: unsupported version {}", ver));
    }
    let _total_len = varint::get_u64(bytes, &mut i)? as usize;
    let _other_len = varint::get_u64(bytes, &mut i)? as usize;
    let _max_ticks = varint::get_u64(bytes, &mut i)?;
    let recipe_len = varint::get_u64(bytes, &mut i)? as usize;
    if i + recipe_len > bytes.len() {
        return Err(anyhow!("k8l1: recipe oob"));
    }
    i += recipe_len;
    let mut omega_len = 0usize;
    if ver >= 2 {
        omega_len = varint::get_u64(bytes, &mut i)? as usize;
        if i + omega_len > bytes.len() {
            return Err(anyhow!("k8l1: omega oob"));
        }
        i += omega_len;
    }
    let class_patch_len = varint::get_u64(bytes, &mut i)? as usize;
    if i + class_patch_len > bytes.len() {
        return Err(anyhow!("k8l1: class_patch oob"));
    }
    let class_patch = bytes[i..i + class_patch_len].to_vec();
    i += class_patch_len;
    let other_patch_len = varint::get_u64(bytes, &mut i)? as usize;
    if i + other_patch_len > bytes.len() {
        return Err(anyhow!("k8l1: other_patch oob"));
    }
    let other_patch = bytes[i..i + other_patch_len].to_vec();
    i += other_patch_len;
    Ok(K8L1ViewAny {
        class_patch,
        other_patch,
        omega_len,
        trailing_len: bytes.len().saturating_sub(i),
    })
}

pub fn patch_count(patch_bytes: &[u8]) -> Result<usize> {
    let p = PatchList::decode(patch_bytes).map_err(|e| anyhow!("{e}"))?;
    Ok(p.entries.len())
}

pub fn percent_from_ppm(ppm: u64) -> f64 {
    (ppm as f64) / 10_000.0
}

pub fn signed_percent_from_ppm(ppm: i64) -> f64 {
    (ppm as f64) / 10_000.0
}
