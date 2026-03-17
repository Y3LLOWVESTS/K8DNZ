use k8dnz_apextrace::{
    generate_bytes, point_xy, render_lattice, render_paths, render_subtree_stats, ApexKey,
};

#[test]
fn point_coordinates_match_90_degree_lattice() {
    assert_eq!(point_xy(0, 0), (0, 0));
    assert_eq!(point_xy(1, 0), (-1, 1));
    assert_eq!(point_xy(1, 1), (1, 1));
    assert_eq!(point_xy(2, 0), (-2, 2));
    assert_eq!(point_xy(2, 1), (0, 2));
    assert_eq!(point_xy(2, 2), (2, 2));
}

#[test]
fn lattice_and_paths_are_consistent_for_small_key() {
    let key = ApexKey::new_dibit_v1(2, 1, 7, 3).unwrap();
    let lattice = render_lattice(&key, None).unwrap();
    let (paths, labels) = render_paths(&key, None).unwrap();

    assert_eq!(key.quat_len, 8);
    assert_eq!(lattice.len(), 10);
    assert_eq!(labels.len(), 8);
    assert_eq!(paths.len(), 8 * (usize::from(key.depth) + 1));
    assert_eq!(lattice[0].visits, 8);
}

#[test]
fn subtree_stats_root_is_perfect_for_self_generated_target() {
    let key = ApexKey::new_dibit_v1(2, 1, 7, 3).unwrap();
    let bytes = generate_bytes(&key).unwrap();
    let stats = render_subtree_stats(&key, &bytes, None).unwrap();

    let root = stats.iter().find(|s| s.row == 0 && s.k == 0).unwrap();
    assert_eq!(root.subtree_size, key.quat_len);
    assert_eq!(root.leaf_range_start, 0);
    assert_eq!(root.leaf_range_end, key.quat_len - 1);
    assert_eq!(root.matches, key.quat_len);
    assert_eq!(root.mismatches, 0);
    assert_eq!(root.match_rate_ppm(), 1_000_000);
    assert_eq!(root.match_excess_ppm(), 750_000);
}
