use k8dnz_apextrace::{ApexMap, ApexMapCfg, OverrideDecision, RefineCfg};

#[test]
fn apex_map_boundary_pair_tracks_left_and_right_modes() {
    let symbols = vec![0u8, 0, 0, 0, 2, 2, 2, 2];
    let map = ApexMap::from_symbols(
        &symbols,
        ApexMapCfg {
            class_count: 3,
            max_depth: 0,
            depth_shift: 1,
        },
    )
    .unwrap();

    let pair = map.boundary_pair(4, 1).unwrap();
    assert_eq!(pair.left, 0);
    assert_eq!(pair.right, 2);
}

#[test]
fn apex_map_is_deterministic() {
    let symbols = vec![0u8, 1, 1, 0, 2, 2, 1, 0, 0, 2, 1, 1];
    let cfg = ApexMapCfg {
        class_count: 3,
        max_depth: 0,
        depth_shift: 1,
    };

    let a = ApexMap::from_symbols(&symbols, cfg).unwrap();
    let b = ApexMap::from_symbols(&symbols, cfg).unwrap();

    assert_eq!(a, b);

    for pos in 0..symbols.len() {
        assert_eq!(a.score_at(pos).unwrap(), b.score_at(pos).unwrap());
        assert_eq!(a.dominant_at(pos).unwrap(), b.dominant_at(pos).unwrap());
    }
}

#[test]
fn apex_map_refine_boundaries_can_flip_toward_pair() {
    let field_source = vec![0u8, 0, 0, 0, 2, 2, 2, 2];
    let base = vec![0u8, 0, 1, 1, 1, 1, 2, 2];

    let map = ApexMap::from_symbols(
        &field_source,
        ApexMapCfg {
            class_count: 3,
            max_depth: 0,
            depth_shift: 1,
        },
    )
    .unwrap();

    let cfg = RefineCfg::new(1, 1, 1);
    let (refined, stats) = map.refine_boundaries(&base, &[4], cfg).unwrap();

    assert_eq!(refined[3], 0);
    assert_eq!(refined[4], 2);
    assert!(stats.overrides >= 2);
    assert_eq!(stats.boundary_count, 1);
}

#[test]
fn apex_map_class_specific_margin_can_block_newline_flip() {
    let field_source = vec![0u8, 0, 0, 0, 2, 2, 2, 2];
    let base = vec![0u8, 0, 1, 1, 1, 1, 2, 2];

    let map = ApexMap::from_symbols(
        &field_source,
        ApexMapCfg {
            class_count: 3,
            max_depth: 0,
            depth_shift: 1,
        },
    )
    .unwrap();

    let mut cfg = RefineCfg::new(1, 1, 1);
    cfg.desired_margin_add[2] = 1_000_000;

    let (refined, _) = map.refine_boundaries(&base, &[4], cfg).unwrap();

    assert_eq!(refined[3], 0);
    assert_eq!(refined[4], 1);
}

#[test]
fn apex_map_newline_mask_can_block_flip_from_other() {
    let field_source = vec![0u8, 0, 0, 0, 2, 2, 2, 2];
    let base = vec![0u8, 0, 0, 0, 0, 0, 2, 2];

    let map = ApexMap::from_symbols(
        &field_source,
        ApexMapCfg {
            class_count: 3,
            max_depth: 0,
            depth_shift: 1,
        },
    )
    .unwrap();

    let mut cfg = RefineCfg::new(2, 1, 1);
    cfg.desired_from_mask[2] = (1 << 1) | (1 << 2);

    let (refined, _) = map.refine_boundaries(&base, &[4], cfg).unwrap();

    assert_eq!(refined[4], 0);
    assert_eq!(refined[5], 0);
}

#[test]
fn apex_map_evaluate_override_reports_mask_block_reason() {
    let field_source = vec![0u8, 0, 0, 0, 2, 2, 2, 2];
    let map = ApexMap::from_symbols(
        &field_source,
        ApexMapCfg {
            class_count: 3,
            max_depth: 0,
            depth_shift: 1,
        },
    )
    .unwrap();

    let mut cfg = RefineCfg::new(2, 1, 1);
    cfg.desired_from_mask[2] = (1 << 1) | (1 << 2);

    let trace = map.evaluate_override(4, 0, 2, cfg).unwrap();
    assert_eq!(trace.decision, OverrideDecision::DisallowedFromMask);
    assert!(!trace.applied());
}

#[test]
fn apex_map_evaluate_override_reports_margin_reason() {
    let field_source = vec![0u8, 0, 0, 0, 2, 2, 2, 2];
    let map = ApexMap::from_symbols(
        &field_source,
        ApexMapCfg {
            class_count: 3,
            max_depth: 0,
            depth_shift: 1,
        },
    )
    .unwrap();

    let mut cfg = RefineCfg::new(2, 1, 1);
    cfg.desired_margin_add[2] = 1_000_000;

    let trace = map.evaluate_override(4, 1, 2, cfg).unwrap();
    assert_eq!(trace.decision, OverrideDecision::MarginTooSmall);
    assert!(!trace.applied());
}


#[test]
fn apex_map_space_to_newline_transition_margin_can_block_flip() {
    let field_source = vec![0u8, 0, 0, 0, 2, 2, 2, 2];
    let base = vec![0u8, 0, 1, 1, 1, 1, 2, 2];

    let map = ApexMap::from_symbols(
        &field_source,
        ApexMapCfg {
            class_count: 3,
            max_depth: 0,
            depth_shift: 1,
        },
    )
    .unwrap();

    let mut cfg = RefineCfg::new(2, 1, 1);
    cfg.transition_margin_add[1][2] = 1_000_000;

    let trace = map.evaluate_override(4, 1, 2, cfg).unwrap();
    assert_eq!(trace.decision, OverrideDecision::MarginTooSmall);
    assert!(!trace.applied());
}

#[test]
fn apex_map_newline_budget_caps_applied_overrides() {
    let field_source = vec![0u8, 0, 0, 0, 2, 2, 2, 2];
    let base = vec![0u8, 0, 1, 1, 1, 1, 2, 2];

    let map = ApexMap::from_symbols(
        &field_source,
        ApexMapCfg {
            class_count: 3,
            max_depth: 0,
            depth_shift: 1,
        },
    )
    .unwrap();

    let mut cfg = RefineCfg::new(2, 1, 1);
    cfg.desired_apply_budget[2] = 1;

    let (refined, stats) = map.refine_boundaries(&base, &[4], cfg).unwrap();

    assert_eq!(refined[4], 2);
    assert_eq!(refined[5], 1);
    assert_eq!(stats.applied_by_desired[2], 1);
    assert_eq!(stats.blocked_by_budget[2], 1);
}
