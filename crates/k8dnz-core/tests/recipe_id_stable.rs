use k8dnz_core::recipe;

#[test]
fn recipe_id_changes_when_qshift_changes() {
    let r0 = recipe::defaults::default_recipe();
    let id0 = recipe::format::recipe_id_16(&r0);

    let mut r1 = recipe::defaults::default_recipe();
    r1.quant.shift = r1.quant.shift.saturating_add(1);
    let id1 = recipe::format::recipe_id_16(&r1);

    assert_ne!(id0, id1, "recipe_id must change when qshift changes");
}

#[test]
fn recipe_id_is_stable_for_same_recipe() {
    let r = recipe::defaults::default_recipe();
    let a = recipe::format::recipe_id_16(&r);
    let b = recipe::format::recipe_id_16(&r);
    assert_eq!(a, b);
}
