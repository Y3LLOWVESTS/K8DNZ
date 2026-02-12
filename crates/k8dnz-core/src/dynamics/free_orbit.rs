use crate::fixed::{math::turn32_dist, turn32::Turn32};
use crate::dynamics::state::FreeOrbitState;
use crate::recipe::recipe::FreeOrbitParams;

/// Tick free orbit (A forward, C backward).
pub fn tick(s: FreeOrbitState, p: &FreeOrbitParams) -> FreeOrbitState {
    FreeOrbitState {
        phi_a: s.phi_a.wrapping_add(p.v_a),
        phi_c: s.phi_c.wrapping_sub(p.v_c),
    }
}

pub fn aligned(s: FreeOrbitState, epsilon: Turn32) -> bool {
    turn32_dist(s.phi_a, s.phi_c).0 <= epsilon.0
}
