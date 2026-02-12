use crate::fixed::turn32::Turn32;
use crate::dynamics::state::FreeOrbitState;

pub fn reset_from_lockstep(phi_l: Turn32, delta: Turn32) -> FreeOrbitState {
    FreeOrbitState {
        phi_a: phi_l,
        phi_c: phi_l.wrapping_add(delta),
    }
}
