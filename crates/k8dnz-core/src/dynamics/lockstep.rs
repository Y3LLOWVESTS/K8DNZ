use crate::fixed::{turn32::Turn32, unit32::Unit32};
use crate::dynamics::state::LockstepState;
use crate::recipe::recipe::LockstepParams;

pub fn enter(phi_l: Turn32) -> LockstepState {
    LockstepState { phi_l, t: Unit32::MIN }
}

pub fn tick(s: LockstepState, p: &LockstepParams) -> LockstepState {
    let phi_l = s.phi_l.wrapping_add(p.v_l);
    let t = s.t.saturating_add(p.t_step);
    LockstepState { phi_l, t }
}

pub fn done(s: &LockstepState) -> bool {
    s.t.is_max()
}
