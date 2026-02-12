use crate::fixed::{turn32::Turn32, unit32::Unit32};

#[derive(Clone, Copy, Debug)]
pub struct FreeOrbitState {
    pub phi_a: Turn32,
    pub phi_c: Turn32,
}

#[derive(Clone, Copy, Debug)]
pub struct LockstepState {
    pub phi_l: Turn32,
    pub t: Unit32,
}

#[derive(Clone, Copy, Debug)]
pub enum Mode {
    FreeOrbit(FreeOrbitState),
    Lockstep {
        pre_lock: FreeOrbitState,
        lock: LockstepState,
    },
}
