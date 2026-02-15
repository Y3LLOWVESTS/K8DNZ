use crate::error::{K8Error, Result};
use crate::fixed::turn32::Turn32;
use crate::recipe::recipe::{Alphabet, Recipe, ResetMode};

pub fn validate_recipe(r: &Recipe) -> Result<()> {
    // FREE_ORBIT invariant: different speeds (magnitudes).
    if r.free.v_a == r.free.v_c {
        return Err(K8Error::Validation("vA must differ from vC".into()));
    }
    // ε must be < 0.5 turns to avoid degenerate always-aligned behavior.
    if r.free.epsilon.0 >= Turn32::HALF.0 {
        return Err(K8Error::Validation("epsilon must be < 0.5 turns".into()));
    }
    // LOCKSTEP: Δ cannot be 0 (would coincide).
    if r.lock.delta.0 == 0 {
        return Err(K8Error::Validation("delta must be non-zero".into()));
    }
    // Sanity: t_step must be non-zero or lockstep never reaches top.
    if r.lock.t_step == 0 {
        return Err(K8Error::Validation("t_step must be non-zero".into()));
    }

    // Alphabet
    match r.alphabet {
        Alphabet::N16 => {}
    }

    // Reset mode
    match r.reset_mode {
        ResetMode::HoldAandC | ResetMode::FromLockstep => {}
    }

    // Field clamp sanity
    if r.field_clamp.min >= r.field_clamp.max {
        return Err(K8Error::Validation(
            "field_clamp.min must be < field_clamp.max".into(),
        ));
    }

    // Quant sanity
    if r.quant.min >= r.quant.max {
        return Err(K8Error::Validation("quant.min must be < quant.max".into()));
    }

    Ok(())
}
