// crates/k8dnz-cli/src/cmd/timemap/residual.rs

use super::args::ResidualMode;

pub fn make_residual_byte(mode: ResidualMode, model: u8, plain: u8) -> u8 {
    match mode {
        ResidualMode::Xor => model ^ plain,
        ResidualMode::Sub => plain.wrapping_sub(model),
    }
}

pub fn apply_residual_byte(mode: ResidualMode, model: u8, resid: u8) -> u8 {
    match mode {
        ResidualMode::Xor => model ^ resid,
        ResidualMode::Sub => model.wrapping_add(resid),
    }
}

pub fn sym_mask(bits_per_emission: u8) -> u8 {
    if bits_per_emission == 0 {
        0
    } else if bits_per_emission >= 8 {
        0xFF
    } else {
        ((1u16 << bits_per_emission) - 1) as u8
    }
}

pub fn make_residual_symbol(mode: ResidualMode, model: u8, plain: u8, mask: u8) -> u8 {
    match mode {
        ResidualMode::Xor => (model ^ plain) & mask,
        ResidualMode::Sub => plain.wrapping_sub(model) & mask,
    }
}

pub fn apply_residual_symbol(mode: ResidualMode, model: u8, resid: u8, mask: u8) -> u8 {
    match mode {
        ResidualMode::Xor => (model ^ resid) & mask,
        ResidualMode::Sub => model.wrapping_add(resid) & mask,
    }
}
