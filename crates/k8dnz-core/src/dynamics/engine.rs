// crates/k8dnz-core/src/dynamics/engine.rs

use crate::error::Result;
use crate::validate::validate_recipe;

use crate::dynamics::{
    free_orbit, lockstep, reset,
    state::{FreeOrbitState, Mode},
};
use crate::field::{params::FieldModel, tri_wave};
use crate::fixed::unit32::Unit32;
use crate::recipe::recipe::{Alphabet, Recipe, ResetMode};
use crate::signal::{quantize, sample::FieldSample, token::PairToken};
use crate::stats::counters::Counters;

#[derive(Clone, Copy, Debug, Default)]
pub struct FieldRangeStats {
    pub raw_min: i64,
    pub raw_max: i64,
    pub clamped_min: i64,
    pub clamped_max: i64,
    pub saw_any: bool,
}

impl FieldRangeStats {
    pub fn observe(&mut self, raw: i64, clamped: i64) {
        if !self.saw_any {
            self.saw_any = true;
            self.raw_min = raw;
            self.raw_max = raw;
            self.clamped_min = clamped;
            self.clamped_max = clamped;
        } else {
            self.raw_min = self.raw_min.min(raw);
            self.raw_max = self.raw_max.max(raw);
            self.clamped_min = self.clamped_min.min(clamped);
            self.clamped_max = self.clamped_max.max(clamped);
        }
    }
}

/// Emission-time field samples for the two dots at the top rim.
/// - raw_* is unclamped field
/// - clamped_* is after recipe-driven clamp
#[derive(Clone, Copy, Debug)]
pub struct EmissionField {
    pub raw_a: i64,
    pub raw_c: i64,
    pub clamped_a: i64,
    pub clamped_c: i64,
}

pub struct Engine {
    pub recipe: Recipe,
    pub mode: Mode,
    pub stats: Counters,
    pub field: FieldModel,
    pub time: u64,
}

impl Engine {
    pub fn new(recipe: Recipe) -> Result<Self> {
        validate_recipe(&recipe)?;

        // Field clamp is now driven by recipe (v3+).
        let field = FieldModel::new(recipe.field.clone(), recipe.field_clamp.into());

        let start = FreeOrbitState {
            phi_a: recipe.free.phi_a0,
            phi_c: recipe.free.phi_c0,
        };
        Ok(Self {
            recipe,
            mode: Mode::FreeOrbit(start),
            stats: Counters::default(),
            field,
            time: 0,
        })
    }

    /// Step one tick. Returns Some(token) only on emission.
    pub fn step(&mut self) -> Option<PairToken> {
        self.step_with_fields().map(|(tok, _)| tok)
    }

    /// Step one tick. Returns Some((token, emission_field)) only on emission.
    ///
    /// IMPORTANT: cadence dynamics unchanged; this only exposes emission-time samples.
    pub fn step_with_fields(&mut self) -> Option<(PairToken, EmissionField)> {
        self.stats.ticks += 1;
        self.time = self.time.wrapping_add(1);

        match self.mode {
            Mode::FreeOrbit(s) => {
                let s_next = free_orbit::tick(s, &self.recipe.free);

                // edge-trigger alignment: transition only when it becomes true
                let was = free_orbit::aligned(s, self.recipe.free.epsilon);
                let now = free_orbit::aligned(s_next, self.recipe.free.epsilon);

                if !was && now {
                    self.stats.alignments += 1;
                    let phi_l = s_next.phi_a; // deterministic, simple
                    let lock = lockstep::enter(phi_l);
                    self.mode = Mode::Lockstep {
                        pre_lock: s_next,
                        lock,
                    };
                    None
                } else {
                    self.mode = Mode::FreeOrbit(s_next);
                    None
                }
            }

            Mode::Lockstep { pre_lock, lock } => {
                let lock_next = lockstep::tick(lock, &self.recipe.lock);

                if lockstep::done(&lock_next) {
                    // Emit at top rim (t == MAX)
                    let phi1 = lock_next.phi_l;
                    let phi2 = lock_next.phi_l.wrapping_add(self.recipe.lock.delta);
                    let t_top = Unit32::MAX;

                    // raw + clamped (clamp comes from recipe-driven model.cfg)
                    let s1_raw = tri_wave::eval_raw(&self.field, phi1, t_top, self.time);
                    let s2_raw = tri_wave::eval_raw(&self.field, phi2, t_top, self.time);

                    let s1 = s1_raw.clamp(self.field.cfg.clamp_min, self.field.cfg.clamp_max);
                    let s2 = s2_raw.clamp(self.field.cfg.clamp_min, self.field.cfg.clamp_max);

                    // quantize to N=16 bins using recipe quant range (+ optional shift)
                    let n = match self.recipe.alphabet {
                        Alphabet::N16 => 16u8,
                    };

                    // Apply the deterministic "bin boundary shift" knob:
                    // eff_min = min + shift
                    // eff_max = max + shift
                    let (qmin, qmax) = quantize::shifted_bounds(
                        self.recipe.quant.min,
                        self.recipe.quant.max,
                        self.recipe.quant.shift,
                    );

                    let p0 = quantize::quantize(FieldSample(s1), qmin, qmax, n);
                    let p1 = quantize::quantize(FieldSample(s2), qmin, qmax, n);

                    let tok = PairToken { a: p0, b: p1 };
                    self.stats.emissions += 1;

                    // Reset behavior
                    let next_free = match self.recipe.reset_mode {
                        ResetMode::HoldAandC => pre_lock, // MVP default
                        ResetMode::FromLockstep => {
                            reset::reset_from_lockstep(lock_next.phi_l, self.recipe.lock.delta)
                        }
                    };

                    self.mode = Mode::FreeOrbit(next_free);

                    Some((
                        tok,
                        EmissionField {
                            raw_a: s1_raw,
                            raw_c: s2_raw,
                            clamped_a: s1,
                            clamped_c: s2,
                        },
                    ))
                } else {
                    self.mode = Mode::Lockstep {
                        pre_lock,
                        lock: lock_next,
                    };
                    None
                }
            }
        }
    }

    /// Run until we collect `k` emissions (or until `max_ticks`).
    pub fn run_emissions(&mut self, k: u64, max_ticks: u64) -> Vec<PairToken> {
        let mut out = Vec::with_capacity(k as usize);
        while out.len() < k as usize && self.stats.ticks < max_ticks {
            if let Some(tok) = self.step() {
                out.push(tok);
            }
        }
        out
    }

    /// Like run_emissions, but also returns field-range stats measured at emission time.
    pub fn run_emissions_with_field_stats(
        &mut self,
        k: u64,
        max_ticks: u64,
    ) -> (Vec<PairToken>, FieldRangeStats) {
        let mut out = Vec::with_capacity(k as usize);
        let mut fr = FieldRangeStats::default();

        while out.len() < k as usize && self.stats.ticks < max_ticks {
            // We intentionally share the exact cadence loop with step_with_fields,
            // but we keep this method as-is (your existing code) for stability.
            self.stats.ticks += 1;
            self.time = self.time.wrapping_add(1);

            match self.mode {
                Mode::FreeOrbit(s) => {
                    let s_next = free_orbit::tick(s, &self.recipe.free);

                    let was = free_orbit::aligned(s, self.recipe.free.epsilon);
                    let now = free_orbit::aligned(s_next, self.recipe.free.epsilon);

                    if !was && now {
                        self.stats.alignments += 1;
                        let phi_l = s_next.phi_a;
                        let lock = lockstep::enter(phi_l);
                        self.mode = Mode::Lockstep {
                            pre_lock: s_next,
                            lock,
                        };
                    } else {
                        self.mode = Mode::FreeOrbit(s_next);
                    }
                }

                Mode::Lockstep { pre_lock, lock } => {
                    let lock_next = lockstep::tick(lock, &self.recipe.lock);

                    if lockstep::done(&lock_next) {
                        let phi1 = lock_next.phi_l;
                        let phi2 = lock_next.phi_l.wrapping_add(self.recipe.lock.delta);
                        let t_top = Unit32::MAX;

                        // raw + clamped so we can tune clamp/quant ranges intelligently
                        let s1_raw = tri_wave::eval_raw(&self.field, phi1, t_top, self.time);
                        let s2_raw = tri_wave::eval_raw(&self.field, phi2, t_top, self.time);

                        let s1 = s1_raw.clamp(self.field.cfg.clamp_min, self.field.cfg.clamp_max);
                        let s2 = s2_raw.clamp(self.field.cfg.clamp_min, self.field.cfg.clamp_max);

                        fr.observe(s1_raw, s1);
                        fr.observe(s2_raw, s2);

                        let n = match self.recipe.alphabet {
                            Alphabet::N16 => 16u8,
                        };

                        // Apply the deterministic "bin boundary shift" knob here too.
                        let (qmin, qmax) = quantize::shifted_bounds(
                            self.recipe.quant.min,
                            self.recipe.quant.max,
                            self.recipe.quant.shift,
                        );

                        let p0 = quantize::quantize(FieldSample(s1), qmin, qmax, n);
                        let p1 = quantize::quantize(FieldSample(s2), qmin, qmax, n);

                        let tok = PairToken { a: p0, b: p1 };
                        self.stats.emissions += 1;
                        out.push(tok);

                        let next_free = match self.recipe.reset_mode {
                            ResetMode::HoldAandC => pre_lock,
                            ResetMode::FromLockstep => {
                                reset::reset_from_lockstep(lock_next.phi_l, self.recipe.lock.delta)
                            }
                        };

                        self.mode = Mode::FreeOrbit(next_free);
                    } else {
                        self.mode = Mode::Lockstep {
                            pre_lock,
                            lock: lock_next,
                        };
                    }
                }
            }
        }

        (out, fr)
    }

    /// NEW: run and return both tokens and their emission-time field samples.
    /// This is the bridge we need for true cone-law RGB and DNA-style coupled adders.
    pub fn run_emissions_with_fields(
        &mut self,
        k: u64,
        max_ticks: u64,
    ) -> Vec<(PairToken, EmissionField)> {
        let mut out: Vec<(PairToken, EmissionField)> = Vec::with_capacity(k as usize);
        while out.len() < k as usize && self.stats.ticks < max_ticks {
            if let Some(pair) = self.step_with_fields() {
                out.push(pair);
            }
        }
        out
    }
}
