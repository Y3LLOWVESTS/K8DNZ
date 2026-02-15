// crates/k8dnz-cli/src/cmd/recipe.rs

use clap::{Args, Subcommand};
use k8dnz_core::recipe::format as recipe_format;
use k8dnz_core::Recipe;

use crate::io::recipe_file;

#[derive(Args)]
pub struct RecipeArgs {
    #[command(subcommand)]
    pub cmd: RecipeCmd,
}

#[derive(Subcommand)]
pub enum RecipeCmd {
    /// Print all recipe fields (human readable) and warn on degenerate ranges
    Inspect(InspectArgs),
}

#[derive(Args)]
pub struct InspectArgs {
    /// Recipe path (.k8r)
    #[arg(long)]
    pub recipe: String,
}

pub fn run(args: RecipeArgs) -> anyhow::Result<()> {
    match args.cmd {
        RecipeCmd::Inspect(a) => cmd_inspect(a),
    }
}

fn cmd_inspect(a: InspectArgs) -> anyhow::Result<()> {
    let r: Recipe = recipe_file::load_k8r(&a.recipe)?;
    let rid = recipe_format::recipe_id_hex(&r);

    println!("recipe_path  = {}", a.recipe);
    println!("recipe_id    = {}", rid);

    // Header-ish fields (use Debug for maximum compatibility)
    println!("version      = {:?}", r.version);
    println!("seed         = {:?}", r.seed);
    println!("alphabet     = {:?}", r.alphabet);
    println!("reset_mode   = {:?}", r.reset_mode);
    println!("keystream_mix = {:?}", r.keystream_mix);
    println!("payload_kind  = {:?}", r.payload_kind);

    // Free orbit (Turn32 prints via Debug)
    println!("free.phi_a0  = {:?}", r.free.phi_a0);
    println!("free.phi_c0  = {:?}", r.free.phi_c0);
    println!("free.v_a     = {:?}", r.free.v_a);
    println!("free.v_c     = {:?}", r.free.v_c);
    println!("free.epsilon = {:?}", r.free.epsilon);

    // Lockstep (Turn32 prints via Debug)
    println!("lock.v_l     = {:?}", r.lock.v_l);
    println!("lock.delta   = {:?}", r.lock.delta);
    println!("lock.t_step  = {:?}", r.lock.t_step);

    // Field (your FieldParams only has waves)
    println!("field.waves  = {}", r.field.waves.len());
    for (i, w) in r.field.waves.iter().enumerate() {
        // Per compiler, FieldWave fields: k_phi, k_t, k_time, phase, amp
        println!(
            "field.wave[{}] amp={:?} phase={:?} k_phi={:?} k_t={:?} k_time={:?}",
            i, w.amp, w.phase, w.k_phi, w.k_t, w.k_time
        );
    }

    // Clamp/quant (these are the knobs most likely to cause “all zero” streams)
    println!("field_clamp.min = {:?}", r.field_clamp.min);
    println!("field_clamp.max = {:?}", r.field_clamp.max);

    println!("quant.min    = {:?}", r.quant.min);
    println!("quant.max    = {:?}", r.quant.max);
    println!("quant.shift  = {:?}", r.quant.shift);

    // RGB config (print Debug; avoids Display constraints)
    println!("rgb.backend  = {:?}", r.rgb.backend);
    println!("rgb.alt_mode = {:?}", r.rgb.alt_mode);
    println!("rgb.base_a   = {:?}", r.rgb.base_a);
    println!("rgb.base_c   = {:?}", r.rgb.base_c);
    println!("rgb.g_step   = {:?}", r.rgb.g_step);
    println!("rgb.p_scale  = {:?}", r.rgb.p_scale);

    println!();
    println!("--- diagnostics ---");
    diagnostics(&r);

    Ok(())
}

fn diagnostics(r: &Recipe) {
    // Clamp degeneration is a prime suspect for “flatline output”.
    if r.field_clamp.min == r.field_clamp.max {
        println!(
            "WARN: field_clamp is degenerate (min==max=={:?}). Field will clamp to a constant.",
            r.field_clamp.min
        );
    } else if r.field_clamp.min > r.field_clamp.max {
        println!(
            "WARN: field_clamp is inverted (min={:?} > max={:?}). Tuner should avoid this.",
            r.field_clamp.min, r.field_clamp.max
        );
    }

    // Quant degeneration is the other prime suspect.
    if r.quant.min == r.quant.max {
        println!(
            "WARN: quant range is degenerate (min==max=={:?}). Quantization can collapse to a constant bin.",
            r.quant.min
        );
    } else if r.quant.min > r.quant.max {
        println!(
            "WARN: quant range is inverted (min={:?} > max={:?}). Tuner should avoid this.",
            r.quant.min, r.quant.max
        );
    }

    if r.quant.min >= r.quant.max {
        println!(
            "WARN: quant.min >= quant.max (min={:?}, max={:?}). Non-degenerate quantization requires min < max.",
            r.quant.min, r.quant.max
        );
    }

    if r.field_clamp.min >= r.field_clamp.max {
        println!(
            "WARN: field_clamp.min >= field_clamp.max (min={:?}, max={:?}). Non-degenerate clamp requires min < max.",
            r.field_clamp.min, r.field_clamp.max
        );
    }

    // We intentionally avoid “Turn32 == 0” checks here because Turn32 may not expose a constructor
    // or comparable literal. The clamp/quant warnings are the high-signal checks for the zero-stream bug.
}
