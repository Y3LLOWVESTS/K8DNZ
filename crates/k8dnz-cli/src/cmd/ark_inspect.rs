use clap::Args;

use crate::io::ark;

#[derive(Args, Debug)]
pub struct ArkInspectArgs {
    /// Input .ark path
    #[arg(long)]
    pub r#in: String,

    /// Also recompute recipe_id from decoded recipe and report match/mismatch
    #[arg(long, default_value_t = true)]
    pub verify_recipe_id: bool,

    /// If set, dump the ciphertext bytes to this file path
    #[arg(long)]
    pub dump_ciphertext: Option<String>,
}

pub fn run(args: ArkInspectArgs) -> anyhow::Result<()> {
    let (embedded_rid, recipe, data) = ark::read_ark_with_id(&args.r#in)?;

    eprintln!("--- ark-inspect ---");
    eprintln!("file              = {}", args.r#in);
    eprintln!("ark_ok             = true (magic + crc32 verified)");
    eprintln!("ciphertext_bytes   = {}", data.len());
    eprintln!("embedded_recipe_id = {}", embedded_rid);

    if args.verify_recipe_id {
        let recomputed = k8dnz_core::recipe::format::recipe_id_hex(&recipe);
        let ok = recomputed == embedded_rid;
        eprintln!("recomputed_recipe_id = {}", recomputed);
        eprintln!("recipe_id_match      = {}", ok);
        if !ok {
            eprintln!("WARNING: embedded recipe_id != recomputed recipe_id (should never happen)");
        }
    }

    // Print the key recipe knobs that matter for determinism/provenance.
    eprintln!("--- recipe ---");
    eprintln!("seed             = {}", recipe.seed);
    eprintln!(
        "field_clamp       = [{}, {}]",
        recipe.field_clamp.min, recipe.field_clamp.max
    );
    eprintln!(
        "quant             = min={} max={} shift={}",
        recipe.quant.min, recipe.quant.max, recipe.quant.shift
    );

    if let Some(out) = args.dump_ciphertext.as_deref() {
        std::fs::write(out, &data)?;
        eprintln!("dump_ciphertext    = {} ({} bytes)", out, data.len());
    }

    Ok(())
}
