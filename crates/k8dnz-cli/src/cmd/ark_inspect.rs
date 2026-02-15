// crates/k8dnz-cli/src/cmd/ark_inspect.rs

use clap::Args;
use std::io::Cursor;

use crate::io::ark;

#[derive(Args, Debug)]
pub struct ArkInspectArgs {
    /// Input .ark path
    #[arg(long)]
    pub r#in: String,

    /// Also recompute recipe_id from decoded recipe and report match/mismatch
    #[arg(long, default_value_t = true)]
    pub verify_recipe_id: bool,

    /// If set, dump the ciphertext/residual bytes to this file path
    #[arg(long)]
    pub dump_ciphertext: Option<String>,

    /// Also report zstd compressed size of the payload and “effective ratio”
    #[arg(long, default_value_t = true)]
    pub report_zstd: bool,

    /// Zstd compression level (1..=22 typical). Higher is slower.
    #[arg(long, default_value_t = 3)]
    pub zstd_level: i32,
}

pub fn run(args: ArkInspectArgs) -> anyhow::Result<()> {
    let meta_len = std::fs::metadata(&args.r#in).ok().map(|m| m.len());

    let (embedded_rid, recipe, data) = ark::read_ark_with_id(&args.r#in)?;

    eprintln!("--- ark-inspect ---");
    eprintln!("file              = {}", args.r#in);
    if let Some(n) = meta_len {
        eprintln!("ark_file_bytes     = {}", n);
    }
    eprintln!("ark_ok             = true (magic + crc32 verified)");
    eprintln!("data_bytes         = {}", data.len());
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

    eprintln!("--- recipe ---");
    eprintln!("version          = {}", recipe.version);
    eprintln!("seed             = {}", recipe.seed);
    eprintln!("keystream_mix    = {:?}", recipe.keystream_mix);
    eprintln!("payload_kind     = {:?}", recipe.payload_kind);
    eprintln!(
        "field_clamp       = [{}, {}]",
        recipe.field_clamp.min, recipe.field_clamp.max
    );
    eprintln!(
        "quant             = min={} max={} shift={}",
        recipe.quant.min, recipe.quant.max, recipe.quant.shift
    );

    if args.report_zstd {
        // Deterministic recipe byte size (what actually lives inside ARK)
        let recipe_bytes = k8dnz_core::recipe::format::encode(&recipe);
        let recipe_len = recipe_bytes.len();

        let z = zstd_size(&data, args.zstd_level)?;

        // For CipherXor / ResidualXor, payload bytes == original plaintext length.
        let plain_len = data.len();

        let effective = recipe_len + z;
        let ratio_eff = if effective == 0 {
            0.0
        } else {
            (plain_len as f64) / (effective as f64)
        };

        let ratio_payload = if z == 0 {
            0.0
        } else {
            (plain_len as f64) / (z as f64)
        };

        eprintln!("--- zstd scoreboard ---");
        eprintln!("zstd_level            = {}", args.zstd_level);
        eprintln!("recipe_bytes          = {}", recipe_len);
        eprintln!("payload_bytes         = {}", plain_len);
        eprintln!("payload_zstd_bytes    = {}", z);
        eprintln!(
            "effective_bytes       = {} (recipe + payload_zstd)",
            effective
        );
        eprintln!("ratio_payload/zstd    = {:.4}x", ratio_payload);
        eprintln!("ratio_plain/effective = {:.4}x", ratio_eff);
    }

    if let Some(out) = args.dump_ciphertext.as_deref() {
        std::fs::write(out, &data)?;
        eprintln!("dump_ciphertext    = {} ({} bytes)", out, data.len());
    }

    Ok(())
}

fn zstd_size(bytes: &[u8], level: i32) -> anyhow::Result<usize> {
    let out = zstd::stream::encode_all(Cursor::new(bytes), level)?;
    Ok(out.len())
}
