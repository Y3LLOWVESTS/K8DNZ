use clap::{Args, Subcommand};
use k8dnz_core::recipe::ark_key::{decode_ark1s, encode_ark1s};

use crate::io::recipe_file;

#[derive(Args)]
pub struct ArkKeyArgs {
    #[command(subcommand)]
    pub cmd: ArkKeyCmd,
}

#[derive(Subcommand)]
pub enum ArkKeyCmd {
    FromRecipe(FromRecipeArgs),
    ToRecipe(ToRecipeArgs),
}

#[derive(Args)]
pub struct FromRecipeArgs {
    #[arg(long)]
    pub recipe: String,
}

#[derive(Args)]
pub struct ToRecipeArgs {
    #[arg(long)]
    pub ark: String,

    #[arg(long)]
    pub out: String,
}

pub fn run(args: ArkKeyArgs) -> anyhow::Result<()> {
    match args.cmd {
        ArkKeyCmd::FromRecipe(a) => {
            let r = recipe_file::load_k8r(&a.recipe)?;
            let s = encode_ark1s(&r);
            println!("{s}");
            Ok(())
        }
        ArkKeyCmd::ToRecipe(a) => {
            let r = decode_ark1s(&a.ark).map_err(|e| anyhow::anyhow!("{e}"))?;
            recipe_file::save_k8r(&a.out, &r)?;
            eprintln!("arkkey ok: out={}", a.out);
            Ok(())
        }
    }
}
