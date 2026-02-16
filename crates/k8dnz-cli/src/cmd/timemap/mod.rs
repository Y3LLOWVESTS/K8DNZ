// crates/k8dnz-cli/src/cmd/timemap/mod.rs

pub mod args;
mod bf_lanes;
mod bitfield;
mod byte_pipeline;
mod gen_law; // NEW
mod mapping;
mod residual;
mod tags;
mod util;

pub use args::TimemapArgs;

pub fn run(args: TimemapArgs) -> anyhow::Result<()> {
    use args::TimemapCmd::*;

    match args.cmd {
        Make(a) => byte_pipeline::cmd_make(a),
        Inspect(a) => byte_pipeline::cmd_inspect(a),
        MapSeed(a) => byte_pipeline::cmd_map_seed(a),
        Apply(a) => byte_pipeline::cmd_apply(a),
        Fit(a) => byte_pipeline::cmd_fit(a),
        FitXor(a) => byte_pipeline::cmd_fit_xor(a),
        FitXorChunked(a) => {
            if a.map == args::MapMode::Bitfield {
                bitfield::cmd_fit_xor_chunked_bitfield(a)
            } else {
                byte_pipeline::cmd_fit_xor_chunked(a)
            }
        }
        Reconstruct(a) => {
            if a.map == args::MapMode::Bitfield {
                bitfield::cmd_reconstruct_bitfield(a)
            } else {
                byte_pipeline::cmd_reconstruct(a)
            }
        }
        GenLaw(a) => gen_law::cmd_gen_law(a),
        BfLanes(a) => bf_lanes::cmd_bf_lanes(a),
    }
}
