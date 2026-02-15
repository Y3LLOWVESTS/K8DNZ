// crates/k8dnz-cli/src/cmd/timemap/bf_reconstruct.rs

use std::fs;
use std::io;

use k8dnz_core::Engine;

use crate::io::{recipe_file, timemap};

use super::args::BfReconstructArgs;
use super::bitfield::{read_bf_any, map_to_symbols};
use super::residual::{apply_residual_symbol, sym_mask};

fn ensure_stream_len(
    eng: &mut Engine,
    stream: &mut Vec<u8>,
    needed_len: usize,
    max_ticks: u64,
    mode: super::args::ApplyMode,
) -> io::Result<()> {
    while stream.len() < needed_len {
        let tok = eng.step(max_ticks);
        match mode {
            super::args::ApplyMode::Pair => stream.push(tok.pair.0),
            super::args::ApplyMode::Rgbpair => stream.extend_from_slice(&[
                tok.rgbpair.0 .0,
                tok.rgbpair.0 .1,
                tok.rgbpair.0 .2,
                tok.rgbpair.1 .0,
                tok.rgbpair.1 .1,
                tok.rgbpair.1 .2,
            ]),
        }
    }
    Ok(())
}

pub fn cmd_bf_reconstruct(a: BfReconstructArgs) -> io::Result<()> {
    let recipe = recipe_file::read(&a.recipe)?;
    let tm = timemap::read_tm1(&a.timemap)?;

    let indices = tm.indices();
    if indices.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "tm1 is empty"));
    }
    let base = indices[0] as usize;
    // Require contiguous for now (that’s what fit-xor-chunked writes)
    for (i, &idx) in indices.iter().enumerate() {
        if idx as usize != base + i {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bf-reconstruct currently requires contiguous tm1",
            ));
        }
    }
    let n = indices.len();

    let (h, resid_syms) = read_bf_any(&a.residual)?;
    if resid_syms.len() != n {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "residual symbol count mismatch: bf has {}, tm1 has {}",
                resid_syms.len(),
                n
            ),
        ));
    }

    // Generate model stream up to base+n
    let mut eng = Engine::from_recipe(recipe);
    let mut stream: Vec<u8> = Vec::with_capacity(base + n);
    ensure_stream_len(&mut eng, &mut stream, base + n, a.max_ticks, h.apply_mode)?;

    let win = &stream[base..base + n];
    // For decode we use the same mapping as fit used (bit_mapping is embedded in the mapping function in bitfield.rs)
    // NOTE: read_bf_any stores the header mapping seed strategy but the actual bit extraction is independent.
    // If you add more bit mapping variants later, store it in BF headers too.
    let model_syms = map_to_symbols(win, h.bits_per_emission, super::args::BitMapping::Geom);

    let mask = sym_mask(h.bits_per_emission);

    let mut out = vec![0u8; n];
    for i in 0..n {
        let m = model_syms[i] & mask;
        let r = resid_syms[i] & mask;
        let plain = apply_residual_symbol(h.residual_mode, m, r, mask);
        out[i] = plain; // already a symbol-in-low-bits byte
    }

    fs::write(&a.out, &out)?;
    eprintln!(
        "[bf-reconstruct] wrote {} bytes to {:?} (bf_version={}, bits={}, residual_mode={:?}, apply_mode={:?})",
        out.len(),
        a.out,
        h.version,
        h.bits_per_emission,
        h.residual_mode,
        h.apply_mode
    );

    Ok(())
}
