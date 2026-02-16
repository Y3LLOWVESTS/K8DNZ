// crates/k8dnz-cli/src/cmd/timemap/bf_reconstruct.rs

use std::fs;
use std::io;

use k8dnz_core::signal::bitpack;
use k8dnz_core::Engine;

use crate::io::{recipe_file, timemap};

use super::args::BfReconstructArgs;
use super::bitfield::{map_to_symbols, read_bf_any};
use super::residual::{apply_residual_symbol, sym_mask};

fn io_err(kind: io::ErrorKind, msg: impl Into<String>) -> io::Error {
    io::Error::new(kind, msg.into())
}

fn ensure_stream_len(
    eng: &mut Engine,
    stream: &mut Vec<u8>,
    needed_len: usize,
    max_ticks: u64,
    mode: super::args::ApplyMode,
) -> io::Result<()> {
    while stream.len() < needed_len {
        if eng.stats.ticks >= max_ticks {
            return Err(io_err(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "bf-reconstruct: max_ticks reached before model stream ready (have={} need={} ticks={} emissions={})",
                    stream.len(),
                    needed_len,
                    eng.stats.ticks,
                    eng.stats.emissions
                ),
            ));
        }

        if let Some(tok) = eng.step() {
            match mode {
                super::args::ApplyMode::Pair => {
                    stream.push(tok.pack_byte());
                }
                super::args::ApplyMode::Rgbpair => {
                    stream.extend_from_slice(&tok.to_rgb_pair().to_bytes());
                }
            }
        }
    }
    Ok(())
}

pub fn cmd_bf_reconstruct(a: BfReconstructArgs) -> io::Result<()> {
    // NOTE: keep io::Result signature, but load_k8r/read_tm1 return anyhow::Result in this codebase.
    let recipe = recipe_file::load_k8r(&a.recipe)
        .map_err(|e| io_err(io::ErrorKind::InvalidData, format!("read recipe: {e}")))?;
    let tm = timemap::read_tm1(&a.timemap)
        .map_err(|e| io_err(io::ErrorKind::InvalidData, format!("read timemap: {e}")))?;

    if tm.indices.is_empty() {
        return Err(io_err(io::ErrorKind::InvalidData, "tm1 is empty"));
    }

    let indices = &tm.indices;

    // Require contiguous for now (this path is intended for the contiguous window / law-driven cases).
    let base = indices[0] as usize;
    for (i, &idx) in indices.iter().enumerate() {
        if idx as usize != base + i {
            return Err(io_err(
                io::ErrorKind::InvalidData,
                "bf-reconstruct currently requires contiguous timemap indices",
            ));
        }
    }
    let n_syms = indices.len();

    let (h, resid_syms) =
        read_bf_any(&a.residual).map_err(|e| io_err(io::ErrorKind::InvalidData, format!("{e}")))?;
    if resid_syms.len() != n_syms {
        return Err(io_err(
            io::ErrorKind::InvalidData,
            format!(
                "residual symbol count mismatch: bf has {}, timemap has {}",
                resid_syms.len(),
                n_syms
            ),
        ));
    }

    // Generate model stream up to base+n_syms (bytes)
    let mut eng =
        Engine::new(recipe).map_err(|e| io_err(io::ErrorKind::InvalidData, format!("{e}")))?;
    let mut stream: Vec<u8> = Vec::with_capacity(base + n_syms);
    ensure_stream_len(&mut eng, &mut stream, base + n_syms, a.max_ticks, h.apply_mode)?;

    let win = &stream[base..base + n_syms];

    // IMPORTANT: decode must use the same bit mapping used during fit/gen-law.
    // We assume the BF header includes this (h.bit_mapping). If your header field name differs,
    // rename here to match (e.g., h.mapping).
    let model_syms = map_to_symbols(win, h.bits_per_emission, h.bit_mapping);

    let mask = sym_mask(h.bits_per_emission);

    // Recover plaintext symbols.
    let mut plain_syms: Vec<u8> = vec![0u8; n_syms];
    for i in 0..n_syms {
        let m = model_syms[i] & mask;
        let r = resid_syms[i] & mask;
        let plain = apply_residual_symbol(h.residual_mode, m, r, mask);
        plain_syms[i] = plain & mask;
    }

    // Pack symbols back into bytes and truncate to original byte length.
    let mut out_bytes =
        bitpack::pack_symbols(h.bits_per_emission, &plain_syms).map_err(|e| {
            io_err(
                io::ErrorKind::InvalidData,
                format!("pack_symbols failed: {e}"),
            )
        })?;
    if h.orig_len_bytes <= out_bytes.len() {
        out_bytes.truncate(h.orig_len_bytes);
    }

    fs::write(&a.out, &out_bytes)?;
    eprintln!(
        "[bf-reconstruct] wrote {} bytes to {:?} (bf_version={}, orig_len_bytes={}, symbols={}, bits={}, bit_mapping={:?}, residual_mode={:?}, apply_mode={:?})",
        out_bytes.len(),
        a.out,
        h.version,
        h.orig_len_bytes,
        n_syms,
        h.bits_per_emission,
        h.bit_mapping,
        h.residual_mode,
        h.apply_mode
    );

    Ok(())
}
