// crates/k8dnz-core/src/signal/bitpack.rs

use crate::error::{K8Error, Result};

const MAX_BITS: u8 = 8;

/// Pack `symbols` where each symbol occupies exactly `bits_per_symbol` bits.
///
/// Bit order is MSB-first within the packed byte stream:
/// - The first bit written becomes the MSB of output[0].
/// - Bits flow left-to-right, byte by byte.
///
/// Requirements:
/// - `bits_per_symbol` must be in 1..=8.
/// - Each symbol must be <= (1<<bits_per_symbol)-1.
pub fn pack_symbols(bits_per_symbol: u8, symbols: &[u8]) -> Result<Vec<u8>> {
    validate_bits(bits_per_symbol)?;
    let mask: u8 = ((1u16 << bits_per_symbol) - 1) as u8;

    let total_bits: usize = (symbols.len())
        .checked_mul(bits_per_symbol as usize)
        .ok_or_else(|| K8Error::Validation("pack_symbols overflow".into()))?;

    let out_len: usize = (total_bits + 7) / 8;
    let mut out = vec![0u8; out_len];

    let mut bit_cursor: usize = 0;
    for &sym in symbols.iter() {
        if sym & !mask != 0 {
            return Err(K8Error::Validation(format!(
                "symbol out of range: sym={} bits_per_symbol={} mask=0x{:02x}",
                sym, bits_per_symbol, mask
            )));
        }

        // write sym bits from MSB to LSB within the symbol width
        for b in (0..bits_per_symbol).rev() {
            let bit = (sym >> b) & 1;
            let byte_idx = bit_cursor / 8;
            let bit_in_byte = bit_cursor % 8;
            // MSB-first in byte: bit_in_byte=0 -> 0x80
            if bit == 1 {
                out[byte_idx] |= 1u8 << (7 - bit_in_byte);
            }
            bit_cursor += 1;
        }
    }

    Ok(out)
}

/// Unpack `symbol_count` symbols, each `bits_per_symbol` bits, from a packed MSB-first bitstream.
///
/// This is the inverse of `pack_symbols` when the same `(bits_per_symbol, symbol_count)` is used.
///
/// Requirements:
/// - `bits_per_symbol` must be in 1..=8.
/// - `packed` must contain enough bits for `symbol_count` symbols.
pub fn unpack_symbols(bits_per_symbol: u8, packed: &[u8], symbol_count: usize) -> Result<Vec<u8>> {
    validate_bits(bits_per_symbol)?;

    let total_bits: usize = symbol_count
        .checked_mul(bits_per_symbol as usize)
        .ok_or_else(|| K8Error::Validation("unpack_symbols overflow".into()))?;
    let need_bytes: usize = (total_bits + 7) / 8;

    if packed.len() < need_bytes {
        return Err(K8Error::Validation(format!(
            "unpack_symbols short: need {} bytes for {} symbols ({} bits/sym), got {}",
            need_bytes,
            symbol_count,
            bits_per_symbol,
            packed.len()
        )));
    }

    let mut out = Vec::with_capacity(symbol_count);

    let mut bit_cursor: usize = 0;
    for _ in 0..symbol_count {
        let mut sym: u8 = 0;
        for _ in 0..bits_per_symbol {
            let byte_idx = bit_cursor / 8;
            let bit_in_byte = bit_cursor % 8;
            let bit = (packed[byte_idx] >> (7 - bit_in_byte)) & 1;
            sym = (sym << 1) | bit;
            bit_cursor += 1;
        }
        out.push(sym);
    }

    Ok(out)
}

#[inline]
fn validate_bits(bits_per_symbol: u8) -> Result<()> {
    if bits_per_symbol == 0 || bits_per_symbol > MAX_BITS {
        return Err(K8Error::Validation(format!(
            "bits_per_symbol must be in 1..=8, got {}",
            bits_per_symbol
        )));
    }
    Ok(())
}
