#!/usr/bin/env bash
set -euo pipefail

BASE="${BASE:-0x00456789abcdef}"   # or set BASE before running
BASE16="${BASE#0x}"

# last 12 hex digits (lower 6 bytes)
TAIL12="${BASE16: -12}"

for AMP in 0 16 32 48 64 80 96 112 128 144 160 176 192 208 224 240 255; do
  AMP_HEX=$(printf "%02x" "$AMP")
  SEED_HEX="0x00${AMP_HEX}${TAIL12}"

  echo "=== AMP=$AMP SEED=$SEED_HEX ==="
  cargo run -p k8dnz-cli -- timemap fit-xor-chunked \
    --recipe ./configs/tuned_validated.k8r \
    --target text/Genesis1.txt \
    --map text40-field \
    --map-seed-hex "$SEED_HEX" \
    --objective zstd \
    --chunk-size 512 \
    --refine-topk 256 \
    --scan-step 1 \
    --search-emissions 250000 \
    --max-ticks 20000000 \
    --lookahead 200000 \
    --out-timemap "/tmp/amp_${AMP}.tm1" \
    --out-residual "/tmp/amp_${AMP}.res" \
  | egrep "map_seed=|effective_bytes_no_recipe|tm1_zstd_bytes|resid_zstd_bytes"
done
