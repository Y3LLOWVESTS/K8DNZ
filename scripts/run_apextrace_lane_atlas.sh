#!/usr/bin/env bash
set -euo pipefail

cargo build
cargo run -p k8dnz-cli --bin k8dnz-cli -- apextrace apex-lane-atlas \
  --recipe configs/tuned_validated.k8r \
  --in text/Genesis1.txt \
  --window-bytes 256 \
  --step-bytes 128 \
  --max-windows 12 \
  --seed-count 64 \
  --chunk-sweep 32,64 \
  --boundary-band-sweep 8,12 \
  --field-margin-sweep 4,8 \
  --newline-demote-margin-sweep 0,4 \
  --format txt
