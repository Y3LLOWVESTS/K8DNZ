#!/usr/bin/env bash
set -euo pipefail

cargo build
cargo test -p k8dnz-cli

cargo run -p k8dnz-cli --bin k8dnz-cli -- apextrace apex-punct-kind-atlas --in text/Genesis1.txt --window-bytes 256 --step-bytes 256 --max-windows 12 --seed-count 64 --chunk-sweep 32,64 --boundary-band-sweep 8,12 --field-margin-sweep 4,8 --format txt --out /tmp/apex_punct_kind_atlas_g1.txt
cat /tmp/apex_punct_kind_atlas_g1.txt

cargo run -p k8dnz-cli --bin k8dnz-cli -- apextrace apex-punct-kind-atlas --in text/Genesis2.txt --window-bytes 256 --step-bytes 256 --max-windows 12 --seed-count 64 --chunk-sweep 32,64 --boundary-band-sweep 8,12 --field-margin-sweep 4,8 --format txt --out /tmp/apex_punct_kind_atlas_g2.txt
cat /tmp/apex_punct_kind_atlas_g2.txt
