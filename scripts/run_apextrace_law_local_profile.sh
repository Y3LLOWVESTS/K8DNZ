#!/usr/bin/env bash
set -euo pipefail

cargo build
cargo test -p k8dnz-cli

cargo run -p k8dnz-cli --bin k8dnz-cli -- apextrace apex-lane-law-local-profile --recipe configs/tuned_validated.k8r --in text/Genesis1.txt --in text/Genesis2.txt --window-bytes 256 --step-bytes 256 --max-windows 12 --seed-count 64 --chunk-sweep 32,64 --boundary-band-sweep 8,12 --field-margin-sweep 4,8 --newline-demote-margin-sweep 0,4 --local-chunk-sweep 32,64,96,128 --top-rows 12 --format txt --out /tmp/apex_lane_law_local_profile_g12.txt
cat /tmp/apex_lane_law_local_profile_g12.txt

cargo run -p k8dnz-cli --bin k8dnz-cli -- apextrace apex-lane-law-local-profile --recipe configs/tuned_validated.k8r --in text/Genesis1.txt --window-bytes 256 --step-bytes 256 --max-windows 12 --seed-count 64 --chunk-sweep 32,64 --boundary-band-sweep 8,12 --field-margin-sweep 4,8 --newline-demote-margin-sweep 0,4 --local-chunk-sweep 32,64,96,128 --top-rows 12 --format txt --out /tmp/apex_lane_law_local_profile_g1.txt
cat /tmp/apex_lane_law_local_profile_g1.txt
