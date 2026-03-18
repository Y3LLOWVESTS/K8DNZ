# K8DNZ chunked ws-lane runbook

## Build

cargo build
cargo test -p k8dnz-apextrace

## Reproduce current global baseline

cargo run -p k8dnz-cli --bin k8dnz-cli -- apextrace ws-lane --recipe ./configs/tuned_validated.k8r --in text/Genesis1.txt --seed-count 512

## First chunked comparison pass

cargo run -p k8dnz-cli --bin k8dnz-cli -- apextrace ws-lane --recipe ./configs/tuned_validated.k8r --in text/Genesis1.txt --seed-count 512 --chunk-bytes 256

## Sweep a few chunk sizes

cargo run -p k8dnz-cli --bin k8dnz-cli -- apextrace ws-lane --recipe ./configs/tuned_validated.k8r --in text/Genesis1.txt --seed-count 512 --chunk-bytes 64
cargo run -p k8dnz-cli --bin k8dnz-cli -- apextrace ws-lane --recipe ./configs/tuned_validated.k8r --in text/Genesis1.txt --seed-count 512 --chunk-bytes 128
cargo run -p k8dnz-cli --bin k8dnz-cli -- apextrace ws-lane --recipe ./configs/tuned_validated.k8r --in text/Genesis1.txt --seed-count 512 --chunk-bytes 256
cargo run -p k8dnz-cli --bin k8dnz-cli -- apextrace ws-lane --recipe ./configs/tuned_validated.k8r --in text/Genesis1.txt --seed-count 512 --chunk-bytes 512

## Save global key, chunk manifest, and predicted outputs

cargo run -p k8dnz-cli --bin k8dnz-cli -- apextrace ws-lane --recipe ./configs/tuned_validated.k8r --in text/Genesis1.txt --seed-count 512 --chunk-bytes 256 --out-key /tmp/ws_lane_global.atk --out-pred /tmp/ws_lane_global.txt --out /tmp/ws_lane_report.txt

## What the updated command now reports

- baseline K8L1 class patch bytes
- global ApexTrace class patch bytes
- chunked ApexTrace class patch bytes
- delta chunked vs baseline
- delta chunked vs global
- estimated chunk key bytes
- per-chunk seed/quadrant summaries in txt output
- optional chunk manifest at PATH.chunks.csv when --out-key is used
