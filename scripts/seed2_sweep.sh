#!/usr/bin/env bash
set -euo pipefail

RECIPE="./configs/tuned_validated.k8r"
TARGET="text/Genesis1.txt"
MAP="text40-field"

BASE="0x0000456789abcdef"
BASE16=${BASE#0x}
TAIL12=${BASE16: -12}

# ---- safety: prevent accidental double-run (looks like "looping") ----
LOCKDIR="/tmp/seed2_sweep.lock"
if ! mkdir "$LOCKDIR" 2>/dev/null; then
  echo "seed2_sweep: already running (lock exists at $LOCKDIR)."
  echo "If you're sure nothing is running: rm -rf $LOCKDIR"
  exit 1
fi
cleanup() { rm -rf "$LOCKDIR"; }
trap cleanup EXIT INT TERM

# ---- optional: per-run log file (still prints to stdout) ----
RUN_TS="$(date +%Y%m%d_%H%M%S)"
LOG="/tmp/seed2_sweep_${RUN_TS}.log"

echo "seed2_sweep START $(date)"
echo "RECIPE=$RECIPE"
echo "TARGET=$TARGET"
echo "MAP=$MAP"
echo "BASE=$BASE  (TAIL12=$TAIL12)"
echo "LOG=$LOG"
echo

# Sweep B1 and B2 but keep tail constant
B1_LIST=(00 10 20 30 40 50 60 70 80 90 a0 b0 c0 d0 e0 f0)
B2_LIST=(00 40 80 c0 ff)

total=$(( ${#B1_LIST[@]} * ${#B2_LIST[@]} ))
i=0

for B1 in "${B1_LIST[@]}"; do
  for B2 in "${B2_LIST[@]}"; do
    i=$((i+1))
    SEED_HEX="0x${B1}${B2}${TAIL12}"

    echo "[$i/$total] === SEED=$SEED_HEX ===" | tee -a "$LOG"

    # Run and filter the interesting lines; never fail the whole sweep just because grep finds nothing.
    # Also tee raw output to the log so you can inspect any oddities.
    cargo run -p k8dnz-cli -- timemap fit-xor-chunked \
      --recipe "$RECIPE" \
      --target "$TARGET" \
      --map "$MAP" \
      --map-seed-hex "$SEED_HEX" \
      --objective matches \
      --chunk-size 128 \
      --refine-topk 2048 \
      --scan-step 1 \
      --search-emissions 2000000 \
      --max-ticks 20000000 \
      --lookahead 1200000 \
      --out-timemap "/tmp/seed2_${B1}_${B2}.tm1" \
      --out-residual "/tmp/seed2_${B1}_${B2}.res" \
    2>&1 | tee -a "$LOG" \
      | grep -E "map_seed=|matches=|^--- scoreboard ---$|tm1_zstd_bytes|resid_zstd_bytes|effective_bytes_no_recipe" || true

    echo | tee -a "$LOG"
  done
done

echo "seed2_sweep END $(date)  (log: $LOG)"
