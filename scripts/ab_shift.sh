#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 5 ]]; then
  echo "usage: scripts/ab_shift.sh <recipe.k8r> <target> <base_seed_u64> <shift0_u8> <shift1_u8>" >&2
  exit 2
fi

RECIPE="$1"
TARGET="$2"
BASE_IN="$3"
SHIFT0="$4"
SHIFT1="$5"

to_lower() {
  echo "$1" | tr '[:upper:]' '[:lower:]'
}

to_hex16() {
  local s="$1"
  local t
  t="$(to_lower "$s")"
  if [[ "$t" == 0x* ]]; then
    t="${t#0x}"
    t="$(echo "$t" | tr -d '_')"
    printf "%016s" "$t" | tr ' ' '0'
    return 0
  fi
  if echo "$t" | grep -Eq '[a-f]'; then
    t="$(echo "$t" | tr -d '_')"
    printf "%016s" "$t" | tr ' ' '0'
    return 0
  fi
  printf "%016x" "$s"
}

byte_hex() {
  local n="$1"
  if ! [[ "$n" =~ ^[0-9]+$ ]]; then
    echo "shift byte must be 0..255, got: $n" >&2
    exit 2
  fi
  if (( n < 0 || n > 255 )); then
    echo "shift byte must be 0..255, got: $n" >&2
    exit 2
  fi
  printf "%02x" "$n"
}

BASE_HEX="$(to_hex16 "$BASE_IN")"
S0="$(byte_hex "$SHIFT0")"
S1="$(byte_hex "$SHIFT1")"

SEED0_HEX="0x${S0}${BASE_HEX:2}"
SEED1_HEX="0x${S1}${BASE_HEX:2}"

echo "base_seed_in=$BASE_IN"
echo "base_hex=0x$BASE_HEX"
echo "seed0_hex=$SEED0_HEX shift0=$SHIFT0"
echo "seed1_hex=$SEED1_HEX shift1=$SHIFT1"

cargo build >/dev/null

target/debug/k8dnz-cli timemap map-seed --map text40-field --map-seed-hex "$SEED0_HEX"
target/debug/k8dnz-cli timemap map-seed --map text40-field --map-seed-hex "$SEED1_HEX"

OUT0_TM="/tmp/ab_shift_s${SHIFT0}.tm1"
OUT0_RS="/tmp/ab_shift_s${SHIFT0}.resid"
OUT0_RC="/tmp/ab_shift_s${SHIFT0}.recon"

OUT1_TM="/tmp/ab_shift_s${SHIFT1}.tm1"
OUT1_RS="/tmp/ab_shift_s${SHIFT1}.resid"
OUT1_RC="/tmp/ab_shift_s${SHIFT1}.recon"

target/debug/k8dnz-cli timemap fit-xor-chunked \
  --recipe "$RECIPE" \
  --target "$TARGET" \
  --out-timemap "$OUT0_TM" \
  --out-residual "$OUT0_RS" \
  --mode pair \
  --map text40-field \
  --map-seed-hex "$SEED0_HEX" \
  --residual xor \
  --objective zstd \
  --zstd-level 3 \
  --chunk-size 512 \
  --scan-step 1 \
  --refine-topk 256 \
  --lookahead 200000 \
  --start-emission 0 \
  --search-emissions 400000 \
  --max-ticks 320000000

target/debug/k8dnz-cli timemap reconstruct \
  --recipe "$RECIPE" \
  --timemap "$OUT0_TM" \
  --residual "$OUT0_RS" \
  --out "$OUT0_RC" \
  --mode pair \
  --map text40-field \
  --map-seed-hex "$SEED0_HEX" \
  --residual-mode xor \
  --max-ticks 320000000

cmp -s "$OUT0_RC" "$TARGET" && echo "A_OK shift=$SHIFT0" || echo "A_FAIL shift=$SHIFT0"

target/debug/k8dnz-cli timemap fit-xor-chunked \
  --recipe "$RECIPE" \
  --target "$TARGET" \
  --out-timemap "$OUT1_TM" \
  --out-residual "$OUT1_RS" \
  --mode pair \
  --map text40-field \
  --map-seed-hex "$SEED1_HEX" \
  --residual xor \
  --objective zstd \
  --zstd-level 3 \
  --chunk-size 512 \
  --scan-step 1 \
  --refine-topk 256 \
  --lookahead 200000 \
  --start-emission 0 \
  --search-emissions 400000 \
  --max-ticks 320000000

target/debug/k8dnz-cli timemap reconstruct \
  --recipe "$RECIPE" \
  --timemap "$OUT1_TM" \
  --residual "$OUT1_RS" \
  --out "$OUT1_RC" \
  --mode pair \
  --map text40-field \
  --map-seed-hex "$SEED1_HEX" \
  --residual-mode xor \
  --max-ticks 320000000

cmp -s "$OUT1_RC" "$TARGET" && echo "B_OK shift=$SHIFT1" || echo "B_FAIL shift=$SHIFT1"

echo "outputs:"
echo "$OUT0_TM"
echo "$OUT0_RS"
echo "$OUT1_TM"
echo "$OUT1_RS"
