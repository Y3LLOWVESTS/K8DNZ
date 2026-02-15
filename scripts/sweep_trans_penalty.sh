set -euo pipefail

RECIPE="./configs/tuned_validated.k8r"
TARGET="text/Genesis1.txt"

OUTDIR="/tmp/k8dnz_tp_sweep"
mkdir -p "$OUTDIR"

BITS=1
MAP="bitfield"
BITMAP="geom"
RESID="sub"
OBJ="matches"
CHUNK=128
TOPK=2048
LOOK=200000
STEP=1
EMIT=2000000
TICKS=400000000
Z=3

CSV="$OUTDIR/results.csv"
if [ ! -f "$CSV" ]; then
  echo "ts,trans_penalty,tm1_zstd,resid_zstd,effective_no_recipe,effective_with_recipe,plain_zstd,flip_rate,run_count,median,p90,max,tm_path,bf1_path" > "$CSV"
fi

for TP in 0 1 2 4; do
  TM="$OUTDIR/g1_bf${BITS}_${RESID}_tp${TP}.tm1"
  BF="$OUTDIR/g1_bf${BITS}_${RESID}_tp${TP}.bf1"

  echo "=== trans_penalty=$TP ==="
  cargo run -p k8dnz-cli -- timemap fit-xor-chunked \
    --recipe "$RECIPE" \
    --target "$TARGET" \
    --out-timemap "$TM" \
    --out-residual "$BF" \
    --mode rgbpair \
    --map "$MAP" \
    --bits-per-emission "$BITS" \
    --bit-mapping "$BITMAP" \
    --residual "$RESID" \
    --objective "$OBJ" \
    --chunk-size "$CHUNK" \
    --refine-topk "$TOPK" \
    --lookahead "$LOOK" \
    --scan-step "$STEP" \
    --search-emissions "$EMIT" \
    --max-ticks "$TICKS" \
    --zstd-level "$Z" \
    --trans-penalty "$TP" \
    2> "$OUTDIR/run_tp${TP}.log" 1> /dev/null

  # Extract scoreboard numbers from the log (robust to spacing)
  plain_zstd=$(rg -m1 'plain_zstd_bytes\s*=\s*([0-9]+)' -or '$1' "$OUTDIR/run_tp${TP}.log")
  tm1_zstd=$(rg -m1 'tm1_zstd_bytes\s*=\s*([0-9]+)' -or '$1' "$OUTDIR/run_tp${TP}.log")
  resid_zstd=$(rg -m1 'resid_zstd_bytes\s*=\s*([0-9]+)' -or '$1' "$OUTDIR/run_tp${TP}.log")
  eff_no=$(rg -m1 'effective_bytes_no_recipe\s*=\s*([0-9]+)' -or '$1' "$OUTDIR/run_tp${TP}.log")
  eff_with=$(rg -m1 'effective_bytes_with_recipe\s*=\s*([0-9]+)' -or '$1' "$OUTDIR/run_tp${TP}.log")

  # Analyze BF1 bit flips + run lengths
  python3 - <<'PY' "$BF" "$Z" "$TP" "$tm1_zstd" "$resid_zstd" "$eff_no" "$eff_with" "$plain_zstd" "$TM" "$OUTDIR" "$CSV"
import sys, subprocess, time

bf = sys.argv[1]
z  = int(sys.argv[2])
tp = int(sys.argv[3])
tm1_zstd = int(sys.argv[4])
resid_zstd = int(sys.argv[5])
eff_no = int(sys.argv[6])
eff_with = int(sys.argv[7])
plain_zstd = int(sys.argv[8])
tm_path = sys.argv[9]
outdir = sys.argv[10]
csv = sys.argv[11]

b = open(bf,'rb').read()

# BF1 header is 24 bytes; payload begins at 24
payload = b[24:]
bits=[]
for byte in payload:
    for i in range(8):
        bits.append((byte>>i)&1)

if len(bits) < 2:
    flip_rate = 0.0
    flips = 0
else:
    flips=sum(bits[i]!=bits[i-1] for i in range(1,len(bits)))
    flip_rate=flips/(len(bits)-1)

runs=[]
cur=1
for i in range(1,len(bits)):
    if bits[i]==bits[i-1]:
        cur+=1
    else:
        runs.append(cur); cur=1
runs.append(cur)
runs.sort()

run_count=len(runs)
median=runs[run_count//2] if run_count else 0
p90=runs[int(run_count*0.9)] if run_count else 0
mx=runs[-1] if run_count else 0

ts = time.strftime("%Y-%m-%dT%H:%M:%S")

row = f"{ts},{tp},{tm1_zstd},{resid_zstd},{eff_no},{eff_with},{plain_zstd},{flip_rate:.6f},{run_count},{median},{p90},{mx},{tm_path},{bf}\n"
open(csv,"a").write(row)
print(row, end="")
PY

done

echo "Wrote: $CSV"
