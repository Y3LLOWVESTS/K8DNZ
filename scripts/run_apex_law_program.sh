set -euo pipefail

MODE="${1:-quick}"
: "${K8DNZ_LOCAL_MIX_JOBS:=2}"
: "${K8DNZ_APEX_MAP_SWEEP_JOBS:=2}"
: "${K8DNZ_REPLAY_JOBS:=4}"
: "${K8DNZ_SURFACE_JOBS:=3}"
: "${K8DNZ_BODY_VALIDATE_TOP_K:=1}"
: "${K8DNZ_COMPARE_SURFACES_ON_REPLAY:=0}"

cargo build -p k8dnz-cli --bin k8dnz-cli --bin apex_law_program --bin apex_law_program_audit
cargo test -p k8dnz-cli --bin apex_law_program
cargo test -p k8dnz-cli --bin apex_law_program_audit

LAW_BIN="target/debug/apex_law_program"
AUDIT_BIN="target/debug/apex_law_program_audit"

build_case() {
  local label="$1"
  shift

  local artifact="/tmp/${label}.aklp"
  local build_report="/tmp/${label}_build.txt"
  local replay_report="/tmp/${label}_replay.txt"

  echo "== build_case ${label}: build =="
  "$LAW_BIN" build "$@" --out "$artifact" --out-report "$build_report"

  echo "== build_case ${label}: replay =="
  if [ "$K8DNZ_COMPARE_SURFACES_ON_REPLAY" = "1" ]; then
    "$LAW_BIN" replay --artifact "$artifact" --compare-surfaces --out-report "$replay_report"
  else
    "$LAW_BIN" replay --artifact "$artifact" --out-report "$replay_report"
  fi

  echo "built_case=${label}"
  echo "artifact=${artifact}"
  echo "build_report=${build_report}"
  echo "replay_report=${replay_report}"
  echo
}

COMMON_G12=(
  --recipe configs/tuned_validated.k8r
  --in text/Genesis1.txt
  --in text/Genesis2.txt
  --window-bytes 256
  --step-bytes 256
  --max-windows 12
  --seed-count 64
  --chunk-sweep 32,64
  --boundary-band-sweep 8,12
  --field-margin-sweep 4,8
  --newline-demote-margin-sweep 0,4
  --local-chunk-sweep 32,64,96,128
  --tune-default-body
  --default-body-chunk-sweep 64,96,128
  --emit-body-scoreboard
  --min-override-gain-exact 1
  --exact-subset-limit 12
)

COMMON_G1=(
  --recipe configs/tuned_validated.k8r
  --in text/Genesis1.txt
  --window-bytes 256
  --step-bytes 256
  --max-windows 12
  --seed-count 64
  --chunk-sweep 32,64
  --boundary-band-sweep 8,12
  --field-margin-sweep 4,8
  --newline-demote-margin-sweep 0,4
  --local-chunk-sweep 32,64,96,128
  --tune-default-body
  --default-body-chunk-sweep 64,96,128
  --emit-body-scoreboard
  --min-override-gain-exact 1
  --exact-subset-limit 12
)

build_case g12_codec_selected_total "${COMMON_G12[@]}" --body-select-objective selected-total
build_case g12_closure_default_total "${COMMON_G12[@]}" --body-select-objective default-total
build_case g1_codec_selected_total "${COMMON_G1[@]}" --body-select-objective selected-total
build_case g1_closure_default_total "${COMMON_G1[@]}" --body-select-objective default-total

if [ "$MODE" = "full" ]; then
  build_case g12_frontier_selected_target "${COMMON_G12[@]}" --body-select-objective selected-target
  build_case g1_frontier_selected_target "${COMMON_G1[@]}" --body-select-objective selected-target
fi

AUDIT_ARGS=(
  --case "g12_codec_selected_total|/tmp/g12_codec_selected_total_build.txt|/tmp/g12_codec_selected_total_replay.txt"
  --case "g12_closure_default_total|/tmp/g12_closure_default_total_build.txt|/tmp/g12_closure_default_total_replay.txt"
  --case "g1_codec_selected_total|/tmp/g1_codec_selected_total_build.txt|/tmp/g1_codec_selected_total_replay.txt"
  --case "g1_closure_default_total|/tmp/g1_closure_default_total_build.txt|/tmp/g1_closure_default_total_replay.txt"
)

if [ "$MODE" = "full" ]; then
  AUDIT_ARGS+=(
    --case "g12_frontier_selected_target|/tmp/g12_frontier_selected_target_build.txt|/tmp/g12_frontier_selected_target_replay.txt"
    --case "g1_frontier_selected_target|/tmp/g1_frontier_selected_target_build.txt|/tmp/g1_frontier_selected_target_replay.txt"
  )
fi

"$AUDIT_BIN" "${AUDIT_ARGS[@]}" > /tmp/apex_law_program_audit.txt
cat /tmp/apex_law_program_audit.txt
