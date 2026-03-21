#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ]; then
  echo "usage: bash scripts/run_apextrace_dual_genesis.sh <apextrace-subcommand> [args...]" >&2
  exit 1
fi

for arg in "$@"; do
  case "$arg" in
    --in|--out|--out-pred|--out-key|--out-diag)
      echo "do not pass --in/--out flags to run_apextrace_dual_genesis.sh" >&2
      exit 1
      ;;
  esac
done

subcmd="$1"
shift

for target in text/Genesis1.txt text/Genesis2.txt; do
  if [ ! -f "$target" ]; then
    echo "missing target: $target" >&2
    exit 1
  fi
done

for target in text/Genesis1.txt text/Genesis2.txt; do
  echo
  echo "===== APEXTRACE $subcmd :: $target ====="
  cargo run -p k8dnz-cli --bin k8dnz-cli -- apextrace "$subcmd" --in "$target" "$@"
done