#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-full}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVER_DIR="$ROOT_DIR/rust/server"
BENCH_ROOT="$ROOT_DIR/.benchmarks"
LATEST_DIR="$BENCH_ROOT/latest"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$(git -C "$ROOT_DIR" rev-parse --short HEAD 2>/dev/null || printf unknown)"
RUN_DIR="$BENCH_ROOT/runs/$RUN_ID"

mkdir -p "$LATEST_DIR" "$BENCH_ROOT/runs" "$BENCH_ROOT/baseline"

if [[ "$MODE" == "baseline" ]]; then
  cp "$LATEST_DIR/result.json" "$BENCH_ROOT/baseline/result.json"
  cp "$LATEST_DIR/metrics.csv" "$BENCH_ROOT/baseline/metrics.csv"
  cp "$LATEST_DIR/summary.md" "$BENCH_ROOT/baseline/summary.md"
  exit 0
fi

GIT_SHA="$(git -C "$ROOT_DIR" rev-parse --short HEAD 2>/dev/null || printf unknown)"
DIRTY=false
if [[ -n "$(git -C "$ROOT_DIR" status --short)" ]]; then
  DIRTY=true
fi

DIRTY_ARG=()
if [[ "$DIRTY" == true ]]; then
  DIRTY_ARG=(--dirty)
fi

pushd "$SERVER_DIR" >/dev/null
cargo build --release --bin kafkalite --bin bench_runner
BROKER_BIN="$SERVER_DIR/target/release/kafkalite"
BENCH_BIN="$SERVER_DIR/target/release/bench_runner"
BINARY_BYTES="$(stat -c %s "$BROKER_BIN")"
PACKAGE_BYTES=0
if [[ "$MODE" == "full" || "$MODE" == "quick" || "$MODE" == "size" ]]; then
  cargo package --allow-dirty --no-verify >/dev/null
  PACKAGE_BYTES="$(stat -c %s "$SERVER_DIR/target/package/kafkalite-server-"*.crate | tail -n 1)"
fi
"$BENCH_BIN" \
  --output-dir "$RUN_DIR" \
  --broker-bin "$BROKER_BIN" \
  --mode "$MODE" \
  --binary-bytes "$BINARY_BYTES" \
  --package-bytes "$PACKAGE_BYTES" \
  --git-sha "$GIT_SHA" \
  "${DIRTY_ARG[@]}"
popd >/dev/null

cp "$RUN_DIR/result.json" "$LATEST_DIR/result.json"
cp "$RUN_DIR/metrics.csv" "$LATEST_DIR/metrics.csv"
cp "$RUN_DIR/summary.md" "$LATEST_DIR/summary.md"
