#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-full}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVER_DIR="$ROOT_DIR/rust/server"
BENCH_ROOT="$ROOT_DIR/.benchmarks"

GIT_SHA="$(git -C "$ROOT_DIR" rev-parse --short HEAD 2>/dev/null || printf unknown)"
RUN_LABEL="${2:-$(date -u +%Y%m%dT%H%M%SZ)-$GIT_SHA}"
RUN_DIR="$BENCH_ROOT/$RUN_LABEL"
DIRTY=false
if [[ -n "$(git -C "$ROOT_DIR" status --short)" ]]; then
  DIRTY=true
fi

mkdir -p "$RUN_DIR"

DIRTY_ARG=()
if [[ "$DIRTY" == true ]]; then
  DIRTY_ARG=(--dirty)
fi

pushd "$SERVER_DIR" >/dev/null
cargo build --release --features bench-internal --bin kafkalite --bin bench_runner
BROKER_BIN="$SERVER_DIR/target/release/kafkalite"
BENCH_BIN="$SERVER_DIR/target/release/bench_runner"
BINARY_BYTES="$(stat -c %s "$BROKER_BIN")"
PACKAGE_BYTES=0
if [[ "$MODE" == "full" || "$MODE" == "runtime" ]]; then
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
