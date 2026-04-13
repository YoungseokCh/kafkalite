#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-full}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVER_DIR="$ROOT_DIR/rust/server"
BENCH_ROOT="$ROOT_DIR/.benchmarks"

GIT_SHA="$(git -C "$ROOT_DIR" rev-parse --short HEAD 2>/dev/null || printf unknown)"
RUN_LABEL="${2:-$(date -u +%Y%m%dT%H%M%SZ)-$GIT_SHA-$$}"
RUN_DIR="$BENCH_ROOT/$RUN_LABEL"
DIRTY_STATUS="$(git -C "$ROOT_DIR" status --short)"

if [[ -n "$DIRTY_STATUS" ]]; then
  echo "benchmark runs require a clean git tree; commit or stash changes first" >&2
  exit 1
fi

if [[ -e "$RUN_DIR" ]]; then
  echo "benchmark output directory already exists: $RUN_DIR" >&2
  exit 1
fi

mkdir -p "$RUN_DIR"

pushd "$SERVER_DIR" >/dev/null
cargo build --release --features bench-internal --bin kafkalite --bin bench_runner
BROKER_BIN="$SERVER_DIR/target/release/kafkalite"
BENCH_BIN="$SERVER_DIR/target/release/bench_runner"
BINARY_BYTES="$(stat -c %s "$BROKER_BIN")"
PACKAGE_BYTES=0
if [[ "$MODE" == "full" || "$MODE" == "runtime" ]]; then
  cargo package >/dev/null
  PACKAGE_BYTES="$(stat -c %s "$SERVER_DIR/target/package/kafkalite-server-"*.crate | tail -n 1)"
fi
"$BENCH_BIN" \
  --output-dir "$RUN_DIR" \
  --broker-bin "$BROKER_BIN" \
  --mode "$MODE" \
  --binary-bytes "$BINARY_BYTES" \
  --package-bytes "$PACKAGE_BYTES" \
  --git-sha "$GIT_SHA"
popd >/dev/null
