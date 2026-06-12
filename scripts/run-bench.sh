#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-full}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVER_DIR="$ROOT_DIR/rust/server"
BENCH_ROOT="$ROOT_DIR/.benchmarks"

GIT_SHA="$(git -C "$ROOT_DIR" rev-parse --short HEAD 2>/dev/null || printf unknown)"
CANONICAL_LABEL="$(date -u +%Y%m%dT%H%M%SZ)-$GIT_SHA-$$"
ALIAS_LABEL="${2:-}"
STAGING_DIR=""

if [[ -n "$ALIAS_LABEL" ]] && [[ ! "$ALIAS_LABEL" =~ ^[A-Za-z0-9._-]+$ || "$ALIAS_LABEL" == "." || "$ALIAS_LABEL" == ".." ]]; then
  echo "invalid label: must match [A-Za-z0-9._-]+ and must not be '.' or '..'" >&2
  exit 1
fi

RUN_DIR="$BENCH_ROOT/$CANONICAL_LABEL"
ALIAS_PATH=""
if [[ -n "$ALIAS_LABEL" ]]; then
  ALIAS_PATH="$BENCH_ROOT/$ALIAS_LABEL"
fi
DIRTY_STATUS="$(git -C "$ROOT_DIR" status --short)"

if [[ -n "$DIRTY_STATUS" ]]; then
  echo "benchmark runs require a clean git tree; commit or stash changes first" >&2
  exit 1
fi

if [[ -e "$RUN_DIR" ]]; then
  echo "benchmark output directory already exists: $RUN_DIR" >&2
  exit 1
fi

if [[ -n "$ALIAS_PATH" && -e "$ALIAS_PATH" ]]; then
  echo "benchmark alias already exists: $ALIAS_PATH" >&2
  exit 1
fi

mkdir -p "$BENCH_ROOT"
STAGING_DIR="$(mktemp -d "$BENCH_ROOT/.tmp-${CANONICAL_LABEL}-XXXXXX")"

cleanup() {
  if [[ -n "$STAGING_DIR" && -d "$STAGING_DIR" ]]; then
    rm -rf "$STAGING_DIR"
  fi
}

trap cleanup EXIT INT TERM

pushd "$SERVER_DIR" >/dev/null
cargo build --release --features bench-internal --bin kafkalite --bin bench_runner
BROKER_BIN="$SERVER_DIR/target/release/kafkalite"
BENCH_BIN="$SERVER_DIR/target/release/bench_runner"
BINARY_BYTES="$(stat -c %s "$BROKER_BIN")"
PACKAGE_BYTES=0
if [[ "$MODE" == "full" ]]; then
  cargo package >/dev/null
  PACKAGE_BYTES="$(stat -c %s "$SERVER_DIR/target/package/kafkalite-server-"*.crate | tail -n 1)"
fi
"$BENCH_BIN" \
  --output-dir "$STAGING_DIR" \
  --broker-bin "$BROKER_BIN" \
  --mode "$MODE" \
  --binary-bytes "$BINARY_BYTES" \
  --package-bytes "$PACKAGE_BYTES" \
  --git-sha "$GIT_SHA"
popd >/dev/null

if [[ -e "$RUN_DIR" ]]; then
  echo "benchmark output directory already exists: $RUN_DIR" >&2
  exit 1
fi

mv "$STAGING_DIR" "$RUN_DIR"
STAGING_DIR=""

if [[ -n "$ALIAS_PATH" ]]; then
  ln -s "$CANONICAL_LABEL" "$ALIAS_PATH"
fi

trap - EXIT INT TERM
