#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PATH="${AIOKAFKA_VENV:-/tmp/kafkalite-aiokafka-venv}"

python3 -m venv "$VENV_PATH"
"$VENV_PATH/bin/python" -m pip install --quiet aiokafka

cd "$ROOT_DIR/rust/server"
AIOKAFKA_PYTHON="$VENV_PATH/bin/python" cargo test --test python_compat
