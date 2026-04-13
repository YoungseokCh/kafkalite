#!/usr/bin/env bash
set -euo pipefail

BASE="${1:-}"
NEW="${2:-}"
OUTPUT="${3:-}"

if [[ -z "$BASE" || -z "$NEW" ]]; then
  echo "usage: $0 BASE_RESULT_JSON NEW_RESULT_JSON [OUTPUT_MARKDOWN]" >&2
  exit 1
fi

if [[ ! -f "$BASE" ]]; then
  echo "missing base benchmark file: $BASE" >&2
  exit 1
fi

if [[ ! -f "$NEW" ]]; then
  echo "missing new benchmark file: $NEW" >&2
  exit 1
fi

OUTPUT_ARGS=()
if [[ -n "$OUTPUT" ]]; then
  OUTPUT_ARGS=("$OUTPUT")
fi

python3 - "$BASE" "$NEW" "${OUTPUT_ARGS[@]}" <<'PY'
import json
import sys

base = json.load(open(sys.argv[1]))
new = json.load(open(sys.argv[2]))
output = sys.argv[3] if len(sys.argv) > 3 else None

out = []
out.append("# Benchmark Comparison\n")
out.append(f"- base: {sys.argv[1]}")
out.append(f"- new: {sys.argv[2]}\n")
out.append("## Build")
for key in ["binary_bytes", "package_bytes"]:
    b = base["build"].get(key, 0)
    n = new["build"].get(key, 0)
    delta = n - b
    out.append(f"- {key}: {b} -> {n} (delta {delta:+})")
out.append("\n## Scenarios")
base_map = {s["name"]: s for s in base["scenarios"]}
new_map = {s["name"]: s for s in new["scenarios"]}
for name in sorted(set(base_map) | set(new_map)):
    b = base_map.get(name)
    n = new_map.get(name)
    if not b or not n:
        out.append(f"- {name}: missing in one report")
        continue
    out.append(f"### {name}")
    out.append(f"- elapsed_ms: {b['runtime']['elapsed_ms']:.2f} -> {n['runtime']['elapsed_ms']:.2f}")
    out.append(f"- throughput_msgs_per_sec: {b['runtime']['throughput_msgs_per_sec']:.2f} -> {n['runtime']['throughput_msgs_per_sec']:.2f}")
    out.append(f"- peak_rss_kb: {b['memory']['peak_rss_kb']} -> {n['memory']['peak_rss_kb']}")
    out.append(f"- total_bytes: {b['storage']['total_bytes']} -> {n['storage']['total_bytes']}\n")
text = "\n".join(out) + "\n"
print(text, end="")
if output:
    open(output, "w").write(text)
PY
