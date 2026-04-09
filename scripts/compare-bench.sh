#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BASE="${1:-$ROOT_DIR/.benchmarks/baseline/result.json}"
NEW="${2:-$ROOT_DIR/.benchmarks/latest/result.json}"
OUTPUT="$ROOT_DIR/.benchmarks/latest/compare.md"

if [[ ! -f "$BASE" ]]; then
  echo "missing base benchmark file: $BASE" >&2
  exit 1
fi

if [[ ! -f "$NEW" ]]; then
  echo "missing new benchmark file: $NEW" >&2
  exit 1
fi

python3 - "$BASE" "$NEW" "$OUTPUT" <<'PY'
import json, sys
base = json.load(open(sys.argv[1]))
new = json.load(open(sys.argv[2]))
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
open(sys.argv[3], "w").write(text)
PY
