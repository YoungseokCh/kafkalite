# Benchmarks

- each benchmark run is stored in its own directory directly under `.benchmarks/`
- commit only reviewable outputs (`result.json`, `metrics.csv`, `summary.md`)
- dirty benchmark runs are local-only; keep `*-dirty/` directories and `*-dirty.md` comparisons untracked
- use a stable label when needed (for example `make bench-runtime LABEL=v1.1.0`), but do not overwrite prior runs
- compare any two saved runs explicitly with `make bench-compare BASE=... NEW=...`

Benchmark history is intended to be committed so performance changes stay visible over time.
