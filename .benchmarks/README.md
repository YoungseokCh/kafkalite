# Benchmarks

- each benchmark run is stored in its own directory directly under `.benchmarks/`
- use a stable label when needed (for example a version or git sha), but do not overwrite prior runs
- compare any two saved runs explicitly with `make bench-compare BASE=... NEW=...`

Benchmark history is intended to be committed so performance changes stay visible over time.
