# Benchmark Summary

- git_sha: `cdbc3b7`
- dirty: `false`
- binary_bytes: `4639472`
- package_bytes: `55342`

| scenario | partitions | elapsed_ms | msgs/sec | peak_rss_kb | total_bytes |
|---|---:|---:|---:|---:|---:|
| bench.produce.small | 1 | 48128.53 | 20.78 | 5516 | 369895 |
| bench.produce.multi_partition | 3 | 48308.29 | 20.70 | 5572 | 643973 |
| bench.roundtrip | 1 | 10550.89 | 18.96 | 5432 | 155295 |
| bench.fetch.multi_partition | 3 | 24305.61 | 20.57 | 5496 | 522919 |
| bench.fetch.tail | 1 | 88.60 | 5643.13 | 5396 | 389283 |
| bench.commit.resume | 1 | 1518.25 | 2.63 | 5660 | 2231 |
| bench.mixed.handoff | 1 | 11489.33 | 17.41 | 5568 | 106206 |
