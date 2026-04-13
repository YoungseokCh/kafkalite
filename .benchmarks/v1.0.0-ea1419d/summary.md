# Benchmark Summary

- git_sha: `ea1419d`
- dirty: `false`
- binary_bytes: `4588056`
- package_bytes: `55287`

| scenario | partitions | elapsed_ms | msgs/sec | peak_rss_kb | total_bytes |
|---|---:|---:|---:|---:|---:|
| bench.produce.small | 1 | 48028.99 | 20.82 | 5408 | 369895 |
| bench.produce.multi_partition | 3 | 48085.38 | 20.80 | 5448 | 643973 |
| bench.roundtrip | 1 | 10516.34 | 19.02 | 5356 | 155295 |
| bench.fetch.multi_partition | 3 | 24268.32 | 20.60 | 5352 | 522919 |
| bench.fetch.tail | 1 | 89.64 | 5578.08 | 5444 | 389283 |
| bench.commit.resume | 1 | 1500.32 | 2.67 | 5480 | 2231 |
| bench.mixed.handoff | 1 | 11449.08 | 17.47 | 5668 | 106206 |
