# Benchmark Summary

- git_sha: `a4411e4`
- dirty: `false`
- binary_bytes: `5275592`
- package_bytes: `126245`

| scenario | partitions | elapsed_ms | msgs/sec | peak_rss_kb | total_bytes |
|---|---:|---:|---:|---:|---:|
| bench.produce.small | 1 | 49087.01 | 20.37 | 5668 | 370639 |
| bench.produce.multi_partition | 3 | 50700.56 | 19.72 | 5732 | 645629 |
| bench.roundtrip | 1 | 10562.42 | 18.94 | 6232 | 156032 |
| bench.fetch.multi_partition | 3 | 25337.97 | 19.73 | 5644 | 524572 |
| bench.fetch.tail | 1 | 85.43 | 5852.94 | 5856 | 390021 |
| bench.commit.resume | 1 | 1502.62 | 2.66 | 5804 | 2966 |
| bench.mixed.handoff | 1 | 11365.88 | 17.60 | 6000 | 106947 |
| bench.cluster.replication.metadata | 1 | 1.59 | 125899.08 | 0 | 515 |
| bench.cluster.reassignment.metadata | 1 | 1.14 | 87656.71 | 0 | 1032 |
