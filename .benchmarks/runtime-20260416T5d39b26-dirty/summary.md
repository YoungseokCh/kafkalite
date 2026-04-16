# Benchmark Summary

- git_sha: `5d39b26`
- dirty: `true`
- binary_bytes: `5260216`
- package_bytes: `0`

| scenario | partitions | elapsed_ms | msgs/sec | peak_rss_kb | total_bytes |
|---|---:|---:|---:|---:|---:|
| bench.produce.small | 1 | 47669.53 | 20.98 | 5624 | 370639 |
| bench.produce.multi_partition | 3 | 47475.21 | 21.06 | 5724 | 645629 |
| bench.roundtrip | 1 | 10479.81 | 19.08 | 5636 | 156032 |
| bench.fetch.multi_partition | 3 | 24078.61 | 20.77 | 5748 | 524572 |
| bench.fetch.tail | 1 | 86.42 | 5785.87 | 5820 | 390021 |
| bench.commit.resume | 1 | 1498.67 | 2.67 | 5776 | 2966 |
| bench.mixed.handoff | 1 | 11403.18 | 17.54 | 5856 | 106947 |
| bench.cluster.replication.metadata | 1 | 0.50 | 200000.00 | 0 | 515 |
| bench.cluster.reassignment.metadata | 1 | 0.35 | 100000.00 | 0 | 1032 |
