# Benchmark Summary

- git_sha: `ca2ac3e`
- dirty: `false`
- binary_bytes: `5353040`
- package_bytes: `143754`

| scenario | partitions | elapsed_ms | msgs/sec | peak_rss_kb | total_bytes |
|---|---:|---:|---:|---:|---:|
| bench.produce.small | 1 | 50230.64 | 19.91 | 6692 | 179020 |
| bench.produce.multi_partition | 3 | 50814.27 | 19.68 | 6364 | 179932 |
| bench.roundtrip | 1 | 10597.64 | 18.87 | 6252 | 118813 |
| bench.fetch.multi_partition | 3 | 25687.77 | 19.46 | 6276 | 296869 |
| bench.fetch.tail | 1 | 88.74 | 5634.49 | 6928 | 295902 |
| bench.commit.resume | 1 | 1496.00 | 2.67 | 6024 | 3508 |
| bench.mixed.handoff | 1 | 11516.12 | 17.37 | 6552 | 72754 |
| bench.cluster.replication.metadata | 1 | 1.92 | 104415.03 | 0 | 515 |
| bench.cluster.reassignment.metadata | 1 | 1.36 | 73745.55 | 0 | 1032 |
