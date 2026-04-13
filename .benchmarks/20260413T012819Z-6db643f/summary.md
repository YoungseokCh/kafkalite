# Benchmark Summary

- git_sha: `6db643f`
- dirty: `true`
- binary_bytes: `4639848`
- package_bytes: `55215`

| scenario | partitions | elapsed_ms | msgs/sec | peak_rss_kb | total_bytes |
|---|---:|---:|---:|---:|---:|
| bench.produce.small | 1 | 49290.69 | 20.29 | 5496 | 369895 |
| bench.produce.multi_partition | 3 | 48051.86 | 20.81 | 5400 | 643973 |
| bench.roundtrip | 1 | 10607.22 | 18.86 | 5424 | 155295 |
| bench.fetch.multi_partition | 3 | 24295.37 | 20.58 | 5496 | 522919 |
| bench.fetch.tail | 1 | 86.04 | 5811.13 | 5392 | 389283 |
| bench.commit.resume | 1 | 1549.18 | 2.58 | 5500 | 2231 |
| bench.mixed.handoff | 1 | 11497.04 | 17.40 | 5536 | 106206 |
