# Benchmark Comparison

- base: .benchmarks/v1.0.0/result.json
- new: .benchmarks/runtime-20260416T5d39b26-dirty/result.json

## Build
- binary_bytes: 4639472 -> 5260216 (delta +620744)
- package_bytes: 55342 -> 0 (delta -55342)

## Scenarios
- bench.cluster.reassignment.metadata: missing in one report
- bench.cluster.replication.metadata: missing in one report
### bench.commit.resume
- elapsed_ms: 1518.25 -> 1498.67
- throughput_msgs_per_sec: 2.63 -> 2.67
- peak_rss_kb: 5660 -> 5776
- total_bytes: 2231 -> 2966

### bench.fetch.multi_partition
- elapsed_ms: 24305.61 -> 24078.61
- throughput_msgs_per_sec: 20.57 -> 20.77
- peak_rss_kb: 5496 -> 5748
- total_bytes: 522919 -> 524572

### bench.fetch.tail
- elapsed_ms: 88.60 -> 86.42
- throughput_msgs_per_sec: 5643.13 -> 5785.87
- peak_rss_kb: 5396 -> 5820
- total_bytes: 389283 -> 390021

### bench.mixed.handoff
- elapsed_ms: 11489.33 -> 11403.18
- throughput_msgs_per_sec: 17.41 -> 17.54
- peak_rss_kb: 5568 -> 5856
- total_bytes: 106206 -> 106947

### bench.produce.multi_partition
- elapsed_ms: 48308.29 -> 47475.21
- throughput_msgs_per_sec: 20.70 -> 21.06
- peak_rss_kb: 5572 -> 5724
- total_bytes: 643973 -> 645629

### bench.produce.small
- elapsed_ms: 48128.53 -> 47669.53
- throughput_msgs_per_sec: 20.78 -> 20.98
- peak_rss_kb: 5516 -> 5624
- total_bytes: 369895 -> 370639

### bench.roundtrip
- elapsed_ms: 10550.89 -> 10479.81
- throughput_msgs_per_sec: 18.96 -> 19.08
- peak_rss_kb: 5432 -> 5636
- total_bytes: 155295 -> 156032

