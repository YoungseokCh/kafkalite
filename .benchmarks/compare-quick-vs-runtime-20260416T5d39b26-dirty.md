# Benchmark Comparison

- base: .benchmarks/quick-20260416T5d39b26-dirty/result.json
- new: .benchmarks/runtime-20260416T5d39b26-dirty/result.json

## Build
- binary_bytes: 5260216 -> 5260216 (delta +0)
- package_bytes: 0 -> 0 (delta +0)

## Scenarios
- bench.cluster.reassignment.metadata: missing in one report
- bench.cluster.replication.metadata: missing in one report
- bench.commit.resume: missing in one report
- bench.fetch.multi_partition: missing in one report
- bench.fetch.tail: missing in one report
- bench.mixed.handoff: missing in one report
- bench.produce.multi_partition: missing in one report
### bench.produce.small
- elapsed_ms: 47529.56 -> 47669.53
- throughput_msgs_per_sec: 21.04 -> 20.98
- peak_rss_kb: 5572 -> 5624
- total_bytes: 370639 -> 370639

- bench.roundtrip: missing in one report
