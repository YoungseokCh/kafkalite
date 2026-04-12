# kafkalite-server

Broker-owned file-log Kafka wire-protocol server.

## Current scope

- single broker
- many topics
- configurable partition count per topic via `default_partitions` (default: `1`)
- file-log persistence
- narrow Kafka API surface

## Supported Kafka APIs

- `ApiVersions` (v0-3)
- `Metadata` (v1-12)
- `InitProducerId` (v0-4)
- `Produce` (v3-9)
- `Fetch` (v4-11)
- `ListOffsets` (v1-7)
- `FindCoordinator` (v0-4)
- `JoinGroup` (v0-5)
- `SyncGroup` (v0-5)
- `Heartbeat` (v0-4)
- `LeaveGroup` (v0-4)
- `OffsetCommit` (v0-7)
- `OffsetFetch` (v1-7)

## Behavioral notes

- metadata requests can auto-create topics when the request enables auto topic creation
- produce requests also create topics on first successful write when the requested partition is within the configured `default_partitions` range
- metadata advertises every configured partition for known topics
- direct produce/fetch/list-offsets operate on any existing partition in the configured range; out-of-range partitions return `UNKNOWN_TOPIC_OR_PARTITION`
- committed offsets are stored per `(group, topic, partition)`
- idempotent producer support covers validated duplicate retry replay, stale epoch rejection, and unknown producer id rejection for the current single-broker flow
- producer transactions and full multi-broker EOS semantics are out of scope

## Migration note

- storage configuration now uses `data_dir`
- older `db_path`-based configs are obsolete and should be migrated to the directory-based layout

## Configuration

The server reads a Kafka-style `.properties` file.

See `server.properties.example` for a ready-to-copy sample.

Run the broker with:

```bash
cargo run --manifest-path rust/server/Cargo.toml --bin kafkalite -- --config rust/server/server.properties.example
```

```properties
node.id=1
listeners=PLAINTEXT://127.0.0.1:9092
advertised.listeners=PLAINTEXT://127.0.0.1:9092
log.dirs=./data
num.partitions=3
```

- `num.partitions` defaults to `1`, which preserves the single-partition topic shape unless you opt into more partitions
- `log.dirs` currently supports exactly one directory
- `cluster.id` is optional and overrides the default response cluster id if set

## Validation commands

From the repository root:

```bash
make test
make test-python
make test-differential
```

- `make test` runs the Rust server/client suites, including the fast local `tests/contract.rs` contract layer
- `make test-python` provisions a temporary virtualenv and runs the Python compatibility smoke test
- `make test-differential` starts a temporary single-node Kafka container and compares supported roundtrips against the local broker

## Benchmark commands

From the repository root:

```bash
make bench
make bench-quick
make bench-size
make bench-runtime
make bench-memory
make bench-storage
make bench-baseline
make bench-compare
```

- benchmark outputs are written under `.benchmarks/`
- `make bench-runtime` and `make bench` include the multi-partition scenarios `bench.produce.multi_partition` and `bench.fetch.multi_partition` alongside `bench.mixed.handoff`
- `make bench-baseline` promotes the latest run to the comparison baseline
- `make bench-compare` compares the current baseline and latest benchmark JSON reports

## Store inspection and repair

From the repository root:

```bash
cargo run --manifest-path rust/server/Cargo.toml --bin store_tool -- --data-dir ./data storage-summary
cargo run --manifest-path rust/server/Cargo.toml --bin store_tool -- --data-dir ./data topic-summary my-topic
cargo run --manifest-path rust/server/Cargo.toml --bin store_tool -- --data-dir ./data rebuild-indexes my-topic
```

- `storage-summary` prints aggregate topic/group/byte totals
- `topic-summary` prints partition offsets for one topic
- `rebuild-indexes` recreates `.index` and `.timeindex` files from the `.log` data for a topic

## Current benchmark snapshot

Latest recorded benchmark snapshot from `.benchmarks/latest/result.json`:

- git sha: `592d416`
- release binary size: `5,324,152` bytes
- package size: `0` bytes
- host: `linux/x86_64`
- run shape: `make bench-runtime`

| scenario | partitions | workload | elapsed | throughput | peak RSS | storage total |
|---|---:|---:|---:|---:|---:|---:|
| `bench.produce.small` | 1 | 1,000 msgs × 100B | 47,727 ms | 20.95 msgs/s | 5,884 KB | 369,895 B |
| `bench.produce.multi_partition` | 3 | 1,000 msgs × 100B | 47,675 ms | 20.98 msgs/s | 5,992 KB | 643,973 B |
| `bench.roundtrip` | 1 | 200 msgs × 512B | 10,484 ms | 19.08 msgs/s | 6,180 KB | 155,295 B |
| `bench.fetch.multi_partition` | 3 | 500 msgs × 512B | 24,909 ms | 20.07 msgs/s | 5,916 KB | 522,919 B |
| `bench.fetch.tail` | 1 | 500 msgs × 512B | 85 ms | 5861.54 msgs/s | 5,976 KB | 389,283 B |
| `bench.commit.resume` | 1 | 4 msgs × 256B | 1,546 ms | 2.59 msgs/s | 5,944 KB | 2,231 B |
| `bench.mixed.handoff` | 1 | 200 msgs × 256B | 11,383 ms | 17.57 msgs/s | 6,112 KB | 106,206 B |

For reproducibility, rerun either:

```bash
make bench-runtime
make bench
```

## Historical benchmark notes

Reference SQLite-backed benchmark captured from commit `acaa3fe` and stored at:

- `.benchmarks/runs/sqlite-acaa3fe/result.json`

SQLite-backed snapshot (normalized to the same shape as the current benchmark snapshot):

- git sha: `acaa3fe`
- release binary size: `6,887,736` bytes
- package size: `35,266` bytes
- host: `linux/x86_64`

| scenario | workload | elapsed | throughput | peak RSS | storage total | storage breakdown |
|---|---:|---:|---:|---:|---:|---|
| `bench.produce.small` | 1,000 msgs × 100B | 47,634 ms | 20.99 msgs/s | 7,796 KB | 253,952 B | sqlite db 253,952 |
| `bench.produce.medium` | 500 msgs × 1,024B | 23,954 ms | 20.87 msgs/s | 8,260 KB | 638,976 B | sqlite db 638,976 |
| `bench.roundtrip` | 200 msgs × 512B | 10,442 ms | 19.15 msgs/s | 7,536 KB | 4,096 B | sqlite db 4,096 |
| `bench.commit.resume` | 4 msgs × 256B | 1,593 ms | 2.51 msgs/s | 7,584 KB | 4,096 B | sqlite db 4,096 |

This section is intended to be updated over time as new benchmark runs are captured.
