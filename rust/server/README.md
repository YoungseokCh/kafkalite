# kafkalite-server

Broker-owned file-log Kafka wire-protocol server.

## Current scope

- single broker
- many topics
- exactly one partition per topic (`0`)
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

- only partition `0` is supported; any other partition is rejected
- metadata requests can auto-create topics when the request enables auto topic creation
- produce requests also create topics on first successful write
- idempotent producer support covers validated duplicate retry replay, stale epoch rejection, and unknown producer id rejection for the current single-broker flow
- producer transactions and full multi-broker EOS semantics are out of scope

## Migration note

- storage configuration now uses `data_dir`
- older `db_path`-based configs are obsolete and should be migrated to the directory-based layout

## Configuration

The server reads the `[kafkalite]` section from the provided TOML file.

```toml
[kafkalite.broker]
broker_id = 1
host = "127.0.0.1"
port = 9092
advertised_host = "127.0.0.1"
advertised_port = 9092
cluster_id = "kafkalite-single-broker"

[kafkalite.storage]
data_dir = "./data"
```

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
- `make bench-runtime` and `make bench` now include the mixed control-plane/data-plane scenario `bench.mixed.handoff`
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

- git sha: `1278842`
- release binary size: `5,225,744` bytes
- package size: `49,944` bytes
- host: `linux/x86_64`
- run shape: `make bench-quick`

| scenario | workload | elapsed | throughput | peak RSS | storage total | storage breakdown |
|---|---:|---:|---:|---:|---:|---|
| `bench.produce.small` | 1,000 msgs × 100B | 47,507 ms | 21.05 msgs/s | 5,828 KB | 369,895 B | log 185,000 / index 1,764 / timeindex 1,512 / state journal 181,619 |

For reproducibility, rerun either:

```bash
make bench-quick
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
