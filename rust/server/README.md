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

- `make test` runs the Rust server/client suites
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
- `make bench-baseline` promotes the latest run to the comparison baseline
- `make bench-compare` compares the current baseline and latest benchmark JSON reports

## Current benchmark snapshot

Latest full benchmark run recorded from `.benchmarks/latest/result.json`:

- git sha: `9455b75`
- release binary size: `5,140,832` bytes
- package size: `42,777` bytes
- host: `linux/x86_64`

| scenario | workload | elapsed | throughput | peak RSS | storage total | storage breakdown |
|---|---:|---:|---:|---:|---:|---|
| `bench.produce.small` | 1,000 msgs × 100B | 50,741 ms | 19.71 msgs/s | 5,852 KB | 1,056,519 B | log 538,560 / index 68,570 / timeindex 70,680 / state journal 378,709 |
| `bench.produce.medium` | 500 msgs × 1,024B | 25,591 ms | 19.54 msgs/s | 5,832 KB | 1,916,258 B | log 1,655,060 / index 34,939 / timeindex 35,549 / state journal 190,710 |
| `bench.roundtrip` | 200 msgs × 512B | 11,111 ms | 18.00 msgs/s | 6,104 KB | 455,398 B | log 354,560 / index 13,714 / timeindex 14,024 / state journal 73,100 |
| `bench.commit.resume` | 4 msgs × 256B | 1,516 ms | 2.64 msgs/s | 5,852 KB | 11,590 B | log 3,996 / index 248 / timeindex 264 / state journal 7,082 |

For reproducibility, rerun:

```bash
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
