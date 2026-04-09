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
- idempotent producer support covers the validated happy path, but advanced retry/epoch semantics are still intentionally narrow

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

- git sha: `0638556`
- release binary size: `5,165,720` bytes
- package size: `37,755` bytes
- host: `linux/x86_64`

| scenario | workload | elapsed | throughput | peak RSS | storage total | storage breakdown |
|---|---:|---:|---:|---:|---:|---|
| `bench.produce.small` | 1,000 msgs × 100B | 59,275 ms | 16.87 msgs/s | 5,704 KB | 678,250 B | log 538,560 / index 68,570 / timeindex 70,680 / state 440 |
| `bench.produce.medium` | 500 msgs × 1,024B | 29,815 ms | 16.77 msgs/s | 5,760 KB | 1,725,990 B | log 1,655,060 / index 34,939 / timeindex 35,549 / state 442 |
| `bench.roundtrip` | 200 msgs × 512B | 12,995 ms | 15.39 msgs/s | 5,708 KB | 382,725 B | log 354,560 / index 13,714 / timeindex 14,024 / state 427 |
| `bench.commit.resume` | 4 msgs × 256B | 1,603 ms | 2.49 msgs/s | 5,804 KB | 6,796 B | log 3,996 / index 248 / timeindex 264 / state 2,288 |

For reproducibility, rerun:

```bash
make bench
```
