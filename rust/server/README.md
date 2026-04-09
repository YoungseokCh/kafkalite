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
