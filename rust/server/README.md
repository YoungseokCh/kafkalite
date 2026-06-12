# kafkalite-server

Broker-owned file-log Kafka wire-protocol server.

## Install

```bash
cargo install kafkalite-server
```

Installed binaries:

- `kafkalite` — run the broker
- `store_tool` — inspect and repair on-disk storage

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

## Configuration

The server reads a Kafka-style `.properties` file.

See `server.properties.example` for a ready-to-copy sample.

Run the installed broker with:

```bash
kafkalite --config ./server.properties
```

Run from this repository with:

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
make fmt
make clippy
make test
make test-python
make test-differential
make publish-dry-run
make publish-dry-run-dirty
```

- `make fmt` checks Rust formatting
- `make clippy` runs lint checks with warnings denied
- `make test` runs the Rust server/client suites, including the fast local `tests/contract.rs` contract layer
- `make test-python` provisions a temporary virtualenv and runs the broader aiokafka compatibility matrix
- `make test-differential` starts a temporary single-node Kafka container and compares supported roundtrips against the local broker
- `make publish-dry-run` validates the clean release candidate that would be uploaded to crates.io
- `make publish-dry-run-dirty` is a local-only escape hatch for package iteration before committing

## Fuzzing runbook (PR1)

From the repository root, after installing `cargo-fuzz`:

```bash
cargo install cargo-fuzz
cd rust/server/fuzz
cargo fuzz list
cargo fuzz run protocol_peek_header -- -runs=1000
cargo fuzz run cluster_codec_decode -- -runs=1000
```

- `protocol_peek_header` exercises `peek_key_and_version` parsing behavior on raw request bytes.
- `cluster_codec_decode` exercises both `cluster::codec::{decode_request, decode_response}`.
- Keep fuzz corpora/artifacts under `rust/server/fuzz/` and record bounded-smoke outcomes in PR evidence.

Current `make test-python` coverage includes:

- basic `AIOKafkaProducer.send_and_wait` + `AIOKafkaConsumer.getone` roundtrip on partition `0`
- manual offset commit and committed-offset reload for the same consumer group
- multi-partition topic metadata via `producer.partitions_for`
- `beginning_offsets` / `end_offsets` checks for partitions `1` and `2`
- direct assigned reads with `assign` + `seek` on partitions `1` and `2`
- batch-style consumption via `AIOKafkaConsumer.getmany`
- failure-path validation for aiokafka rejecting out-of-range explicit partitions before send
- Adapter-style adapter flow: JSON serializer/deserializer, headers argument usage, constructor topic subscription, manual commit, and same-group resume

## Benchmark commands

From the repository root:

```bash
make bench
make bench LABEL=v1.1.0
```

- benchmark outputs are written under `.benchmarks/`
- runs always use the default timestamp/git-sha naming convention
- when `LABEL` is set, an extra symlink such as `.benchmarks/v1.1.0` points to the canonical run directory
- each run stores only `result.json`, `metrics.csv`, and `summary.md`, so history stays reviewable and portable
- benchmark runs require a clean git tree

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
