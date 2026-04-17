# kafkalite

Toy Kafka implementation in Rust, now extended with a staged Kafka/KRaft-like distributed control plane.

## Current status

Implemented phases:

- Phase 0: properties-based cluster scaffolding
- Phase 1: single-node metadata log / snapshot / replay
- Phase 2: controller quorum state, broker registration, internal RPC scaffolding
- Phase 3: committed metadata-based partition leadership enforcement
- Phase 4: replication metadata, ISR/high-watermark tracking, follower fetch/apply seam
- Phase 5: partition reassignment state machine
- Phase 6: metadata recovery hardening and benchmark/tooling coverage

This is still **not full Kafka compatibility**. The project currently provides a useful distributed-model skeleton with in-memory multi-node scenarios and local persistence, but real production-grade network quorum/replication semantics are still incomplete.

## Configuration

Server configuration uses Kafka-style `server.properties`.

Important keys:

- `process.roles=broker,controller`
- `node.id=1`
- `listeners=PLAINTEXT://:9092,CONTROLLER://:9093`
- `advertised.listeners=PLAINTEXT://127.0.0.1:9092`
- `controller.listener.names=CONTROLLER`
- `controller.quorum.voters=1@127.0.0.1:9093,2@127.0.0.1:9094,3@127.0.0.1:9095`
- `cluster.id=dev-cluster`
- `log.dirs=/tmp/kafkalite-1`
- `num.partitions=3`

See `rust/server/examples/server.properties` for a complete example.

## Running

Example:

```bash
cargo run --manifest-path rust/server/Cargo.toml -- --config rust/server/examples/server.properties
```

Or with env:

```bash
KAFKALITE_CONFIG=rust/server/examples/server.properties cargo run --manifest-path rust/server/Cargo.toml
```

## Distributed mode limitations

Current distributed mode supports:

- controller metadata image/log
- broker registration and heartbeat metadata
- partition leader updates
- replication metadata / ISR / metadata high watermark
- reassignment state machine
- in-memory remote transport and multi-node test harnesses

Still incomplete / experimental:

- real networked quorum transport and majority election
- full broker-to-broker record replication loop over real transport
- full retry/compare-and-swap handling for concurrent metadata writers
- production-ready reassignment safety and observability
- full multi-process distributed compatibility matrix

For a fuller and maintained list, see `docs/LIMITATIONS.md`.

## Implementation summary

For a compact milestone/commit-group overview, see `docs/IMPLEMENTATION_SUMMARY.md`.

## Benchmarks

Bench runner includes local broker scenarios and cluster metadata scenarios.

Examples:

```bash
cargo run --manifest-path rust/server/Cargo.toml --features bench-internal --bin bench_runner -- --profile quick
```

Scenarios include:

- produce-only
- fetch-tail
- roundtrip
- commit-resume
- mixed handoff
- cluster replication metadata
- cluster reassignment metadata

## Testing

Common checks:

```bash
cargo fmt
cargo check
cargo test --test compat rdkafka_producer_and_consumer_smoke -- --nocapture
```

Differential tests against a real Kafka broker require:

```bash
REAL_KAFKA_BOOTSTRAP=host:9092 cargo test --test differential -- --nocapture
```
