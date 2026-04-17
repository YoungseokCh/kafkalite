# kafkalite

Toy Kafka implementation in Rust, now extended with a staged Kafka/KRaft-like distributed control plane.

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

