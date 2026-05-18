# kafkalite

Toy Kafka implementation in Rust, now extended with a staged Kafka/KRaft-like distributed control plane.

## Changelog

### v1.1.2

- Stabilized distributed controller replication workflows around metadata append, leader changes, and replica progress.
- Split large runtime, transport, and integration-test modules into smaller behavior-preserving modules.
- Expanded control-plane, differential, runtime, and transport regression coverage.
- Verified with `cargo test --all-targets --all-features` and clippy with warnings denied.

## Running

Example:

```bash
cargo run --manifest-path rust/server/Cargo.toml -- --config rust/server/examples/server.properties
```

Or with env:

```bash
KAFKALITE_CONFIG=rust/server/examples/server.properties cargo run --manifest-path rust/server/Cargo.toml
```

