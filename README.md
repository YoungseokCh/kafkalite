# kafkalite

Toy Kafka implementation in Rust, now extended with a staged Kafka/KRaft-like distributed control plane.


## Running

Example:

```bash
cargo run --manifest-path rust/server/Cargo.toml -- --config rust/server/examples/server.properties
```

## Changelog

### v1.1.3

- Use Kafka-compatible on-disk log layout
- Persist consumer group and offset metadata in internal topics
- Recover topics from cluster metadata on startup
- Remove the legacy state journal

### v1.1.2

- Support AdminClient::create_topics
