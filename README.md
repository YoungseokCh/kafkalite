# kafkalite

Toy Kafka implementation in Rust, now extended with a staged Kafka/KRaft-like distributed control plane.


## Running

Example:

```bash
cargo run --manifest-path rust/server/Cargo.toml -- --config rust/server/examples/server.properties
```

## Changelog

### v1.1.5

- Simplify the embedded broker lifecycle: remove `KafkaBroker::run()` and make `KafkaBroker::start()` plus `BrokerHandle::{ready,wait,shutdown}` the supported API
- Add transactional produce flow scaffolding, including coordinator-side transaction session/state handling and transaction marker persistence
- Persist transaction coordinator state in Kafka-compatible `__transaction_state` records and extend internal-topic recovery coverage
- Tighten Kafka-compatible log/index/timeindex/leader-epoch-checkpoint filesystem behavior for real Kafka log directory recovery
- Add Kafka-aligned log retention defaults plus size-based and time-based segment eviction/roll handling
- Expand replica and fetch behavior around watermarks, long-polling, leadership-sensitive reads, and append/recovery edge cases
- Add broader recovery and differential coverage, including real Kafka fixture generation, rolled-segment cases, multi-append scenarios, and filesystem compatibility checks
- Add storage and producer regression tests for recovery, replica application, and Kafka-format on-disk state transitions

### v1.1.4

- Support fetch long polling
- Reduce fetch response copy overhead
- Reground benchmark scenarios and simplify validation commands

### v1.1.3

- Use Kafka-compatible on-disk log layout
- Persist consumer group and offset metadata in internal topics
- Recover topics from cluster metadata on startup
- Remove the legacy state journal

### v1.1.2

- Support AdminClient::create_topics
