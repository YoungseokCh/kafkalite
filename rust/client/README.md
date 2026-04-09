# kafkalite

Internal compatibility crate for the legacy Kafkalite gRPC protobuf definitions and shared message models.

This crate is no longer the recommended integration path. The project is moving to a Kafka wire-protocol server, so the old tonic client wrapper has been removed.

Keep this crate only as long as the rewritten server or transitional tests still depend on:

- generated protobuf message and service types under `kafkalite::proto`
- shared message model structs in `src/models.rs`
