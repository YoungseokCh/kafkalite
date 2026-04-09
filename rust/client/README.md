# kafkalite

Internal compatibility crate for the legacy Kafkalite gRPC protobuf definitions and shared message models.

This crate is no longer the recommended integration path. The project uses a Kafka wire-protocol server, and this crate now only preserves protobuf message definitions and shared model structs.

Keep this crate only as long as the rewritten server or transitional tests still depend on:

- generated protobuf message types under `kafkalite::proto`
- shared message model structs in `src/models.rs`
