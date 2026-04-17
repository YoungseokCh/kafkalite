# R12 PR1 Fuzz Validation (Robustness)

## Scope validated

- Fuzz scaffold present at `rust/server/fuzz/`.
- Exactly two fuzz targets are registered:
  - `protocol_peek_header`
  - `cluster_codec_decode`

## Quality gates

Executed from repository root:

```bash
make fmt
make clippy
make test
```

Result: all passed.

## Fuzz smoke runs (bounded)

Executed from `rust/server/fuzz`:

```bash
cargo fuzz list
cargo fuzz run protocol_peek_header -- -runs=1000
cargo fuzz run cluster_codec_decode -- -runs=1000
```

Observed results:

- `cargo fuzz list` reported both targets.
- `protocol_peek_header` completed `-runs=1000` with no crash.
- `cluster_codec_decode` completed `-runs=1000` with no crash.

## Artifacts

- Fuzzer-generated corpus/artifacts are under:
  - `rust/server/fuzz/corpus/`
  - `rust/server/fuzz/artifacts/`

## Benchmark snapshot

Executed from repository root:

```bash
make bench-runtime LABEL=r12-pr1-tcp-route
```

Observed results from `bench.cluster.reassignment.metadata` and representative peers:

- `bench.cluster.reassignment.metadata`: 1.11ms (90,127.44 msgs/sec)
- `bench.cluster.replication.metadata`: 1.95ms (102,350.48 msgs/sec)
- `bench.produce.small`: 48,472.07ms (20.63 msgs/sec)
