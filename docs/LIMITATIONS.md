# Distributed mode limitations

This document tracks the current known gaps of kafkalite's distributed / KRaft-like implementation.

## What is implemented

- Kafka-style `server.properties` configuration
- metadata image/log/snapshot persistence
- controller/broker role scaffolding
- controller registration / heartbeat metadata
- committed metadata-driven partition leader enforcement
- replication metadata, ISR, and metadata high-watermark tracking
- follower fetch/apply seam and divergence truncation
- reassignment state machine
- in-memory remote transport and multi-node scenario coverage
- benchmark scenarios for cluster replication/reassignment metadata

## Known limitations

### 1. Network transport is still mostly in-memory

- Majority of distributed control-plane behavior is exercised through in-memory transport.
- Real wire protocol / codec / remote TCP control-plane transport is not fully implemented.

### 2. Election semantics are still simplified

- A majority-vote flow exists, but it is not yet a full Raft implementation.
- There is no durable vote log, heartbeat timeout loop, or realistic election scheduler.
- Multi-controller startup is safer than before, but still not production-ready.

### 3. Metadata write concurrency is improved, not fully solved

- Compare-and-append updates now use bounded retry helpers in several paths.
- There is still no full CAS loop / backoff / contention strategy across all metadata mutations.

### 4. Replication is still an approximation

- Replica fetch/apply exists as a staged seam and in-memory transport path.
- Full broker-to-broker record replication over a real network path is not complete.
- Replica truncation/reconciliation is intentionally minimal and not yet production-grade.

### 5. ISR / reassignment rules are simplified

- ISR shrink/expand uses simplified lag-based rules.
- Reassignment progression is safer than before, but still lacks the nuance of Kafka's operational logic.
- Observability around reassignment progress is minimal.

### 6. Multi-process distributed validation is incomplete

- The strongest coverage today is in-memory multi-node testing.
- There is not yet a comprehensive multi-process distributed acceptance suite.

### 7. Operational ergonomics are minimal

- Limited diagnostics/logging for distributed internals
- No dedicated admin tooling for controller/replica/reassignment inspection
- No operator guide for cluster recovery procedures

## Practical interpretation

Today, kafkalite is best described as:

> a Kafka/KRaft-inspired distributed prototype with meaningful metadata/control-plane structure,
> partial replication semantics, strong in-memory scenario coverage, and local persistence —
> but not yet a production-ready distributed Kafka replacement.

## Suggested next steps

1. Real controller/broker network transport for control-plane RPCs
2. Durable vote/election state and heartbeat-driven leader management
3. Stronger metadata CAS/retry semantics
4. Real broker-to-broker replication loop over transport
5. Broader multi-process distributed acceptance tests
