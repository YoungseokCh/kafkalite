#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KAFKA_IMAGE="${KAFKA_IMAGE:-apache/kafka:3.9.0}"
KAFKA_PORT="${REAL_KAFKA_PORT:-59092}"
CONTAINER_NAME="${REAL_KAFKA_CONTAINER:-kafkalite-real-kafka-${KAFKA_PORT}}"
BOOTSTRAP="127.0.0.1:${KAFKA_PORT}"
KAFKA_REFERENCE_PORT="${REAL_KAFKA_REFERENCE_PORT:-59093}"
REFERENCE_CONTAINER_NAME="${REAL_KAFKA_REFERENCE_CONTAINER:-kafkalite-real-kafka-${KAFKA_REFERENCE_PORT}}"
REFERENCE_BOOTSTRAP="127.0.0.1:${KAFKA_REFERENCE_PORT}"
WAIT_SECONDS="${REAL_KAFKA_WAIT_SECONDS:-60}"
NETWORK_KAFKA_LOG_DIR="${REAL_KAFKA_NETWORK_LOG_DIR:-$(mktemp -d)}"
KAFKA_LOG_DIR="${REAL_KAFKA_LOG_DIR:-$(mktemp -d)}"
KAFKA_APPEND_REFERENCE_DIR="${REAL_KAFKA_APPEND_REFERENCE_DIR:-$(mktemp -d)}"
KAFKA_ROLLED_LOG_DIR="${REAL_KAFKA_ROLLED_LOG_DIR:-$(mktemp -d)}"
KAFKA_ROLLED_APPEND_REFERENCE_DIR="${REAL_KAFKA_ROLLED_APPEND_REFERENCE_DIR:-$(mktemp -d)}"
KAFKA_MULTI_APPEND_LOG_DIR="${REAL_KAFKA_MULTI_APPEND_LOG_DIR:-$(mktemp -d)}"
KAFKA_MULTI_APPEND_REFERENCE_DIR="${REAL_KAFKA_MULTI_APPEND_REFERENCE_DIR:-$(mktemp -d)}"
REMOVE_NETWORK_KAFKA_LOG_DIR=0
REMOVE_KAFKA_LOG_DIR=0
REMOVE_KAFKA_APPEND_REFERENCE_DIR=0
REMOVE_KAFKA_ROLLED_LOG_DIR=0
REMOVE_KAFKA_ROLLED_APPEND_REFERENCE_DIR=0
REMOVE_KAFKA_MULTI_APPEND_LOG_DIR=0
REMOVE_KAFKA_MULTI_APPEND_REFERENCE_DIR=0

if [[ -z "${REAL_KAFKA_NETWORK_LOG_DIR:-}" ]]; then
  REMOVE_NETWORK_KAFKA_LOG_DIR=1
fi
if [[ -z "${REAL_KAFKA_LOG_DIR:-}" ]]; then
  REMOVE_KAFKA_LOG_DIR=1
fi
if [[ -z "${REAL_KAFKA_APPEND_REFERENCE_DIR:-}" ]]; then
  REMOVE_KAFKA_APPEND_REFERENCE_DIR=1
fi
if [[ -z "${REAL_KAFKA_ROLLED_LOG_DIR:-}" ]]; then
  REMOVE_KAFKA_ROLLED_LOG_DIR=1
fi
if [[ -z "${REAL_KAFKA_ROLLED_APPEND_REFERENCE_DIR:-}" ]]; then
  REMOVE_KAFKA_ROLLED_APPEND_REFERENCE_DIR=1
fi
if [[ -z "${REAL_KAFKA_MULTI_APPEND_LOG_DIR:-}" ]]; then
  REMOVE_KAFKA_MULTI_APPEND_LOG_DIR=1
fi
if [[ -z "${REAL_KAFKA_MULTI_APPEND_REFERENCE_DIR:-}" ]]; then
  REMOVE_KAFKA_MULTI_APPEND_REFERENCE_DIR=1
fi

cleanup() {
  echo "Stopping Kafka Docker containers ${CONTAINER_NAME} and ${REFERENCE_CONTAINER_NAME}..."
  docker stop "$CONTAINER_NAME" >/dev/null 2>&1 || true
  docker stop "$REFERENCE_CONTAINER_NAME" >/dev/null 2>&1 || true
  if [[ "$REMOVE_NETWORK_KAFKA_LOG_DIR" == "1" ]]; then
    rm -rf "$NETWORK_KAFKA_LOG_DIR"
  fi
  if [[ "$REMOVE_KAFKA_LOG_DIR" == "1" ]]; then
    rm -rf "$KAFKA_LOG_DIR"
  fi
  if [[ "$REMOVE_KAFKA_APPEND_REFERENCE_DIR" == "1" ]]; then
    rm -rf "$KAFKA_APPEND_REFERENCE_DIR"
  fi
  if [[ "$REMOVE_KAFKA_ROLLED_LOG_DIR" == "1" ]]; then
    rm -rf "$KAFKA_ROLLED_LOG_DIR"
  fi
  if [[ "$REMOVE_KAFKA_ROLLED_APPEND_REFERENCE_DIR" == "1" ]]; then
    rm -rf "$KAFKA_ROLLED_APPEND_REFERENCE_DIR"
  fi
  if [[ "$REMOVE_KAFKA_MULTI_APPEND_LOG_DIR" == "1" ]]; then
    rm -rf "$KAFKA_MULTI_APPEND_LOG_DIR"
  fi
  if [[ "$REMOVE_KAFKA_MULTI_APPEND_REFERENCE_DIR" == "1" ]]; then
    rm -rf "$KAFKA_MULTI_APPEND_REFERENCE_DIR"
  fi
}
trap cleanup EXIT

dump_logs() {
  local container_name="$1"
  echo "Kafka Docker logs for ${container_name}:" >&2
  docker logs "$container_name" >&2 || true
}

wait_for_kafka() {
  local port="$1"
  local bootstrap="$2"
  local container_name="$3"
  local deadline=$((SECONDS + WAIT_SECONDS))
  echo "Waiting up to ${WAIT_SECONDS}s for Kafka bootstrap ${bootstrap}..."
  until (echo >"/dev/tcp/127.0.0.1/${port}") >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      echo "Kafka bootstrap ${bootstrap} did not become reachable within ${WAIT_SECONDS}s" >&2
      dump_logs "$container_name"
      exit 1
    fi
    echo "Kafka is not ready yet; retrying..."
    sleep 1
  done
  echo "Kafka is ready at ${bootstrap}."
}

start_kafka() {
  local container_name="$1"
  local port="$2"
  local bootstrap="$3"
  local log_dir="$4"
  echo "Starting Kafka Docker container ${container_name} from ${KAFKA_IMAGE} on ${bootstrap}..."
  docker run -d --rm \
    --name "$container_name" \
    -p "${port}:9092" \
    -v "${log_dir}:/tmp/kafka-logs" \
    -e KAFKA_NODE_ID=1 \
    -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_LISTENERS=EXTERNAL://:9092,INTERNAL://:9094,CONTROLLER://:9093 \
    -e KAFKA_ADVERTISED_LISTENERS="EXTERNAL://${bootstrap},INTERNAL://127.0.0.1:9094" \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=EXTERNAL:PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:9093 \
    -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS=1 \
    -e KAFKA_NUM_PARTITIONS=3 \
    "$KAFKA_IMAGE"
}

stop_kafka() {
  local container_name="$1"
  docker stop "$container_name" >/dev/null
}

run_filesystem_diff() {
  local fs_topic="${REAL_KAFKA_TOPIC:-diff.fs.$(date +%s).$$}"
  local rolled_topic="${REAL_KAFKA_ROLLED_TOPIC:-diff.fs.rolled.$(date +%s).$$}"
  local multi_topic="${REAL_KAFKA_MULTI_APPEND_TOPIC:-diff.fs.multi.$(date +%s).$$}"
  local initial_key="kafka-key"
  local initial_payload="kafka-value"
  local initial_timestamp_ms="12345"
  local append_key="kafkalite-key"
  local append_payload="kafkalite-value"
  local append_timestamp_ms="123456"
  local rolled_segment_bytes="2500"
  local rolled_first_timestamp_ms="1000"
  local rolled_second_timestamp_ms="2000"
  local rolled_third_timestamp_ms="3000"
  local multi_append_count="5"

  echo "Building fresh real Kafka filesystem baseline for ${fs_topic}..."
  start_kafka "$CONTAINER_NAME" "$KAFKA_PORT" "$BOOTSTRAP" "$KAFKA_LOG_DIR"
  wait_for_kafka "$KAFKA_PORT" "$BOOTSTRAP" "$CONTAINER_NAME"
  cargo run --features bench-internal --bin real_kafka_fixture -- \
    --bootstrap "$BOOTSTRAP" \
    create-topic-record \
    --topic "$fs_topic" \
    --key "$initial_key" \
    --payload "$initial_payload" \
    --timestamp-ms "$initial_timestamp_ms"
  stop_kafka "$CONTAINER_NAME"

  echo "Building fresh real-Kafka append reference for ${fs_topic}..."
  start_kafka "$REFERENCE_CONTAINER_NAME" "$KAFKA_REFERENCE_PORT" "$REFERENCE_BOOTSTRAP" "$KAFKA_APPEND_REFERENCE_DIR"
  wait_for_kafka "$KAFKA_REFERENCE_PORT" "$REFERENCE_BOOTSTRAP" "$REFERENCE_CONTAINER_NAME"
  cargo run --features bench-internal --bin real_kafka_fixture -- \
    --bootstrap "$REFERENCE_BOOTSTRAP" \
    create-topic-record \
    --topic "$fs_topic" \
    --key "$initial_key" \
    --payload "$initial_payload" \
    --timestamp-ms "$initial_timestamp_ms"
  cargo run --features bench-internal --bin real_kafka_fixture -- \
    --bootstrap "$REFERENCE_BOOTSTRAP" \
    create-topic-record \
    --topic "$fs_topic" \
    --key "$append_key" \
    --payload "$append_payload" \
    --timestamp-ms "$append_timestamp_ms"
  stop_kafka "$REFERENCE_CONTAINER_NAME"

  echo "Building rolled-segment baseline for ${rolled_topic}..."
  start_kafka "$CONTAINER_NAME" "$KAFKA_PORT" "$BOOTSTRAP" "$KAFKA_ROLLED_LOG_DIR"
  wait_for_kafka "$KAFKA_PORT" "$BOOTSTRAP" "$CONTAINER_NAME"
  cargo run --features bench-internal --bin real_kafka_fixture -- \
    --bootstrap "$BOOTSTRAP" \
    create-topic-record \
    --topic "$rolled_topic" \
    --topic-config "segment.bytes=${rolled_segment_bytes}" \
    --key "rolled-key-0" \
    --payload "X" \
    --payload-bytes 2400 \
    --timestamp-ms "$rolled_first_timestamp_ms"
  cargo run --features bench-internal --bin real_kafka_fixture -- \
    --bootstrap "$BOOTSTRAP" \
    create-topic-record \
    --topic "$rolled_topic" \
    --topic-config "segment.bytes=${rolled_segment_bytes}" \
    --key "rolled-key-1" \
    --payload "rolled-second" \
    --timestamp-ms "$rolled_second_timestamp_ms"
  stop_kafka "$CONTAINER_NAME"

  echo "Building rolled-segment append reference for ${rolled_topic}..."
  start_kafka "$REFERENCE_CONTAINER_NAME" "$KAFKA_REFERENCE_PORT" "$REFERENCE_BOOTSTRAP" "$KAFKA_ROLLED_APPEND_REFERENCE_DIR"
  wait_for_kafka "$KAFKA_REFERENCE_PORT" "$REFERENCE_BOOTSTRAP" "$REFERENCE_CONTAINER_NAME"
  cargo run --features bench-internal --bin real_kafka_fixture -- \
    --bootstrap "$REFERENCE_BOOTSTRAP" \
    create-topic-record \
    --topic "$rolled_topic" \
    --topic-config "segment.bytes=${rolled_segment_bytes}" \
    --key "rolled-key-0" \
    --payload "X" \
    --payload-bytes 2400 \
    --timestamp-ms "$rolled_first_timestamp_ms"
  cargo run --features bench-internal --bin real_kafka_fixture -- \
    --bootstrap "$REFERENCE_BOOTSTRAP" \
    create-topic-record \
    --topic "$rolled_topic" \
    --topic-config "segment.bytes=${rolled_segment_bytes}" \
    --key "rolled-key-1" \
    --payload "rolled-second" \
    --timestamp-ms "$rolled_second_timestamp_ms"
  cargo run --features bench-internal --bin real_kafka_fixture -- \
    --bootstrap "$REFERENCE_BOOTSTRAP" \
    create-topic-record \
    --topic "$rolled_topic" \
    --topic-config "segment.bytes=${rolled_segment_bytes}" \
    --key "rolled-key-2" \
    --payload "rolled-third" \
    --timestamp-ms "$rolled_third_timestamp_ms"
  stop_kafka "$REFERENCE_CONTAINER_NAME"

  echo "Building multi-append baseline for ${multi_topic}..."
  start_kafka "$CONTAINER_NAME" "$KAFKA_PORT" "$BOOTSTRAP" "$KAFKA_MULTI_APPEND_LOG_DIR"
  wait_for_kafka "$KAFKA_PORT" "$BOOTSTRAP" "$CONTAINER_NAME"
  cargo run --features bench-internal --bin real_kafka_fixture -- \
    --bootstrap "$BOOTSTRAP" \
    create-topic-record \
    --topic "$multi_topic" \
    --key "$initial_key" \
    --payload "$initial_payload" \
    --timestamp-ms "$initial_timestamp_ms"
  stop_kafka "$CONTAINER_NAME"

  echo "Building multi-append reference for ${multi_topic}..."
  start_kafka "$REFERENCE_CONTAINER_NAME" "$KAFKA_REFERENCE_PORT" "$REFERENCE_BOOTSTRAP" "$KAFKA_MULTI_APPEND_REFERENCE_DIR"
  wait_for_kafka "$KAFKA_REFERENCE_PORT" "$REFERENCE_BOOTSTRAP" "$REFERENCE_CONTAINER_NAME"
  cargo run --features bench-internal --bin real_kafka_fixture -- \
    --bootstrap "$REFERENCE_BOOTSTRAP" \
    create-topic-record \
    --topic "$multi_topic" \
    --key "$initial_key" \
    --payload "$initial_payload" \
    --timestamp-ms "$initial_timestamp_ms"
  for append_index in $(seq 1 "$multi_append_count"); do
    cargo run --features bench-internal --bin real_kafka_fixture -- \
      --bootstrap "$REFERENCE_BOOTSTRAP" \
      create-topic-record \
      --topic "$multi_topic" \
      --key "multi-key-${append_index}" \
      --payload "multi-value-${append_index}" \
      --timestamp-ms "$((123456 + append_index))"
  done
  echo "Stopping Kafka before filesystem byte checks..."
  stop_kafka "$REFERENCE_CONTAINER_NAME"

  if [[ -n "${DIFFERENTIAL_FS_COMMAND:-}" ]]; then
    echo "Running optional filesystem differential command..."
    REAL_KAFKA_BOOTSTRAP="$BOOTSTRAP" \
      REAL_KAFKA_RECOVERY_LOG_DIR="$NETWORK_KAFKA_LOG_DIR" \
      REAL_KAFKA_LOG_DIR="$KAFKA_LOG_DIR" \
      REAL_KAFKA_APPEND_REFERENCE_DIR="$KAFKA_APPEND_REFERENCE_DIR" \
      REAL_KAFKA_ROLLED_LOG_DIR="$KAFKA_ROLLED_LOG_DIR" \
      REAL_KAFKA_ROLLED_APPEND_REFERENCE_DIR="$KAFKA_ROLLED_APPEND_REFERENCE_DIR" \
      REAL_KAFKA_MULTI_APPEND_LOG_DIR="$KAFKA_MULTI_APPEND_LOG_DIR" \
      REAL_KAFKA_MULTI_APPEND_REFERENCE_DIR="$KAFKA_MULTI_APPEND_REFERENCE_DIR" \
      REAL_KAFKA_TOPIC="$fs_topic" \
      REAL_KAFKA_ROLLED_TOPIC="$rolled_topic" \
      REAL_KAFKA_MULTI_APPEND_TOPIC="$multi_topic" \
      bash -c "$DIFFERENTIAL_FS_COMMAND"
    return
  fi

  echo "Running real Kafka filesystem differential tests against ${KAFKA_LOG_DIR}..."
  REAL_KAFKA_LOG_DIR="$KAFKA_LOG_DIR" \
    REAL_KAFKA_RECOVERY_LOG_DIR="$NETWORK_KAFKA_LOG_DIR" \
    REAL_KAFKA_APPEND_REFERENCE_DIR="$KAFKA_APPEND_REFERENCE_DIR" \
    REAL_KAFKA_ROLLED_LOG_DIR="$KAFKA_ROLLED_LOG_DIR" \
    REAL_KAFKA_ROLLED_APPEND_REFERENCE_DIR="$KAFKA_ROLLED_APPEND_REFERENCE_DIR" \
    REAL_KAFKA_MULTI_APPEND_LOG_DIR="$KAFKA_MULTI_APPEND_LOG_DIR" \
    REAL_KAFKA_MULTI_APPEND_REFERENCE_DIR="$KAFKA_MULTI_APPEND_REFERENCE_DIR" \
    REAL_KAFKA_TOPIC="$fs_topic" \
    REAL_KAFKA_ROLLED_TOPIC="$rolled_topic" \
    REAL_KAFKA_MULTI_APPEND_TOPIC="$multi_topic" \
    cargo test kafka_filesystem
}

echo "Removing any stale Kafka Docker container ${CONTAINER_NAME}..."
docker stop "$CONTAINER_NAME" >/dev/null 2>&1 || true
echo "Removing any stale Kafka Docker container ${REFERENCE_CONTAINER_NAME}..."
docker stop "$REFERENCE_CONTAINER_NAME" >/dev/null 2>&1 || true

cd "$ROOT_DIR/rust/server"
mkdir -p "$NETWORK_KAFKA_LOG_DIR"
mkdir -p "$KAFKA_LOG_DIR"
mkdir -p "$KAFKA_APPEND_REFERENCE_DIR"
mkdir -p "$KAFKA_ROLLED_LOG_DIR"
mkdir -p "$KAFKA_ROLLED_APPEND_REFERENCE_DIR"
mkdir -p "$KAFKA_MULTI_APPEND_LOG_DIR"
mkdir -p "$KAFKA_MULTI_APPEND_REFERENCE_DIR"
chmod 0777 "$NETWORK_KAFKA_LOG_DIR"
chmod 0777 "$KAFKA_LOG_DIR"
chmod 0777 "$KAFKA_APPEND_REFERENCE_DIR"
chmod 0777 "$KAFKA_ROLLED_LOG_DIR"
chmod 0777 "$KAFKA_ROLLED_APPEND_REFERENCE_DIR"
chmod 0777 "$KAFKA_MULTI_APPEND_LOG_DIR"
chmod 0777 "$KAFKA_MULTI_APPEND_REFERENCE_DIR"

start_kafka "$CONTAINER_NAME" "$KAFKA_PORT" "$BOOTSTRAP" "$NETWORK_KAFKA_LOG_DIR"

wait_for_kafka "$KAFKA_PORT" "$BOOTSTRAP" "$CONTAINER_NAME"

echo "Running real-Kafka differential tests against ${BOOTSTRAP}..."
if ! REAL_KAFKA_BOOTSTRAP="$BOOTSTRAP" cargo test --test differential -- --test-threads=1; then
  dump_logs "$CONTAINER_NAME"
  exit 1
fi
stop_kafka "$CONTAINER_NAME"
run_filesystem_diff
echo "Differential checks completed successfully."
