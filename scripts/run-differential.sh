#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KAFKA_IMAGE="${KAFKA_IMAGE:-apache/kafka:3.9.0}"
KAFKA_PORT="${REAL_KAFKA_PORT:-59092}"
CONTAINER_NAME="${REAL_KAFKA_CONTAINER:-kafkalite-real-kafka-${KAFKA_PORT}}"
BOOTSTRAP="127.0.0.1:${KAFKA_PORT}"
WAIT_SECONDS="${REAL_KAFKA_WAIT_SECONDS:-60}"
KAFKA_LOG_DIR="${REAL_KAFKA_LOG_DIR:-$(mktemp -d)}"
REMOVE_KAFKA_LOG_DIR=0

if [[ -z "${REAL_KAFKA_LOG_DIR:-}" ]]; then
  REMOVE_KAFKA_LOG_DIR=1
fi

cleanup() {
  echo "Stopping Kafka Docker container ${CONTAINER_NAME}..."
  docker stop "$CONTAINER_NAME" >/dev/null 2>&1 || true
  if [[ "$REMOVE_KAFKA_LOG_DIR" == "1" ]]; then
    rm -rf "$KAFKA_LOG_DIR"
  fi
}
trap cleanup EXIT

dump_logs() {
  echo "Kafka Docker logs for ${CONTAINER_NAME}:" >&2
  docker logs "$CONTAINER_NAME" >&2 || true
}

wait_for_kafka() {
  local deadline=$((SECONDS + WAIT_SECONDS))
  echo "Waiting up to ${WAIT_SECONDS}s for Kafka bootstrap ${BOOTSTRAP}..."
  until (echo >"/dev/tcp/127.0.0.1/${KAFKA_PORT}") >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      echo "Kafka bootstrap ${BOOTSTRAP} did not become reachable within ${WAIT_SECONDS}s" >&2
      dump_logs
      exit 1
    fi
    echo "Kafka is not ready yet; retrying..."
    sleep 1
  done
  echo "Kafka is ready at ${BOOTSTRAP}."
}

run_optional_filesystem_diff() {
  if [[ "${DIFFERENTIAL_FS_CHECK:-0}" != "1" ]]; then
    return
  fi
  local fs_topic="${REAL_KAFKA_TOPIC:-diff.fs.$(date +%s).$$}"
  echo "Creating real Kafka filesystem fixture topic ${fs_topic}..."
  REAL_KAFKA_BOOTSTRAP="$BOOTSTRAP" \
    REAL_KAFKA_TOPIC="$fs_topic" \
    cargo test create_real_kafka_filesystem_fixture -- --ignored
  echo "Stopping Kafka before filesystem byte checks..."
  docker stop "$CONTAINER_NAME" >/dev/null

  if [[ -n "${DIFFERENTIAL_FS_COMMAND:-}" ]]; then
    echo "Running optional filesystem differential command..."
    REAL_KAFKA_BOOTSTRAP="$BOOTSTRAP" \
      REAL_KAFKA_LOG_DIR="$KAFKA_LOG_DIR" \
      REAL_KAFKA_TOPIC="$fs_topic" \
      bash -c "$DIFFERENTIAL_FS_COMMAND"
    return
  fi

  echo "Running real Kafka filesystem differential tests against ${KAFKA_LOG_DIR}..."
  REAL_KAFKA_LOG_DIR="$KAFKA_LOG_DIR" \
    REAL_KAFKA_TOPIC="$fs_topic" \
    cargo test real_kafka_log_dir -- --ignored
}

echo "Removing any stale Kafka Docker container ${CONTAINER_NAME}..."
docker stop "$CONTAINER_NAME" >/dev/null 2>&1 || true

cd "$ROOT_DIR/rust/server"
mkdir -p "$KAFKA_LOG_DIR"
chmod 0777 "$KAFKA_LOG_DIR"

echo "Starting Kafka Docker container ${CONTAINER_NAME} from ${KAFKA_IMAGE} on ${BOOTSTRAP}..."
docker run -d --rm \
  --name "$CONTAINER_NAME" \
  -p "${KAFKA_PORT}:9092" \
  -v "${KAFKA_LOG_DIR}:/tmp/kafka-logs" \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://${BOOTSTRAP}" \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:9093 \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_NUM_PARTITIONS=3 \
  "$KAFKA_IMAGE"

wait_for_kafka

echo "Running real-Kafka differential tests against ${BOOTSTRAP}..."
if ! REAL_KAFKA_BOOTSTRAP="$BOOTSTRAP" cargo test --test differential; then
  dump_logs
  exit 1
fi
run_optional_filesystem_diff
echo "Differential checks completed successfully."
