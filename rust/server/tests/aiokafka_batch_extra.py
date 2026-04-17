import asyncio
import sys

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition

TIMEOUT_SECONDS = 8


async def timestamp_visibility_matrix(bootstrap: str) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    try:
        await producer.send_and_wait("python.timestamps", b"first", partition=0)
        await producer.send_and_wait("python.timestamps", b"second", partition=0)
    finally:
        await producer.stop()

    consumer = AIOKafkaConsumer(bootstrap_servers=bootstrap)
    await consumer.start()
    try:
        topic_partition = TopicPartition("python.timestamps", 0)
        consumer.assign([topic_partition])
        consumer.seek(topic_partition, 0)

        first = await wait_for_message(consumer)
        second = await wait_for_message(consumer)

        assert first.timestamp > 0
        assert second.timestamp > 0
        assert second.timestamp >= first.timestamp
    finally:
        await consumer.stop()


async def tombstone_with_headers_matrix(bootstrap: str) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    try:
        await producer.send_and_wait(
            "python.tombstone-headers",
            None,
            key=b"tombstone-key",
            headers=[("trace-id", b"abc123"), ("empty", None)],
            partition=0,
        )
    finally:
        await producer.stop()

    consumer = AIOKafkaConsumer(bootstrap_servers=bootstrap)
    await consumer.start()
    try:
        topic_partition = TopicPartition("python.tombstone-headers", 0)
        consumer.assign([topic_partition])
        consumer.seek(topic_partition, 0)
        message = await wait_for_message(consumer)

        assert message.key == b"tombstone-key"
        assert message.value is None
        assert dict(message.headers) == {"trace-id": b"abc123", "empty": None}
    finally:
        await consumer.stop()


async def mixed_shapes_getmany_matrix(bootstrap: str) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    try:
        await producer.send_and_wait(
            "python.mixed-getmany",
            b"payload-one",
            key=b"key-one",
            partition=0,
        )
        await producer.send_and_wait(
            "python.mixed-getmany",
            None,
            key=b"key-two",
            partition=0,
        )
        await producer.send_and_wait(
            "python.mixed-getmany",
            b"payload-three",
            partition=0,
        )
    finally:
        await producer.stop()

    consumer = AIOKafkaConsumer(bootstrap_servers=bootstrap)
    await consumer.start()
    try:
        topic_partition = TopicPartition("python.mixed-getmany", 0)
        consumer.assign([topic_partition])
        consumer.seek(topic_partition, 0)
        records = await wait_for_records(consumer, topic_partition, 3)

        assert [record.key for record in records] == [b"key-one", b"key-two", None]
        assert [record.value for record in records] == [
            b"payload-one",
            None,
            b"payload-three",
        ]
    finally:
        await consumer.stop()


async def wait_for_message(consumer: AIOKafkaConsumer):
    return await asyncio.wait_for(consumer.getone(), timeout=TIMEOUT_SECONDS)


async def wait_for_records(
    consumer: AIOKafkaConsumer, topic_partition: TopicPartition, expected_count: int
):
    deadline = asyncio.get_running_loop().time() + TIMEOUT_SECONDS
    records = []
    while len(records) < expected_count:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            break
        batch = await consumer.getmany(
            topic_partition, timeout_ms=250, max_records=expected_count
        )
        records.extend(batch.get(topic_partition, []))
    if len(records) < expected_count:
        raise AssertionError(
            f"expected {expected_count} records for {topic_partition}, got {len(records)}"
        )
    return records[:expected_count]


async def run_scenario(name: str, scenario) -> None:
    print(f"[aiokafka-batch-extra] starting {name}")
    await scenario
    print(f"[aiokafka-batch-extra] passed {name}")


async def main() -> int:
    bootstrap = sys.argv[1]
    await run_scenario(
        "timestamp_visibility_matrix", timestamp_visibility_matrix(bootstrap)
    )
    await run_scenario(
        "tombstone_with_headers_matrix", tombstone_with_headers_matrix(bootstrap)
    )
    await run_scenario(
        "mixed_shapes_getmany_matrix", mixed_shapes_getmany_matrix(bootstrap)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
