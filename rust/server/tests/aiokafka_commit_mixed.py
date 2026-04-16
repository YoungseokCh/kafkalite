import asyncio
import sys

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition

TIMEOUT_SECONDS = 8


async def committed_resume_after_tombstone_matrix(bootstrap: str) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    try:
        await producer.send_and_wait(
            "python.commit-mixed",
            b"payload-one",
            key=b"key-one",
            partition=0,
        )
        await producer.send_and_wait(
            "python.commit-mixed",
            None,
            key=b"key-two",
            partition=0,
        )
        await producer.send_and_wait(
            "python.commit-mixed",
            b"payload-three",
            partition=0,
        )
    finally:
        await producer.stop()

    group_id = "python-commit-mixed-group"
    topic_partition = TopicPartition("python.commit-mixed", 0)

    first_consumer = AIOKafkaConsumer(
        "python.commit-mixed",
        bootstrap_servers=bootstrap,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await first_consumer.start()
    try:
        first = await wait_for_message(first_consumer)
        second = await wait_for_message(first_consumer)

        assert first.key == b"key-one"
        assert first.value == b"payload-one"
        assert second.key == b"key-two"
        assert second.value is None

        await first_consumer.commit({topic_partition: second.offset + 1})
    finally:
        await first_consumer.stop()

    resumed_consumer = AIOKafkaConsumer(
        "python.commit-mixed",
        bootstrap_servers=bootstrap,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await resumed_consumer.start()
    try:
        committed = await resumed_consumer.committed(topic_partition)
        assert committed == 2

        resumed = await wait_for_message(resumed_consumer)
        assert resumed.offset == 2
        assert resumed.key is None
        assert resumed.value == b"payload-three"
    finally:
        await resumed_consumer.stop()


async def wait_for_message(consumer: AIOKafkaConsumer):
    return await asyncio.wait_for(consumer.getone(), timeout=TIMEOUT_SECONDS)


async def main() -> int:
    await committed_resume_after_tombstone_matrix(sys.argv[1])
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
