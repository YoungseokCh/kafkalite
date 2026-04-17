import asyncio
import sys

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition

TIMEOUT_SECONDS = 8


async def mixed_shapes_roundtrip_matrix(bootstrap: str) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    try:
        first = await producer.send_and_wait(
            "python.mixed-shapes",
            b"payload-one",
            key=b"key-one",
            partition=0,
        )
        second = await producer.send_and_wait(
            "python.mixed-shapes",
            None,
            key=b"key-two",
            partition=0,
        )
        third = await producer.send_and_wait(
            "python.mixed-shapes",
            b"payload-three",
            partition=0,
        )
        assert (first.offset, second.offset, third.offset) == (0, 1, 2)
    finally:
        await producer.stop()

    consumer = AIOKafkaConsumer(bootstrap_servers=bootstrap)
    await consumer.start()
    try:
        topic_partition = TopicPartition("python.mixed-shapes", 0)
        consumer.assign([topic_partition])
        consumer.seek(topic_partition, 0)

        first_message = await asyncio.wait_for(
            consumer.getone(), timeout=TIMEOUT_SECONDS
        )
        second_message = await asyncio.wait_for(
            consumer.getone(), timeout=TIMEOUT_SECONDS
        )
        third_message = await asyncio.wait_for(
            consumer.getone(), timeout=TIMEOUT_SECONDS
        )

        assert first_message.key == b"key-one"
        assert first_message.value == b"payload-one"
        assert second_message.key == b"key-two"
        assert second_message.value is None
        assert third_message.key is None
        assert third_message.value == b"payload-three"
    finally:
        await consumer.stop()


async def main() -> int:
    await mixed_shapes_roundtrip_matrix(sys.argv[1])
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
