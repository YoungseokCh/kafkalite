import asyncio
import sys

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition

TIMEOUT_SECONDS = 8


async def header_roundtrip_matrix(bootstrap: str) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    try:
        await producer.send_and_wait(
            "python.headers",
            b"payload",
            headers=[("trace-id", b"abc123"), ("empty", None)],
            partition=0,
        )
    finally:
        await producer.stop()

    consumer = AIOKafkaConsumer(bootstrap_servers=bootstrap)
    await consumer.start()
    try:
        topic_partition = TopicPartition("python.headers", 0)
        consumer.assign([topic_partition])
        consumer.seek(topic_partition, 0)
        message = await asyncio.wait_for(consumer.getone(), timeout=TIMEOUT_SECONDS)
        assert message.value == b"payload"
        assert dict(message.headers) == {"trace-id": b"abc123", "empty": None}
    finally:
        await consumer.stop()


async def main() -> int:
    await header_roundtrip_matrix(sys.argv[1])
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
