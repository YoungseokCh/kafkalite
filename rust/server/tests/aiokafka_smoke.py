import asyncio
import sys

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition


async def main() -> int:
    bootstrap = sys.argv[1]

    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    try:
        metadata = await producer.send_and_wait(
            "python.events", b"python-payload", key=b"python-key"
        )
        assert metadata.partition == 0
        assert metadata.offset == 0
    finally:
        await producer.stop()

    consumer = AIOKafkaConsumer(
        "python.events",
        bootstrap_servers=bootstrap,
        group_id="python-group",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await consumer.start()
    try:
        message = await asyncio.wait_for(consumer.getone(), timeout=8)
        assert message.value == b"python-payload"
        assert message.key == b"python-key"
        await consumer.commit({TopicPartition("python.events", 0): message.offset + 1})
    finally:
        await consumer.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
