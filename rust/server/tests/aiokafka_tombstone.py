import asyncio
import sys

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition

TIMEOUT_SECONDS = 8


async def tombstone_roundtrip_matrix(bootstrap: str) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    try:
        metadata = await producer.send_and_wait(
            "python.tombstone",
            None,
            key=b"tombstone-key",
            partition=0,
        )
        assert metadata.partition == 0
        assert metadata.offset == 0
    finally:
        await producer.stop()

    consumer = AIOKafkaConsumer(bootstrap_servers=bootstrap)
    await consumer.start()
    try:
        topic_partition = TopicPartition("python.tombstone", 0)
        consumer.assign([topic_partition])
        consumer.seek(topic_partition, 0)
        message = await asyncio.wait_for(consumer.getone(), timeout=TIMEOUT_SECONDS)
        assert message.offset == 0
        assert message.key == b"tombstone-key"
        assert message.value is None
    finally:
        await consumer.stop()


async def main() -> int:
    await tombstone_roundtrip_matrix(sys.argv[1])
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
