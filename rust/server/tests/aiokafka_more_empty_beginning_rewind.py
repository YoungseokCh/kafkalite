import asyncio
import sys

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition

TIMEOUT_SECONDS = 8


async def empty_seek_to_beginning_then_seek_to_beginning_matrix(bootstrap: str) -> None:
    consumer = AIOKafkaConsumer(bootstrap_servers=bootstrap)
    await consumer.start()
    try:
        topic_partition = TopicPartition("python.empty-beginning-rewind", 0)
        consumer.assign([topic_partition])
        await consumer.seek_to_beginning(topic_partition)

        producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
        await producer.start()
        try:
            await producer.send_and_wait(
                "python.empty-beginning-rewind", b"first", partition=0
            )
            await producer.send_and_wait(
                "python.empty-beginning-rewind", b"second", partition=0
            )
        finally:
            await producer.stop()

        first = await wait_for_message(consumer)
        assert first.offset == 0 and first.value == b"first"

        await consumer.seek_to_beginning(topic_partition)

        replayed_first = await wait_for_message(consumer)
        replayed_second = await wait_for_message(consumer)
        assert replayed_first.offset == 0 and replayed_first.value == b"first"
        assert replayed_second.offset == 1 and replayed_second.value == b"second"
    finally:
        await consumer.stop()


async def wait_for_message(consumer: AIOKafkaConsumer):
    return await asyncio.wait_for(consumer.getone(), timeout=TIMEOUT_SECONDS)


async def main() -> int:
    await empty_seek_to_beginning_then_seek_to_beginning_matrix(sys.argv[1])
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
