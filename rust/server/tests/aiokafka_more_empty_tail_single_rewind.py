import asyncio
import sys

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition

TIMEOUT_SECONDS = 8


async def empty_seek_to_end_then_single_rewind_matrix(bootstrap: str) -> None:
    consumer = AIOKafkaConsumer(bootstrap_servers=bootstrap)
    await consumer.start()
    try:
        topic_partition = TopicPartition("python.empty-tail-single-rewind", 0)
        consumer.assign([topic_partition])
        await consumer.seek_to_end(topic_partition)

        producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
        await producer.start()
        try:
            await producer.send_and_wait(
                "python.empty-tail-single-rewind", b"first", partition=0
            )
        finally:
            await producer.stop()

        first = await wait_for_message(consumer)
        assert first.offset == 0 and first.value == b"first"

        await consumer.seek_to_beginning(topic_partition)

        replayed = await wait_for_message(consumer)
        assert replayed.offset == 0 and replayed.value == b"first"
    finally:
        await consumer.stop()


async def wait_for_message(consumer: AIOKafkaConsumer):
    return await asyncio.wait_for(consumer.getone(), timeout=TIMEOUT_SECONDS)


async def main() -> int:
    await empty_seek_to_end_then_single_rewind_matrix(sys.argv[1])
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
