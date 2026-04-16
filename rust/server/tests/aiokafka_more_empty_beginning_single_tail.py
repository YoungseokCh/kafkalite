import asyncio
import sys

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition

TIMEOUT_SECONDS = 8


async def empty_seek_to_beginning_then_single_tail_matrix(bootstrap: str) -> None:
    consumer = AIOKafkaConsumer(bootstrap_servers=bootstrap)
    await consumer.start()
    try:
        topic_partition = TopicPartition("python.empty-beginning-single-tail", 0)
        consumer.assign([topic_partition])
        await consumer.seek_to_beginning(topic_partition)

        producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
        await producer.start()
        try:
            await producer.send_and_wait(
                "python.empty-beginning-single-tail", b"first", partition=0
            )
        finally:
            await producer.stop()

        first = await wait_for_message(consumer)
        assert first.offset == 0 and first.value == b"first"

        await consumer.seek_to_end(topic_partition)

        producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
        await producer.start()
        try:
            await producer.send_and_wait(
                "python.empty-beginning-single-tail", b"second", partition=0
            )
        finally:
            await producer.stop()

        second = await wait_for_message(consumer)
        assert second.offset == 1 and second.value == b"second"
    finally:
        await consumer.stop()


async def wait_for_message(consumer: AIOKafkaConsumer):
    return await asyncio.wait_for(consumer.getone(), timeout=TIMEOUT_SECONDS)


async def main() -> int:
    await empty_seek_to_beginning_then_single_tail_matrix(sys.argv[1])
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
