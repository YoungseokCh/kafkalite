import asyncio
import sys

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition

TIMEOUT_SECONDS = 8


async def consume_then_seek_to_end_order_matrix(bootstrap: str) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    try:
        await producer.send_and_wait(
            "python.tail-after-read-order", b"first", partition=0
        )
        await producer.send_and_wait(
            "python.tail-after-read-order", b"second", partition=0
        )
    finally:
        await producer.stop()

    consumer = AIOKafkaConsumer(bootstrap_servers=bootstrap)
    await consumer.start()
    try:
        topic_partition = TopicPartition("python.tail-after-read-order", 0)
        consumer.assign([topic_partition])
        consumer.seek(topic_partition, 0)
        await wait_for_message(consumer)
        await consumer.seek_to_end(topic_partition)

        producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
        await producer.start()
        try:
            await producer.send_and_wait(
                "python.tail-after-read-order", b"third", partition=0
            )
            await producer.send_and_wait(
                "python.tail-after-read-order", b"fourth", partition=0
            )
        finally:
            await producer.stop()

        third = await wait_for_message(consumer)
        fourth = await wait_for_message(consumer)
        assert third.offset == 2 and third.value == b"third"
        assert fourth.offset == 3 and fourth.value == b"fourth"
    finally:
        await consumer.stop()


async def wait_for_message(consumer: AIOKafkaConsumer):
    return await asyncio.wait_for(consumer.getone(), timeout=TIMEOUT_SECONDS)


async def main() -> int:
    await consume_then_seek_to_end_order_matrix(sys.argv[1])
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
