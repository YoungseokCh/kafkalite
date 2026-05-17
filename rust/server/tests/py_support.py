import asyncio

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import TopicPartition

TIMEOUT_SECONDS = 8


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
