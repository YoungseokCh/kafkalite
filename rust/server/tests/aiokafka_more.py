import asyncio
import sys

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition

TIMEOUT_SECONDS = 8


async def seek_absolute_offset_matrix(bootstrap: str) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    try:
        await producer.send_and_wait("python.seek", b"first", partition=0)
        await producer.send_and_wait("python.seek", b"second", partition=0)
    finally:
        await producer.stop()

    consumer = AIOKafkaConsumer(bootstrap_servers=bootstrap)
    await consumer.start()
    try:
        topic_partition = TopicPartition("python.seek", 0)
        consumer.assign([topic_partition])
        consumer.seek(topic_partition, 1)

        message = await wait_for_message(consumer)
        assert message.offset == 1
        assert message.value == b"second"
    finally:
        await consumer.stop()


async def seek_to_end_tail_matrix(bootstrap: str) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    try:
        await producer.send_and_wait("python.tail", b"first", partition=0)
        await producer.send_and_wait("python.tail", b"second", partition=0)
    finally:
        await producer.stop()

    consumer = AIOKafkaConsumer(bootstrap_servers=bootstrap)
    await consumer.start()
    try:
        topic_partition = TopicPartition("python.tail", 0)
        consumer.assign([topic_partition])
        await consumer.seek_to_end(topic_partition)

        producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
        await producer.start()
        try:
            await producer.send_and_wait("python.tail", b"third", partition=0)
        finally:
            await producer.stop()

        message = await wait_for_message(consumer)
        assert message.offset == 2
        assert message.value == b"third"
    finally:
        await consumer.stop()


async def seek_to_beginning_rewind_matrix(bootstrap: str) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    try:
        await producer.send_and_wait("python.rewind", b"first", partition=0)
        await producer.send_and_wait("python.rewind", b"second", partition=0)
    finally:
        await producer.stop()

    consumer = AIOKafkaConsumer(bootstrap_servers=bootstrap)
    await consumer.start()
    try:
        topic_partition = TopicPartition("python.rewind", 0)
        consumer.assign([topic_partition])
        consumer.seek(topic_partition, 0)

        first = await wait_for_message(consumer)
        assert first.offset == 0
        assert first.value == b"first"

        await consumer.seek_to_beginning(topic_partition)

        rewound = await wait_for_message(consumer)
        assert rewound.offset == 0
        assert rewound.value == b"first"
    finally:
        await consumer.stop()


async def seek_to_end_tail_order_matrix(bootstrap: str) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    try:
        await producer.send_and_wait("python.tail-order", b"seed", partition=0)
    finally:
        await producer.stop()

    consumer = AIOKafkaConsumer(bootstrap_servers=bootstrap)
    await consumer.start()
    try:
        topic_partition = TopicPartition("python.tail-order", 0)
        consumer.assign([topic_partition])
        await consumer.seek_to_end(topic_partition)

        producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
        await producer.start()
        try:
            await producer.send_and_wait("python.tail-order", b"third", partition=0)
            await producer.send_and_wait("python.tail-order", b"fourth", partition=0)
        finally:
            await producer.stop()

        third = await wait_for_message(consumer)
        fourth = await wait_for_message(consumer)

        assert third.offset == 1
        assert third.value == b"third"
        assert fourth.offset == 2
        assert fourth.value == b"fourth"
    finally:
        await consumer.stop()


async def consume_then_seek_to_end_matrix(bootstrap: str) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    try:
        await producer.send_and_wait("python.tail-after-read", b"first", partition=0)
        await producer.send_and_wait("python.tail-after-read", b"second", partition=0)
    finally:
        await producer.stop()

    consumer = AIOKafkaConsumer(bootstrap_servers=bootstrap)
    await consumer.start()
    try:
        topic_partition = TopicPartition("python.tail-after-read", 0)
        consumer.assign([topic_partition])
        consumer.seek(topic_partition, 0)

        first = await wait_for_message(consumer)
        assert first.offset == 0
        assert first.value == b"first"

        await consumer.seek_to_end(topic_partition)

        producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
        await producer.start()
        try:
            await producer.send_and_wait(
                "python.tail-after-read", b"third", partition=0
            )
        finally:
            await producer.stop()

        tail = await wait_for_message(consumer)
        assert tail.offset == 2
        assert tail.value == b"third"
    finally:
        await consumer.stop()


async def consume_then_seek_absolute_matrix(bootstrap: str) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    try:
        await producer.send_and_wait("python.seek-after-read", b"first", partition=0)
        await producer.send_and_wait("python.seek-after-read", b"second", partition=0)
    finally:
        await producer.stop()

    consumer = AIOKafkaConsumer(bootstrap_servers=bootstrap)
    await consumer.start()
    try:
        topic_partition = TopicPartition("python.seek-after-read", 0)
        consumer.assign([topic_partition])
        consumer.seek(topic_partition, 0)

        first = await wait_for_message(consumer)
        assert first.offset == 0
        assert first.value == b"first"

        consumer.seek(topic_partition, 1)

        second = await wait_for_message(consumer)
        assert second.offset == 1
        assert second.value == b"second"
    finally:
        await consumer.stop()


async def consume_then_seek_to_beginning_order_matrix(bootstrap: str) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    try:
        await producer.send_and_wait("python.replay-order", b"first", partition=0)
        await producer.send_and_wait("python.replay-order", b"second", partition=0)
    finally:
        await producer.stop()

    consumer = AIOKafkaConsumer(bootstrap_servers=bootstrap)
    await consumer.start()
    try:
        topic_partition = TopicPartition("python.replay-order", 0)
        consumer.assign([topic_partition])
        consumer.seek(topic_partition, 0)

        first = await wait_for_message(consumer)
        assert first.offset == 0
        assert first.value == b"first"

        await consumer.seek_to_beginning(topic_partition)

        replayed_first = await wait_for_message(consumer)
        replayed_second = await wait_for_message(consumer)

        assert replayed_first.offset == 0
        assert replayed_first.value == b"first"
        assert replayed_second.offset == 1
        assert replayed_second.value == b"second"
    finally:
        await consumer.stop()


async def wait_for_message(consumer: AIOKafkaConsumer):
    return await asyncio.wait_for(consumer.getone(), timeout=TIMEOUT_SECONDS)


async def run_scenario(name: str, scenario) -> None:
    print(f"[aiokafka-more] starting {name}")
    await scenario
    print(f"[aiokafka-more] passed {name}")


async def main() -> int:
    bootstrap = sys.argv[1]
    await run_scenario(
        "seek_absolute_offset_matrix", seek_absolute_offset_matrix(bootstrap)
    )
    await run_scenario("seek_to_end_tail_matrix", seek_to_end_tail_matrix(bootstrap))
    await run_scenario(
        "seek_to_beginning_rewind_matrix",
        seek_to_beginning_rewind_matrix(bootstrap),
    )
    await run_scenario(
        "seek_to_end_tail_order_matrix",
        seek_to_end_tail_order_matrix(bootstrap),
    )
    await run_scenario(
        "consume_then_seek_to_end_matrix",
        consume_then_seek_to_end_matrix(bootstrap),
    )
    await run_scenario(
        "consume_then_seek_absolute_matrix",
        consume_then_seek_absolute_matrix(bootstrap),
    )
    await run_scenario(
        "consume_then_seek_to_beginning_order_matrix",
        consume_then_seek_to_beginning_order_matrix(bootstrap),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
