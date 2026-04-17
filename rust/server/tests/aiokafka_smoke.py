import asyncio
import json
import sys

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition

TIMEOUT_SECONDS = 8


async def basic_produce_consume(bootstrap: str) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    try:
        metadata = await producer.send_and_wait(
            "python.events",
            b"python-payload",
            key=b"python-key",
            partition=0,
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
        message = await wait_for_message(consumer)
        assert message.partition == 0
        assert message.offset == 0
        assert message.value == b"python-payload"
        assert message.key == b"python-key"
        topic_partition = TopicPartition("python.events", 0)
        await consumer.commit({topic_partition: message.offset + 1})
        committed = await consumer.committed(topic_partition)
        assert committed == 1
    finally:
        await consumer.stop()


async def committed_offset_reload(bootstrap: str) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    try:
        for payload in (b"resume-first", b"resume-second"):
            await producer.send_and_wait(
                "python.resume",
                payload,
                key=b"resume-key",
                partition=0,
            )
    finally:
        await producer.stop()

    first_consumer = AIOKafkaConsumer(
        "python.resume",
        bootstrap_servers=bootstrap,
        group_id="python-resume-group",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await first_consumer.start()
    try:
        first_message = await wait_for_message(first_consumer)
        assert first_message.value == b"resume-first"
        topic_partition = TopicPartition("python.resume", 0)
        await first_consumer.commit({topic_partition: first_message.offset + 1})
    finally:
        await first_consumer.stop()

    resumed_consumer = AIOKafkaConsumer(
        bootstrap_servers=bootstrap,
        group_id="python-resume-group",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await resumed_consumer.start()
    try:
        topic_partition = TopicPartition("python.resume", 0)
        committed = await resumed_consumer.committed(topic_partition)
        assert committed == 1
    finally:
        await resumed_consumer.stop()


async def multi_partition_matrix(bootstrap: str) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    try:
        partition_one = await producer.send_and_wait(
            "python.multi",
            b"partition-one",
            key=b"partition-one-key",
            partition=1,
        )
        partition_two = await producer.send_and_wait(
            "python.multi",
            b"partition-two",
            key=b"partition-two-key",
            partition=2,
        )
        assert partition_one.partition == 1
        assert partition_one.offset == 0
        assert partition_two.partition == 2
        assert partition_two.offset == 0
        assert await producer.partitions_for("python.multi") == {0, 1, 2}
    finally:
        await producer.stop()

    metadata_consumer = AIOKafkaConsumer(bootstrap_servers=bootstrap)
    await metadata_consumer.start()
    try:
        partition_one_tp = TopicPartition("python.multi", 1)
        partition_two_tp = TopicPartition("python.multi", 2)

        beginnings = await metadata_consumer.beginning_offsets(
            [partition_one_tp, partition_two_tp]
        )
        ends = await metadata_consumer.end_offsets([partition_one_tp, partition_two_tp])
        assert beginnings[partition_one_tp] == 0
        assert beginnings[partition_two_tp] == 0
        assert ends[partition_one_tp] == 1
        assert ends[partition_two_tp] == 1
    finally:
        await metadata_consumer.stop()

    partition_one_consumer = AIOKafkaConsumer(bootstrap_servers=bootstrap)
    await partition_one_consumer.start()
    try:
        topic_partition = TopicPartition("python.multi", 1)
        partition_one_consumer.assign([topic_partition])
        partition_one_consumer.seek(topic_partition, 0)
        message = await wait_for_message(partition_one_consumer)
        assert message.partition == 1
        assert message.value == b"partition-one"
    finally:
        await partition_one_consumer.stop()

    partition_two_consumer = AIOKafkaConsumer(bootstrap_servers=bootstrap)
    await partition_two_consumer.start()
    try:
        topic_partition = TopicPartition("python.multi", 2)
        partition_two_consumer.assign([topic_partition])
        partition_two_consumer.seek(topic_partition, 0)
        message = await wait_for_message(partition_two_consumer)
        assert message.partition == 2
        assert message.value == b"partition-two"
    finally:
        await partition_two_consumer.stop()


async def invalid_partition_rejected(bootstrap: str) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    try:
        try:
            await producer.send_and_wait(
                "python.invalid",
                b"invalid-partition",
                key=b"invalid-key",
                partition=3,
            )
        except AssertionError as err:
            assert str(err) == "Unrecognized partition"
            return
        raise AssertionError("expected aiokafka to reject partition 3")
    finally:
        await producer.stop()


async def batch_getmany_matrix(bootstrap: str) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    try:
        await producer.send_and_wait("python.batch", b"batch-one", partition=0)
    finally:
        await producer.stop()

    consumer = AIOKafkaConsumer(bootstrap_servers=bootstrap)
    await consumer.start()
    try:
        topic_partition = TopicPartition("python.batch", 0)
        consumer.assign([topic_partition])
        consumer.seek(topic_partition, 0)
        records = await wait_for_records(consumer, topic_partition, 1)
        assert [record.offset for record in records] == [0]
        assert [record.value for record in records] == [b"batch-one"]
    finally:
        await consumer.stop()


async def adapter_style_adapter_matrix(bootstrap: str) -> None:
    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
    )
    await producer.start()
    try:
        first = await producer.send_and_wait(
            "adapter.event_ready.sample.processor",
            value={"event_id": "evt-1", "status": "ready"},
            key="project:processor",
            headers=[("adapter-project", b"sample-project")],
            partition=0,
        )
        second = await producer.send_and_wait(
            "adapter.event_ready.sample.processor",
            value={"event_id": "evt-2", "status": "ready"},
            key="project:processor",
            partition=0,
        )
        assert first.partition == 0
        assert second.partition == 0
    finally:
        await producer.stop()

    group_id = "processor.sample-project.sample-processor"
    consumer = AIOKafkaConsumer(
        "adapter.event_ready.sample.processor",
        bootstrap_servers=bootstrap,
        group_id=group_id,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await consumer.start()
    try:
        first_message = await asyncio.wait_for(
            consumer.__anext__(), timeout=TIMEOUT_SECONDS
        )
        assert first_message.offset == 0
        assert first_message.value == {"event_id": "evt-1", "status": "ready"}
        assert first_message.key == b"project:processor"
        partition = TopicPartition(first_message.topic, first_message.partition)
        await consumer.commit({partition: first_message.offset + 1})
    finally:
        await consumer.stop()

    resumed = AIOKafkaConsumer(
        "adapter.event_ready.sample.processor",
        bootstrap_servers=bootstrap,
        group_id=group_id,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await resumed.start()
    try:
        resumed_partition = TopicPartition("adapter.event_ready.sample.processor", 0)
        resumed_committed = await resumed.committed(resumed_partition)
        assert resumed_committed == 1
    finally:
        await resumed.stop()


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


async def run_scenario(name: str, scenario) -> None:
    print(f"[aiokafka] starting {name}")
    await scenario
    print(f"[aiokafka] passed {name}")


async def main() -> int:
    bootstrap = sys.argv[1]
    await run_scenario("basic_produce_consume", basic_produce_consume(bootstrap))
    await run_scenario("committed_offset_reload", committed_offset_reload(bootstrap))
    await run_scenario(
        "adapter_style_adapter_matrix", adapter_style_adapter_matrix(bootstrap)
    )
    await run_scenario("multi_partition_matrix", multi_partition_matrix(bootstrap))
    await run_scenario("batch_getmany_matrix", batch_getmany_matrix(bootstrap))
    await run_scenario(
        "invalid_partition_rejected", invalid_partition_rejected(bootstrap)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
