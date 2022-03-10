import asyncio
from unittest import mock

import pytest
from consumer import KafkaConsumer
from janus import Queue as JanusQueue

from eventbus.config import ConsumerConfig, HttpSinkConfig, HttpSinkMethod
from eventbus.event import EventProcessStatus
from tests.utils import create_kafka_event_from_dict, create_kafka_message_from_dict


@pytest.fixture
def consumer_conf():
    consumer_conf = ConsumerConfig(
        id="test_consumer",
        listening_topics=["topic1"],
        kafka_config={
            "bootstrap.servers": "127.0.0.1:9093",
            "group.id": "test-group-1",
        },
        sink=HttpSinkConfig(
            url="/", method=HttpSinkMethod.POST, timeout=0.2, max_retry_times=3
        ),
    )
    yield consumer_conf


class MockInternalConsumer:
    def __init__(self, queue: JanusQueue):
        self._queue = queue

    def poll(self, timeout):
        try:
            return self._queue.sync_q.get(block=True, timeout=timeout)
        except:
            return None

    def commit(self, message=None, offsets=None, asynchronous=True):
        pass

    def close(self):
        pass


@pytest.mark.asyncio
async def test_send_events(consumer_conf):
    kafka_msg_queue = JanusQueue(maxsize=100)
    send_queue = JanusQueue(maxsize=100)

    consumer = KafkaConsumer(consumer_conf)
    consumer._internal_consumer = MockInternalConsumer(kafka_msg_queue)

    asyncio.create_task(
        consumer.fetch_events(send_queue)
    )  # trigger fetch events thread

    test_msg_1 = create_kafka_message_from_dict({"id": "e1"})
    kafka_msg_queue.sync_q.put(test_msg_1)
    event = await send_queue.async_q.get()
    assert event.id == "e1"
    assert send_queue.async_q.empty() == True

    test_msg_2 = create_kafka_message_from_dict({"id": "e2"})
    test_msg_3 = create_kafka_message_from_dict({"id": "e3"})
    kafka_msg_queue.sync_q.put(test_msg_2)
    kafka_msg_queue.sync_q.put(test_msg_3)
    event = await send_queue.async_q.get()
    assert event.id == "e2"
    event = await send_queue.async_q.get()
    assert event.id == "e3"
    assert send_queue.async_q.empty() == True

    test_msg_4 = create_kafka_message_from_dict({"published": "xxx"})
    kafka_msg_queue.sync_q.put(test_msg_4)
    assert send_queue.async_q.empty() == True

    await consumer.close()

    # assert _send_one_event.call_count == 3


@pytest.mark.asyncio
async def test_commit_events(mocker, consumer_conf):
    commit_queue = JanusQueue(maxsize=100)

    consumer = KafkaConsumer(consumer_conf)
    consumer._internal_consumer = MockInternalConsumer(None)
    commit = mocker.spy(consumer._internal_consumer, "commit")

    asyncio.create_task(
        consumer.commit_events(commit_queue)
    )  # trigger commmit events thread

    test_event_1 = create_kafka_event_from_dict({"id": "e1"})
    commit_queue.sync_q.put((test_event_1, EventProcessStatus.DONE))

    await asyncio.sleep(1)

    assert commit.call_count == 1

    await consumer.close()

    # assert _send_one_event.call_count == 3
