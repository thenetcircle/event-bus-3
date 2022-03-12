import asyncio
from unittest import mock

import pytest
from consumer import ConsumerCoordinator, KafkaConsumer
from janus import Queue as JanusQueue

from eventbus.config import ConsumerConfig, HttpSinkConfig, HttpSinkMethod
from eventbus.event import EventProcessStatus, KafkaEvent
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
        self.committed_msgs = []

    def poll(self, timeout):
        try:
            return self._queue.sync_q.get(block=True, timeout=timeout)
        except:
            return None

    def commit(self, message=None, offsets=None, asynchronous=True):
        self.committed_msgs.append((message, offsets))

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
    commit_spy = mocker.spy(consumer._internal_consumer, "commit")

    asyncio.create_task(
        consumer.commit_events(commit_queue)
    )  # trigger commmit events thread

    test_event_1 = create_kafka_event_from_dict({"id": "e1"})
    test_event_2 = create_kafka_event_from_dict({"id": "e2"})
    commit_queue.sync_q.put((test_event_1, EventProcessStatus.DONE))
    commit_queue.sync_q.put((test_event_2, EventProcessStatus.DONE))

    await asyncio.sleep(0.1)
    await consumer.close()
    assert commit_spy.call_count == 2

    # assert _send_one_event.call_count == 3


@pytest.mark.asyncio
async def test_consumer_coordinator(mocker, consumer_conf):
    async def mock_send_event(self, event: KafkaEvent):
        return event, EventProcessStatus.DONE

    mocker.patch("eventbus.sink.HttpSink.send_event", mock_send_event)

    consumer = KafkaConsumer(consumer_conf)
    kafka_msg_queue = JanusQueue(maxsize=100)
    mock_consumer = MockInternalConsumer(kafka_msg_queue)
    consumer._internal_consumer = mock_consumer
    # commit_spy = mocker.spy(consumer._internal_consumer, "commit")

    coordinator = ConsumerCoordinator(consumer_conf)
    coordinator._consumer = consumer
    coordinator._send_queue = JanusQueue(maxsize=100)
    coordinator._commit_queue = JanusQueue(maxsize=100)

    # let's do this two times to check if the coordinator are able to rerun
    asyncio.create_task(coordinator.run())

    # check the whole pipeline, if can get all events in commit method
    test_events_amount = 10
    for i in range(test_events_amount):
        kafka_msg_queue.sync_q.put(
            create_kafka_message_from_dict({"id": f"e{i+1}", "offset": i + 1})
        )

    await asyncio.sleep(0.1)
    await coordinator.cancel()
    assert len(mock_consumer.committed_msgs) == test_events_amount

    # check how it acts when new events come after the coordinator cancelled
    kafka_msg_queue.sync_q.put(
        create_kafka_message_from_dict({"id": f"ne", "offset": -1})
    )
    await asyncio.sleep(0.1)
    assert len(mock_consumer.committed_msgs) == test_events_amount

    # check the order of received commits
    assert [m[1][0].offset for m in mock_consumer.committed_msgs] == [
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
    ]
