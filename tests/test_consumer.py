import asyncio

import janus
import pytest
from aiohttp import web
from loguru import logger
from pytest_mock import MockFixture

from eventbus.config import ConsumerConfig, HttpSinkConfig, HttpSinkMethod
from eventbus.consumer import KafkaConsumer
from tests.utils import create_kafka_event_from_dict


@pytest.fixture
def consumer():
    consumer_conf = ConsumerConfig(
        id="test_consumer",
        subscribe_events=["test_event1"],
        kafka_config={},
        sink=HttpSinkConfig(
            url="/", method=HttpSinkMethod.POST, timeout=0.2, max_retry_times=3
        ),
    )
    yield KafkaConsumer(config=consumer_conf, topics=["test_topic1"])
    # mocker.patch.object(consumer, "_fetch_events", autospec=True)


@pytest.mark.asyncio
async def test_send_events(mocker: MockFixture, consumer):

    test_event = create_kafka_event_from_dict({})

    send_queue = janus.Queue(maxsize=100)
    commit_queue = janus.Queue(maxsize=100)

    # test logic:
    # put bunch of events into send_queue,

    # spy_send_one_event = mocker.spy(consumer, "_send_one_event")
    _send_one_event = mocker.patch.object(consumer, "_send_one_event")

    send_queue.sync_q.put(test_event)
    send_queue.sync_q.put(test_event)
    send_queue.sync_q.put(test_event)

    try:
        await asyncio.wait_for(
            consumer._wait_and_deliver_events(send_queue.async_q, commit_queue.async_q),
            1,
        )
    except asyncio.TimeoutError:
        pass

    # spy_send_one_event.assert_called_once()
    assert _send_one_event.call_count == 3
