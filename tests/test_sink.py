from queue import Queue

import pytest
from pytest_mock import MockFixture

from eventbus.config import ConsumerConfig, HttpSinkConfig, HttpSinkMethod
from eventbus.consumer import KafkaConsumer
from eventbus.event import KafkaEvent
from tests.utils import create_event_from_dict


@pytest.mark.asyncio
async def test_send_events(mocker: MockFixture):
    consumer_conf = ConsumerConfig(
        id="test_consumer",
        subscribe_events=["test_event1"],
        kafka_config={},
        sink=HttpSinkConfig(url="http://localhost", method=HttpSinkMethod.POST),
    )
    consumer = KafkaConsumer(consumer_conf=consumer_conf, topics=["test_topic1"])
    mocker.patch.object(consumer, "_fetch_events", autospec=True)

    test_event = KafkaEvent(
        kafka_msg=mocker.patch("confluent_kafka.cimpl.Message", autospec=True),
        event=create_event_from_dict({}),
    )

    send_queue = Queue(maxsize=100)
    commit_queue = Queue(maxsize=100)

    # test logic:
    # put bunch of events into send_queue,

    spy_send_one_event = mocker.spy(consumer, "_send_one_event")

    consumer._send_queue.put(test_event, block=True)
    spy_send_one_event.assert_called_once()
