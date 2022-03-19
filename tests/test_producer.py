import json
from threading import Thread

import pytest
import pytest_asyncio
from confluent_kafka import KafkaError, KafkaException, Message
from pytest_mock import MockFixture

from eventbus.producer import EventProducer
from tests.utils import create_event_from_dict, create_kafka_message_from_dict


@pytest_asyncio.fixture
async def mock_producer(mocker: MockFixture):
    p1_retry_counter = 0
    p2_retry_counter = 0

    def produce_mock(self, topic, value, **kwargs) -> None:
        nonlocal p1_retry_counter

        def delivery(err, msg):
            Thread(target=kwargs["on_delivery"], args=(err, msg), daemon=True).start()

        data = json.loads(value)

        if self.id == "p1":
            if data["title"] == "p1_normal":
                msg = create_kafka_message_from_dict({"key": "p1_normal"})
                delivery(None, msg)
            elif data["title"] == "p1_retry_succ":
                p1_retry_counter += 1
                if p1_retry_counter == 3:
                    delivery(
                        None, create_kafka_message_from_dict({"key": "p1_retry_succ"})
                    )
                else:
                    delivery(
                        KafkaException(
                            KafkaError(
                                error=KafkaError.BROKER_NOT_AVAILABLE,
                                fatal=False,
                                retriable=True,
                            )
                        ),
                        None,
                    )
            elif data["title"] == "p1_retry_fail":
                delivery(
                    KafkaException(
                        KafkaError(
                            error=KafkaError.BROKER_NOT_AVAILABLE,
                            fatal=False,
                            retriable=True,
                        )
                    ),
                    None,
                )
            else:
                delivery(RuntimeError("pl_fail"), None)

        elif self.id == "p2":
            if data["title"] == "p2_normal":
                msg = create_kafka_message_from_dict({"key": "p2_normal"})
                delivery(None, msg)
            else:
                delivery(RuntimeError("p2_fail"), None)

    mocker.patch("eventbus.config_watcher.watch_file")
    mocker.patch("eventbus.producer.KafkaProducer.init")
    mocker.patch("eventbus.producer.KafkaProducer.produce", produce_mock)

    producer = EventProducer("test", ["p1", "p2"])
    await producer.init()
    yield producer


@pytest.mark.asyncio
async def test_produce_succeed(mock_producer: EventProducer):
    e1 = create_event_from_dict({"payload": {"title": "p1_normal"}})
    msg = await mock_producer.produce("t1", e1)
    assert isinstance(msg, Message)
    assert msg.key() == "p1_normal"

    e1 = create_event_from_dict({"payload": {"title": "p2_normal"}})
    msg = await mock_producer.produce("t1", e1)
    assert isinstance(msg, Message)
    assert msg.key() == "p2_normal"


@pytest.mark.asyncio
async def test_produce_retry(mock_producer: EventProducer):
    with pytest.raises(RuntimeError):
        e1 = create_event_from_dict({"payload": {"title": "p1_retry_fail"}})
        msg = await mock_producer.produce("t1", e1)

    e1 = create_event_from_dict({"payload": {"title": "p1_retry_succ"}})
    msg = await mock_producer.produce("t1", e1)
    assert isinstance(msg, Message)
    assert msg.key() == "p1_retry_succ"
