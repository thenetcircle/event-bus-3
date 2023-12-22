import json
from threading import Thread

import pytest
import pytest_asyncio
from confluent_kafka import KafkaError, KafkaException, Message
from pytest_mock import MockFixture
from utils import create_event_from_dict, create_kafka_message_from_dict

from eventbus import config
from eventbus.config import ProducerConfig, UseProducersConfig
from eventbus.confluent_kafka_producer import ConfluentKafkaProducer


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
                        KafkaError(
                            error=KafkaError.BROKER_NOT_AVAILABLE,
                            fatal=False,
                            retriable=True,
                        ),
                        None,
                    )
            elif data["title"] == "p1_retry_fail":
                delivery(
                    KafkaError(
                        error=KafkaError.BROKER_NOT_AVAILABLE,
                        fatal=False,
                        retriable=True,
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

    mocker.patch("eventbus.producer.KafkaProducer.init")
    mocker.patch("eventbus.producer.KafkaProducer.produce", produce_mock)

    producer = ConfluentKafkaProducer(
        "test", UseProducersConfig(producer_ids=["p1", "p2"])
    )
    await producer.init()
    yield producer
    await producer.close()


@pytest.mark.asyncio
async def test_produce_succeed(mock_producer: ConfluentKafkaProducer):
    e1 = create_event_from_dict({"payload": {"title": "p1_normal"}})
    msg = await mock_producer.produce("t1", e1)
    assert isinstance(msg, Message)
    assert msg.key() == "p1_normal"

    e1 = create_event_from_dict({"payload": {"title": "p2_normal"}})
    msg = await mock_producer.produce("t1", e1)
    assert isinstance(msg, Message)
    assert msg.key() == "p2_normal"


@pytest.mark.asyncio
async def test_produce_retry(mock_producer: ConfluentKafkaProducer):
    with pytest.raises(KafkaException):
        e1 = create_event_from_dict({"payload": {"title": "p1_retry_fail"}})
        msg = await mock_producer.produce("t1", e1)

    e1 = create_event_from_dict({"payload": {"title": "p1_retry_succ"}})
    msg = await mock_producer.produce("t1", e1)
    assert isinstance(msg, Message)
    assert msg.key() == "p1_retry_succ"


@pytest.mark.asyncio
async def test_config_change_signal(mocker: MockFixture):
    mocker.patch("eventbus.producer.KafkaProducer.init")
    producer = ConfluentKafkaProducer(
        "test", UseProducersConfig(producer_ids=["p1", "p2"])
    )
    await producer.init()

    for p_id in ["p1", "p2"]:
        kafka_producer = [p for p in producer._producers if p.id == p_id][0]
        update_config_mock = mocker.patch.object(kafka_producer, "update_config")
        _config = config.get().dict(exclude_unset=True)
        _config["producers"][p_id]["kafka_config"]["bootstrap.servers"] = "88888"
        config.update_from_dict(_config)
        config.send_signals()
        update_config_mock.assert_called_once()
        update_config_mock.assert_called_with(
            ProducerConfig(
                kafka_config={
                    **config.get().producers[p_id].kafka_config,
                    **{"bootstrap.servers": "88888"},
                },
            )
        )
        update_config_mock.reset_mock()

    await producer.close()
