import json
from threading import Thread

import pytest
import pytest_asyncio
from confluent_kafka.cimpl import Message
from pytest_mock import MockFixture

from eventbus import config
from eventbus.errors import EventProducingError
from eventbus.producer import EventProducer
from tests.utils import create_event_from_dict, create_kafka_message_from_dict


@pytest_asyncio.fixture
async def mock_producer(mocker: MockFixture):
    def produce_mock(self, topic, value, **kwargs) -> None:
        def delivery(err, msg):
            Thread(target=kwargs["on_delivery"], args=(err, msg), daemon=True).start()

        data = json.loads(value)

        if data["title"] == "normal_event":
            msg = create_kafka_message_from_dict({"value": "xxx"})
            delivery(None, msg)
        elif data["title"] == "exceptional_event":
            delivery(RuntimeError("exceptional_event"), None)

        if self.id == "p1":
            if data["title"] == "p1_normal":
                msg = create_kafka_message_from_dict({"key": "p1_normal"})
                delivery(None, msg)
        elif self.id == "p2":
            pass

    mocker.patch("eventbus.config_watcher.watch_file")
    mocker.patch("eventbus.producer.KafkaProducer.init")
    mocker.patch("eventbus.producer.KafkaProducer.produce", produce_mock)

    producer = EventProducer("test", ["p1", "p2"])
    await producer.init()
    yield producer

    # mock InternalKafkaProducer
    # def mock_init(self, producer_config: Dict[str, str]):
    #     self.is_primary = producer_config["bootstrap.servers"] == "localhost:12181"
    #
    # mocker.patch("eventbus.producer.KafkaProducer.__init__", mock_init)

    # if self.is_primary:
    #     if topic == "primary-success":
    #         kwargs["on_delivery"](None, primary_msg)
    #     else:
    #         kwargs["on_delivery"](KafkaException(), None)
    # else:
    #     if topic == "secondary-success":
    #         kwargs["on_delivery"](None, secondary_msg)
    #     else:
    #         kwargs["on_delivery"](KafkaException(), None)


@pytest.mark.asyncio
async def test_produce_normal_event(mock_producer: EventProducer):
    e1 = create_event_from_dict({"payload": {"title": "p1_normal"}})
    msg = await mock_producer.produce("t1", e1)
    assert isinstance(msg, Message)
    assert msg.key() == "p1_normal"


@pytest.mark.asyncio
async def test_produce_from_primary_producer(mock_producer):
    primary_msg, secondary_msg = mock_producer

    # test only primary producer
    producer = EventProducer()
    succ_event = create_event_from_dict({"topic": "primary-success"})
    result = await producer.produce(succ_event)
    assert result is primary_msg

    with pytest.raises(EventProducingError):
        fail_event = create_event_from_dict({"topic": "fail"})
        result = await producer.produce(fail_event)


@pytest.mark.asyncio
async def test_produce_from_both_producers(mock_producer):
    primary_msg, secondary_msg = mock_producer

    # add secondary brokers config
    old_config = config.get()
    new_config = old_config.copy(
        update={
            "producer": old_config.producer.copy(
                update={"secondary_brokers": "localhost:12182"}
            )
        },
        deep=True,
    )
    config.update_from_config(new_config)
    assert config.get().producer.primary_brokers == "localhost:12181"
    assert config.get().producer.secondary_brokers == "localhost:12182"

    # test result from two internal producers
    producer = EventProducer()
    primary_success_event = create_event_from_dict({"topic": "primary-success"})
    result = await producer.produce(primary_success_event)
    assert result is primary_msg

    secondary_success_event = create_event_from_dict({"topic": "secondary-success"})
    result = await producer.produce(secondary_success_event)
    assert result is secondary_msg

    with pytest.raises(EventProducingError):
        fail_event = create_event_from_dict({"topic": "other"})
        await producer.produce(fail_event)
