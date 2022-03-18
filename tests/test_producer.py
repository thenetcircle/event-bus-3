import json
from threading import Thread

import pytest
from pytest_mock import MockFixture

from eventbus import config
from eventbus.errors import EventProducingError
from eventbus.producer import EventProducer
from tests.utils import create_event_from_dict, create_kafka_message_from_dict


@pytest.fixture
def mock_internal_kafka_producer(mocker: MockFixture):
    def produce_mock(self, topic, value, **kwargs) -> None:
        def delivery(err, msg):
            Thread(target=kwargs["on_delivery"], args=(err, msg), daemon=True).start()

        data = json.loads(value)

        if data["title"] == "normal_event":
            msg = create_kafka_message_from_dict({})
            delivery(None, msg)
        elif data["title"] == "exceptional_event":
            delivery(RuntimeError("exceptional_event"), None)

    mocker.patch("eventbus.config_watcher.watch_file")
    mocker.patch("eventbus.producer.KafkaProducer.init")
    mocker.patch("eventbus.producer.KafkaProducer.produce", produce_mock)

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
async def test_produce_from_primary_producer(mock_internal_kafka_producer):
    primary_msg, secondary_msg = mock_internal_kafka_producer

    # test only primary producer
    producer = EventProducer()
    succ_event = create_event_from_dict({"topic": "primary-success"})
    result = await producer.produce(succ_event)
    assert result is primary_msg

    with pytest.raises(EventProducingError):
        fail_event = create_event_from_dict({"topic": "fail"})
        result = await producer.produce(fail_event)


@pytest.mark.asyncio
async def test_produce_from_both_producers(mock_internal_kafka_producer):
    primary_msg, secondary_msg = mock_internal_kafka_producer

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
