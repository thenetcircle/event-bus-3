from typing import Dict

import pytest
from confluent_kafka import KafkaException
from pytest_mock import MockFixture

from eventbus import config
from eventbus.errors import EventProduceError
from eventbus.producer import KafkaProducer
from tests.utils import create_event_from_dict


@pytest.fixture
def mock_internal_producer(mocker: MockFixture):
    # mock InternalKafkaProducer
    def mock_init(self, producer_config: Dict[str, str]):
        self.is_primary = producer_config["bootstrap.servers"] == "localhost:12181"

    mocker.patch("eventbus.producer.InternalKafkaProducer.__init__", mock_init)

    primary_msg = mocker.Mock()
    secondary_msg = mocker.Mock()

    def produce_mock(self, topic, value, **kwargs) -> None:
        if self.is_primary:
            if topic == "primary_success":
                kwargs["on_delivery"](None, primary_msg)
            else:
                kwargs["on_delivery"](KafkaException(), None)
        else:
            if topic == "secondary_success":
                kwargs["on_delivery"](None, secondary_msg)
            else:
                kwargs["on_delivery"](KafkaException(), None)

    mocker.patch("eventbus.producer.InternalKafkaProducer.produce", produce_mock)

    yield (primary_msg, secondary_msg)


@pytest.mark.asyncio
async def test_produce_from_primary_producer(mock_internal_producer):
    primary_msg, secondary_msg = mock_internal_producer

    # test only primary producer
    producer = KafkaProducer()
    succ_event = create_event_from_dict({"topic": "primary_success"})
    result = await producer.produce(succ_event)
    assert result is primary_msg

    with pytest.raises(EventProduceError):
        fail_event = create_event_from_dict({"topic": "fail"})
        result = await producer.produce(fail_event)


@pytest.mark.asyncio
async def test_produce_from_both_producers(mock_internal_producer):
    primary_msg, secondary_msg = mock_internal_producer

    # add secondary brokers config
    old_config = config.get()
    new_config = old_config.copy(
        update={
            "kafka": old_config.kafka.copy(
                update={"secondary_brokers": "localhost:12182"}
            )
        },
        deep=True,
    )
    config.update_from_config(new_config)
    assert config.get().kafka.primary_brokers == "localhost:12181"
    assert config.get().kafka.secondary_brokers == "localhost:12182"

    # test result from two internal producers
    producer = KafkaProducer()
    primary_success_event = create_event_from_dict({"topic": "primary_success"})
    result = await producer.produce(primary_success_event)
    assert result is primary_msg

    secondary_success_event = create_event_from_dict({"topic": "secondary_success"})
    result = await producer.produce(secondary_success_event)
    assert result is secondary_msg

    with pytest.raises(EventProduceError):
        fail_event = create_event_from_dict({"topic": "other"})
        await producer.produce(fail_event)
