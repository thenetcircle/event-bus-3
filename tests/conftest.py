from pathlib import Path
from typing import Dict

import pytest
from pytest_mock import MockFixture

from eventbus import config


@pytest.fixture(autouse=True)
def setup_config(request):
    if "noconfig" not in request.keywords:
        config_path = Path(__file__).parent / "config.yml"
        config.update_from_yaml(config_path)
    yield
    config.reset()


@pytest.fixture
def mock_internal_kafka_producer(mocker: MockFixture):
    import confluent_kafka
    from confluent_kafka import KafkaException

    # mock InternalKafkaProducer
    def mock_init(self, producer_config: Dict[str, str]):
        self.is_primary = producer_config["bootstrap.servers"] == "localhost:12181"

    mocker.patch("eventbus.producer.InternalKafkaProducer.__init__", mock_init)
    mocker.patch("confluent_kafka.Message")

    primary_msg = confluent_kafka.Message()
    secondary_msg = confluent_kafka.Message()

    def produce_mock(self, topic, value, **kwargs) -> None:
        if self.is_primary:
            if topic == "primary-success":
                kwargs["on_delivery"](None, primary_msg)
            else:
                kwargs["on_delivery"](KafkaException(), None)
        else:
            if topic == "secondary-success":
                kwargs["on_delivery"](None, secondary_msg)
            else:
                kwargs["on_delivery"](KafkaException(), None)

    mocker.patch("eventbus.producer.InternalKafkaProducer.produce", produce_mock)

    yield primary_msg, secondary_msg
