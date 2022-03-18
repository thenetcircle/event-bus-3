import multiprocessing
import sys
from pathlib import Path
from sys import platform
from threading import Thread

import loguru
import pytest
from pytest_mock import MockFixture
from utils import create_kafka_message_from_dict

from eventbus import config, producer


def pytest_configure(config):
    if platform in ["darwin", "windows"]:
        multiprocessing.set_start_method("spawn")
    else:
        multiprocessing.set_start_method("fork")


@pytest.fixture(autouse=True)
def setup_config(request):
    loguru.logger.remove()
    loguru.logger.add(sys.stderr, level="INFO")

    if "noconfig" not in request.keywords:
        config_path = Path(__file__).parent / "config.yml"
        config.update_from_yaml(config_path)
    yield
    config.reset()


@pytest.fixture
def mock_internal_kafka_producer(mocker: MockFixture):
    # mock InternalKafkaProducer
    # def mock_init(self, producer_config: Dict[str, str]):
    #     self.is_primary = producer_config["bootstrap.servers"] == "localhost:12181"
    #
    # mocker.patch("eventbus.producer.KafkaProducer.__init__", mock_init)

    def produce_mock(self, topic, value, **kwargs) -> None:
        def delivery(err, msg):
            Thread(target=kwargs["on_delivery"], args=(err, msg), daemon=True).start()

        msg = create_kafka_message_from_dict({})
        delivery(None, msg)

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

    mocker.patch("eventbus.producer.KafkaProducer.init")
    mocker.patch("eventbus.producer.KafkaProducer.produce", produce_mock)
