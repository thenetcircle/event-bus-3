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
