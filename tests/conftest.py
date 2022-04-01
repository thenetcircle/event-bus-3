import sys
from pathlib import Path

import loguru
import pytest

from eventbus import config


@pytest.fixture(autouse=True)
def init_logger():
    loguru.logger.remove()
    loguru.logger.add(sys.stderr, level="INFO")


@pytest.fixture(autouse=True)
def setup_config(request):
    if "noconfig" not in request.keywords:
        config_path = Path(__file__).parent / "fixtures" / "config.yml"
        config.update_from_yaml(config_path)

    yield

    config.reset()
