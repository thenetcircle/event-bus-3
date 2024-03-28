import sys
from pathlib import Path

import loguru
import pytest

from eventbus import config


@pytest.fixture(autouse=True)
def init_logger():
    format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> "
        "| <level>{level: <8}</level> "
        "| <cyan>{process.name}</cyan>:<cyan>{thread.name}</cyan> "
        "| <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> "
        "- <level>{message}</level>"
    )

    loguru.logger.remove()
    loguru.logger.add(sys.stderr, level="INFO", format=format)


@pytest.fixture(autouse=True)
def setup_config(request):
    if "noconfig" not in request.keywords:
        # config_path = Path(__file__).parent / "fixtures" / "config.yml"
        # config.update_from_yaml(config_path)
        config.load_from_environ()

    yield

    config.reset()
