from pathlib import Path

import pytest

from eventbus import config


@pytest.fixture(autouse=True)
def teardown():
    config_path = Path(__file__).parent / "config.yml"
    config.update_from_yaml(config_path)
    yield
    config.reset()
