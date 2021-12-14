import pytest

from eventbus import config


@pytest.fixture(autouse=True)
def clean_config():
    yield
    config.clean()
