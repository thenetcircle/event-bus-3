import pytest

from eventbus import config


@pytest.fixture(autouse=True)
def teardown():
    yield
    config.reset()
