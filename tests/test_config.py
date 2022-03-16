import re
import time
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock

import pytest
import yaml

from eventbus import config, config_watcher, signals
from eventbus.errors import ConfigNoneError, ConfigUpdateError


def test_update_from_yaml():
    # comment these, because they are already in the auto-use fixture
    config_path = Path(__file__).parent / "config.yml"
    config.update_from_yaml(config_path)

    config_data = config.get()
    assert config_data.env == config.Env.TEST
    assert config_data.topic_mapping == [
        config.TopicMapping(
            topic="primary-success",
            patterns=[r"test\.primary-success"],
        ),
        config.TopicMapping(
            topic="secondary-success",
            patterns=[r"test\.secondary-success"],
        ),
        config.TopicMapping(
            topic="event-v3-${env}-default",
            patterns=[r".*"],
        ),
    ]
    assert config_data.producer == config.ProducerConfig(
        primary_brokers="localhost:12181",
        kafka_config={
            "enable.idempotence": True,
            "acks": "all",
            "max.in.flight.requests.per.connection": 5,
            "retries": 3,
        },
    )


def test_invalid_config_path():
    config_path = Path(__file__).parent / "config.non_existed.yml"
    with pytest.raises(FileNotFoundError):
        config.update_from_yaml(config_path)


def test_invalid_data():
    with pytest.raises(ConfigUpdateError):
        config.update_from_dict({"env": "sth"})


def test_hot_update():
    test_update_from_yaml()

    config_path = Path(__file__).parent / "config.yml"
    with open(config_path) as f:
        new_config = yaml.safe_load(f)
        new_config["env"] = "prod"
        config.update_from_dict(new_config)

    assert config.get().env == config.Env.PROD


@pytest.mark.noconfig
def test_config_reset():
    with pytest.raises(ConfigNoneError):
        config.get()


@pytest.mark.noconfig
def test_signals():
    def reset_mocks():
        mock1.reset_mock()
        mock2.reset_mock()
        mock3.reset_mock()

    mock1 = mock.Mock(spec={})
    mock2 = mock.Mock(spec={})
    mock3 = mock.Mock(spec={})

    signals.CONFIG_PRODUCER_CHANGED.connect(mock1)
    signals.CONFIG_TOPIC_MAPPING_CHANGED.connect(mock2)
    signals.CONFIG_CONSUMER_CHANGED.connect(mock3)

    test_update_from_yaml()
    mock1.assert_called_once()
    mock2.assert_called_once()
    mock3.assert_called_once()

    reset_mocks()
    # update config again with same content
    test_update_from_yaml()
    mock1.assert_not_called()
    mock2.assert_not_called()
    mock3.assert_not_called()

    reset_mocks()
    _config = config.get().dict(exclude_defaults=True)
    _config["producer"]["primary_brokers"] = "localhost:12182"
    config.update_from_dict(_config)
    mock1.assert_called_once()
    mock2.assert_not_called()
    mock3.assert_not_called()

    reset_mocks()
    _config = config.get().dict(exclude_defaults=True)
    _config["topic_mapping"][0]["topic"] = "primary-success2"
    config.update_from_dict(_config)
    mock1.assert_not_called()
    mock2.assert_called_once()
    mock3.assert_not_called()

    reset_mocks()
    _config = config.get().dict(exclude_defaults=True)
    _config["consumer"]["instances"][0]["id"] = "test_consumer2"
    config.update_from_dict(_config)
    mock1.assert_not_called()
    mock2.assert_not_called()
    mock3.assert_called_once()


@pytest.mark.noconfig
def test_watch_file(tmpdir, mocker):
    with pytest.raises(ConfigNoneError):
        config.get()

    origin_config_file = Path(__file__).parent / "config.yml"
    with open(origin_config_file, "r") as f:
        origin_config_data = f.read()

    config_file = tmpdir / "config.yml"
    with open(config_file, "w") as f:
        f.write(origin_config_data)

    def reset_mocks():
        mock1.reset_mock()
        mock2.reset_mock()
        mock3.reset_mock()

    mock1 = mock.Mock(spec={})
    mock2 = mock.Mock(spec={})
    mock3 = mock.Mock(spec={})

    signals.CONFIG_CHANGED.connect(mock1)
    signals.CONFIG_CHANGED.connect(mock2)

    config_watcher.watch_file(config_file, checking_interval=0.1)

    time.sleep(0.3)  # waiting for the config_file to be loaded
    assert config.get().env == config.Env.TEST
    mock1.assert_called_once()
    mock2.assert_called_once()
    reset_mocks()

    # add some other subscribers after watching
    signals.CONFIG_CHANGED.connect(mock3)

    class Mock4:
        def __init__(self):
            self.attr1 = "attr1"
            self.call_times = 0

        def sub_method(self, *args):
            self.call_times += 1
            assert self.attr1 == "attr1"

    mock4 = Mock4()
    signals.CONFIG_CHANGED.connect(mock4.sub_method)

    with open(config_file, "w") as f:
        new_config_data = re.sub(r"env: test", "env: prod", origin_config_data)
        f.write(new_config_data)
    time.sleep(0.3)  # waiting for the config_file to be reloaded
    assert config.get().env == config.Env.PROD
    mock1.assert_called_once()
    mock2.assert_called_once()
    mock3.assert_called_once()
    assert mock4.call_times == 1
    reset_mocks()

    time.sleep(0.3)  # waiting if another reloading happened
    assert config.get().env == config.Env.PROD
    mock1.assert_not_called()
    mock2.assert_not_called()
    mock3.assert_not_called()
    assert mock4.call_times == 1
