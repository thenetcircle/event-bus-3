import asyncio
import re
import shutil
from pathlib import Path
from unittest import mock

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
    assert config_data.producers == {
        "p1": config.ProducerConfig(
            kafka_config={
                "enable.idempotence": "false",
                "acks": "all",
                "max.in.flight.requests.per.connection": 5,
                "bootstrap.servers": "localhost:12811",
                "retries": 3,
                "compression.type": "none",
            },
        ),
        "p2": config.ProducerConfig(
            kafka_config={
                "enable.idempotence": "true",
                "acks": "all",
                "max.in.flight.requests.per.connection": 5,
                "bootstrap.servers": "localhost:12811",
                "retries": 3,
                "compression.type": "gzip",
            },
        ),
    }


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


def test_signals():
    def reset_mocks():
        mock1.reset_mock()
        mock2.reset_mock()
        mock3.reset_mock()

    mock1 = mock.Mock(spec={})
    mock2 = mock.Mock(spec={})
    mock3 = mock.Mock(spec={})

    signal_sender = "config"

    signals.CONFIG_PRODUCER_CHANGED.connect(mock1)
    signals.CONFIG_TOPIC_MAPPING_CHANGED.connect(mock2)
    signals.CONFIG_CONSUMER_CHANGED.connect(mock3)

    def update_config_and_send_signals(_config: dict):
        old_config = config.get()
        config.update_from_dict(_config)
        config_watcher._send_signals(old_config, config.get())

    reset_mocks()
    _config = config.get().dict(exclude_unset=True)
    _config["producers"]["p1"]["kafka_config"]["compression.type"] = "snappy"
    _config["producers"]["p3"] = {"kafka_config": {}}
    del _config["producers"]["p2"]
    update_config_and_send_signals(_config)
    mock1.assert_called_once()
    mock1.assert_called_with(
        signal_sender, added={"p3"}, removed={"p2"}, changed={"p1"}
    )
    mock2.assert_not_called()
    mock3.assert_not_called()

    reset_mocks()
    _config = config.get().dict(exclude_unset=True)
    _config["consumers"]["c1"]["kafka_config"]["group.id"] = "group11"
    _config["consumers"]["c3"] = config.get().consumers["c2"].dict()
    del _config["consumers"]["c2"]
    update_config_and_send_signals(_config)
    mock1.assert_not_called()
    mock2.assert_not_called()
    mock3.assert_called_once()
    mock3.assert_called_with(
        signal_sender, added={"c3"}, removed={"c2"}, changed={"c1"}
    )

    reset_mocks()
    _config = config.get().dict(exclude_unset=True)
    _config["topic_mapping"][0]["topic"] = "primary-success2"
    update_config_and_send_signals(_config)
    mock1.assert_not_called()
    mock2.assert_called_once()
    mock3.assert_not_called()


@pytest.mark.asyncio
async def test_watch_file(tmpdir):
    tmp_config_file = tmpdir / "config.yml"
    shutil.copyfile(config.get().config_file_path, tmp_config_file)
    with open(tmp_config_file, "r") as f:
        tmp_config_data = f.read()

    def reset_mocks():
        mock1.reset_mock()
        mock2.reset_mock()
        mock3.reset_mock()
        mock4.reset_mock()

    mock1 = mock.Mock(spec={})
    mock2 = mock.Mock(spec={})
    mock3 = mock.Mock(spec={})

    signals.CONFIG_CHANGED.connect(mock1)
    signals.CONFIG_CHANGED.connect(mock2)
    signals.CONFIG_CHANGED.connect(mock3)

    class Mock4:
        def __init__(self):
            self.attr1 = "attr1"
            self.call_times = 0

        def sub_method(self, *args, **kwargs):
            self.call_times += 1
            assert self.attr1 == "attr1"

        def reset_mock(self):
            self.call_times = 0

    mock4 = Mock4()
    signals.CONFIG_CHANGED.connect(mock4.sub_method)

    await config_watcher.watch_config_file(tmp_config_file, checking_interval=0.1)

    with open(tmp_config_file, "w") as f:
        new_config_data = re.sub(r"env: test", "env: prod", tmp_config_data)
        f.write(new_config_data)

    await asyncio.sleep(0.3)  # waiting for the config_file to be reloaded
    assert config.get().env == config.Env.PROD
    mock1.assert_called_once()
    mock2.assert_called_once()
    mock3.assert_called_once()
    assert mock4.call_times == 1
    reset_mocks()

    await asyncio.sleep(0.3)  # waiting if another reloading happened
    assert config.get().env == config.Env.PROD
    mock1.assert_not_called()
    mock2.assert_not_called()
    mock3.assert_not_called()
    assert mock4.call_times == 0


@pytest.mark.noconfig
def test_fill_config(tmpdir, mocker):
    config_path = Path(__file__).parent / "config.yml"
    config.update_from_yaml(config_path)

    _config = config.get()

    assert len(_config.producers["p1"].kafka_config) == 6
    assert _config.producers["p1"].kafka_config["enable.idempotence"] == "false"
    assert _config.producers["p1"].kafka_config["compression.type"] == "none"
    assert _config.producers["p1"].kafka_config["acks"] == "all"
    assert len(_config.producers["p2"].kafka_config) == 6
    assert _config.producers["p2"].kafka_config["enable.idempotence"] == "true"
    assert _config.producers["p2"].kafka_config["compression.type"] == "gzip"
    assert _config.producers["p2"].kafka_config["acks"] == "all"

    assert len(_config.consumers["c1"].kafka_config) == 5
    assert _config.consumers["c1"].kafka_config["group.id"] == "group1"
    assert _config.consumers["c1"].kafka_config["max.poll.interval.ms"] == "100"
    assert _config.consumers["c1"].kafka_config["enable.auto.commit"] == "false"
    assert _config.consumers["c2"].kafka_config["group.id"] == "group2"
    assert _config.consumers["c2"].kafka_config["max.poll.interval.ms"] == "80"
    assert _config.consumers["c2"].kafka_config["enable.auto.commit"] == "false"
