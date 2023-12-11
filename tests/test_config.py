import asyncio
import re
import shutil
import threading
from pathlib import Path
from unittest import mock

import pytest
import yaml

import eventbus.model
from eventbus import config
from eventbus.errors import ConfigNoneError, ConfigUpdateError


def test_update_from_yaml():
    config_data = config.get()
    assert config_data.app.env == config.Env.TEST
    assert config_data.topic_mapping == [
        eventbus.model.TopicMappingEntry(
            topic="primary-success",
            patterns=[r"test\.primary-success"],
        ),
        eventbus.model.TopicMappingEntry(
            topic="secondary-success",
            patterns=[r"test\.secondary-success"],
        ),
        eventbus.model.TopicMappingEntry(
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
                "delivery.timeout.ms": "2000",
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
                "delivery.timeout.ms": "2000",
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
    config_path = Path(__file__).parent / "fixtures" / "config.yml"
    with open(config_path) as f:
        new_config = yaml.safe_load(f)
        new_config["app"]["env"] = "prod"
        config.update_from_dict(new_config)

    assert config.get().app.env == config.Env.PROD


@pytest.mark.noconfig
def test_config_reset():
    with pytest.raises(ConfigNoneError):
        config.get()


@pytest.mark.noconfig
def test_merge_default_config(tmpdir, mocker):
    config_path = Path(__file__).parent / "fixtures" / "config.yml"
    config.update_from_yaml(config_path)

    _config = config.get()

    assert len(_config.producers["p1"].kafka_config) == 7
    assert _config.producers["p1"].kafka_config["enable.idempotence"] == "false"
    assert _config.producers["p1"].kafka_config["compression.type"] == "none"
    assert _config.producers["p1"].kafka_config["acks"] == "all"

    assert len(_config.producers["p2"].kafka_config) == 7
    assert _config.producers["p2"].kafka_config["enable.idempotence"] == "true"
    assert _config.producers["p2"].kafka_config["compression.type"] == "gzip"
    assert _config.producers["p2"].kafka_config["acks"] == "all"

    assert len(_config.consumers["c1"].kafka_config) == 5
    assert _config.consumers["c1"].kafka_topics == ["topic1"]
    assert _config.consumers["c1"].kafka_config["group.id"] == "group1"
    assert _config.consumers["c1"].kafka_config["max.poll.interval.ms"] == "300100"
    assert _config.consumers["c1"].kafka_config["enable.auto.commit"] == "false"
    assert _config.consumers["c1"].include_events == ["test\\..*"]
    assert _config.consumers["c1"].sink.url == "http://localhost:8001"
    assert _config.consumers["c1"].sink.timeout == 5

    assert len(_config.consumers["c2"].kafka_config) == 5
    assert _config.consumers["c2"].kafka_topics == ["topic2"]
    assert _config.consumers["c2"].kafka_config["group.id"] == "group2"
    assert _config.consumers["c2"].kafka_config["max.poll.interval.ms"] == "300080"
    assert _config.consumers["c2"].kafka_config["enable.auto.commit"] == "false"
    assert _config.consumers["c2"].include_events == ["test2\\..*"]
    assert _config.consumers["c2"].sink.url == "http://localhost:8001"
    assert _config.consumers["c2"].sink.timeout == 10


@pytest.mark.asyncio
async def test_watch_file(tmpdir):
    tmp_config_file = tmpdir / "config.yml"
    shutil.copyfile(config.get().config_file_path, tmp_config_file)
    with open(tmp_config_file, "r") as f:
        tmp_config_data = f.read()

    last_called_thread = None

    def reset_mocks():
        mock1.reset_mock()
        mock2.reset_mock()
        mock3.reset_mock()
        mock4.reset_mock()
        nonlocal last_called_thread
        last_called_thread = None

    def mock1_side_effect(sender, **kwargs):
        nonlocal last_called_thread
        last_called_thread = threading.current_thread().ident

    mock1 = mock.Mock(spec={}, side_effect=mock1_side_effect)
    mock2 = mock.Mock(spec={})
    mock3 = mock.Mock(spec={})

    config.ConfigSignals.ANY_CHANGE.connect(mock1)
    config.ConfigSignals.ANY_CHANGE.connect(mock2)
    config.ConfigSignals.ANY_CHANGE.connect(mock3)

    class Mock4:
        def __init__(self):
            self.attr1 = "attr1"
            self.call_times = 0

        def sub_method(self, sender, **kwargs):
            self.call_times += 1
            assert self.attr1 == "attr1"

        def reset_mock(self):
            self.call_times = 0

    mock4 = Mock4()
    config.ConfigSignals.ANY_CHANGE.connect(mock4.sub_method)

    tmp_config_file = Path(tmp_config_file).resolve()

    # await config_watcher.async_watch_config_file(tmp_config_file, checking_interval=0.1)
    await asyncio.sleep(1)

    with open(tmp_config_file, "w") as f:
        new_config_data = re.sub(r"env: test", "env: prod", tmp_config_data)
        f.write(new_config_data)

    await asyncio.sleep(0.3)  # waiting for the config_file to be reloaded
    assert config.get().app.env == config.Env.PROD
    mock1.assert_called_once()
    assert last_called_thread == threading.current_thread().ident
    mock2.assert_called_once()
    mock3.assert_called_once()
    assert mock4.call_times == 1
    reset_mocks()

    await asyncio.sleep(0.3)  # waiting if another reloading happened
    assert config.get().app.env == config.Env.PROD
    mock1.assert_not_called()
    mock2.assert_not_called()
    mock3.assert_not_called()
    assert mock4.call_times == 0


def test_signals():
    def reset_mocks():
        p_listener.reset_mock()
        tm_listener.reset_mock()
        c_listener.reset_mock()

    p_listener = mock.Mock(spec={})
    tm_listener = mock.Mock(spec={})
    c_listener = mock.Mock(spec={})

    signal_sender = "config"

    config.ConfigSignals.PRODUCER_CHANGE.connect(p_listener)
    config.ConfigSignals.TOPIC_MAPPING_CHANGE.connect(tm_listener)
    config.ConfigSignals.CONSUMER_CHANGE.connect(c_listener)

    def update_config_and_send_signals(_config: dict):
        config.update_from_dict(_config)
        config.send_signals()

    _config = config.get().dict(exclude_unset=True)
    _config["producers"]["p1"]["kafka_config"]["compression.type"] = "snappy"
    _config["producers"]["p3"] = {"kafka_config": {}}
    del _config["producers"]["p2"]
    update_config_and_send_signals(_config)
    p_listener.assert_called_once()
    p_listener.assert_called_with(
        signal_sender, added={"p3"}, removed={"p2"}, changed={"p1"}
    )
    tm_listener.assert_not_called()
    c_listener.assert_not_called()
    reset_mocks()

    _config = config.get().dict(exclude_unset=True)
    _config["consumers"]["c1"]["kafka_config"]["group.id"] = "group11"
    _config["consumers"]["c3"] = config.get().consumers["c2"].dict()
    del _config["consumers"]["c2"]
    update_config_and_send_signals(_config)
    p_listener.assert_not_called()
    tm_listener.assert_not_called()
    c_listener.assert_called_once()
    c_listener.assert_called_with(
        signal_sender, added={"c3"}, removed={"c2"}, changed={"c1"}
    )
    reset_mocks()

    _config = config.get().dict(exclude_unset=True)
    _config["topic_mapping"][0]["topic"] = "primary-success2"
    update_config_and_send_signals(_config)
    p_listener.assert_not_called()
    tm_listener.assert_called_once()
    c_listener.assert_not_called()
    reset_mocks()

    _config = config.get().dict()
    _config["producers"]["p1"]["kafka_config"]["retries"] = 101
    update_config_and_send_signals(_config)
    p_listener.assert_called_once()
    p_listener.assert_called_with(
        signal_sender, added=set(), removed=set(), changed={"p1"}
    )
    tm_listener.assert_not_called()
    c_listener.assert_not_called()
    reset_mocks()

    _config = config.get().dict()
    _config["producers"]["p3"]["kafka_config"]["bootstrap.servers"] = "localhost:13000"
    update_config_and_send_signals(_config)
    p_listener.assert_called_once()
    p_listener.assert_called_with(
        signal_sender, added=set(), removed=set(), changed={"p3"}
    )
    tm_listener.assert_not_called()
    c_listener.assert_not_called()
    reset_mocks()
