from pathlib import Path

import pytest
import yaml

from eventbus import config
from eventbus.errors import ConfigNoneError, ConfigUpdateError


def test_update_from_yaml():
    config_path = Path(__file__).parent / "config.yml"
    config.update_from_yaml(config_path)

    config_data = config.get()
    assert config_data.env == config.Env.TEST
    assert config_data.allowed_namespaces == ["n1", "n2"]
    assert config_data.topic_mapping == [
        config.TopicMapping(
            topic="event-v3-${namespace}${env}-greenarrow",
            namespaces=["n1", "n2"],
            patterns=[".*"],
        )
    ]
    assert config_data.kafka == config.KafkaConfig(
        primary_brokers="localhost:12181",
        producer_config={
            "enable.idempotence": True,
            "acks": "all",
            "max.in.flight.requests.per.connection": 5,
            "retries": 3,
        },
        consumer_config={"poll-interval": "50ms"},
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
        new_config["allowed_namespaces"] = ["n3", "n4"]
        config.update_from_dict(new_config)

    assert config.get().allowed_namespaces == ["n3", "n4"]


def test_config_reset():
    with pytest.raises(ConfigNoneError):
        config.get()


def test_add_subscribe():
    def sub1():
        pass

    def sub2():
        pass

    def sub3():
        pass

    sub4 = sub1

    config.add_subscriber(sub1)
    assert config._config_subscribers == {sub1}

    config.add_subscriber(sub2, sub3)
    assert config._config_subscribers == {sub1, sub2, sub3}

    config.add_subscriber(sub4)
    assert config._config_subscribers == {sub1, sub2, sub3}

    config.remove_subscriber(sub2)
    assert config._config_subscribers == {sub1, sub3}

    config.remove_subscriber(sub1, sub3)
    assert config._config_subscribers == set()


def test_call_subscribe(mocker):
    sub1 = mocker.MagicMock()
    config.add_subscriber(sub1)
    test_hot_update()
    assert sub1.call_count == 2

    sub1.reset_mock()
    config.remove_subscriber(sub1)
    test_hot_update()
    sub1.assert_not_called()
