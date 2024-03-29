from pathlib import Path

import pytest
import yaml

from eventbus import config
from eventbus.errors import ConfigNoneError, ConfigUpdateError


def test_update_from_yaml():
    config_data = config.get()
    assert config_data.app.env == config.Env.TEST
    assert config_data.zookeeper == config.ZookeeperConfig(
        hosts="localhost:2181",
        timeout=10.0,
        topic_mapping_path="/event-bus-3/test/topics",
        story_path="/event-bus-3/test/stories",
    )
    assert config_data.default_kafka_params == config.DefaultKafkaParams(
        producer={
            "bootstrap_servers": ["localhost:9092"],
            "enable_idempotence": "true",
            "acks": "all",
        },
        consumer={
            "bootstrap_servers": ["localhost:9092"],
            "enable_auto_commit": False,
            "auto_commit_interval_ms": 2000,
            "auto_offset_reset": "latest",
            "max_poll_interval_ms": 600000,
            "metadata_max_age_ms": 180000,
            "heartbeat_interval_ms": 3000,
            "session_timeout_ms": 60000,
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
