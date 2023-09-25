import os
import threading
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml
from blinker import signal
from loguru import logger
from pydantic import BaseModel, StrictStr

from eventbus.errors import ConfigNoneError, ConfigUpdateError, SendSignalError
from eventbus.utils import deep_merge_two_dict


class Env(str, Enum):
    PROD = "prod"
    STAGE = "stage"
    LAB = "lab"
    DEV = "dev"
    TEST = "test"


class ConfigModel(BaseModel):
    class Config:
        allow_mutation = False


class StatsdConfig(ConfigModel):
    host: StrictStr
    port: int
    prefix: Optional[StrictStr]


class SentryConfig(ConfigModel):
    dsn: StrictStr
    sample_rate: float = 1.0
    traces_sample_rate: float = 0.2


class AppConfig(ConfigModel):
    project_id: StrictStr
    env: Env
    debug: bool
    max_response_time: int = 3
    sentry: Optional[SentryConfig] = None
    statsd: Optional[StatsdConfig] = None


class ProducerConfig(ConfigModel):
    kafka_config: Dict[str, str]
    max_retries: int = 3


class ConsumerConfig(ConfigModel):
    kafka_config: Dict[str, str]


class HttpSinkMethod(str, Enum):
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"


class HttpSinkConfig(ConfigModel):
    url: StrictStr
    method: HttpSinkMethod = HttpSinkMethod.POST
    headers: Optional[Dict[str, str]] = None
    timeout: float = 300  # seconds
    max_retry_times: int = 3
    backoff_retry_step: float = 0.1
    backoff_retry_max_time: float = 60.0


class StoryConfig(ConfigModel):
    kafka_topic: StrictStr
    sink: StrictStr
    event_poll_interval: float = 1.0
    include_events: Optional[List[StrictStr]] = None
    exclude_events: Optional[List[StrictStr]] = None
    concurrent_per_partition: int = 1
    send_queue_size: int = 100
    commit_queue_size: int = 10
    tp_queue_size: int = 3
    max_produce_retries = 3
    max_commit_retries = 2
    max_skipped_events = 100
    disabled = False


class TopicMappingConfig(ConfigModel):
    topic: StrictStr
    patterns: List[StrictStr]


class Config(ConfigModel):
    app: AppConfig
    producer: ProducerConfig
    consumer: ConsumerConfig
    sinks: Dict[str, HttpSinkConfig]
    stories: List[StoryConfig]
    topic_mapping: List[TopicMappingConfig]
    last_update_time: Optional[float] = None
    config_file_path: Optional[str] = None


class ConfigSignals:
    ANY_CHANGE = signal("any_config_change")
    PRODUCER_CHANGE = signal("producer_config_change")
    TOPIC_MAPPING_CHANGE = signal("topic_mapping_config_change")
    CONSUMER_CHANGE = signal("consumer_config_change")


_old_config: Optional[Config] = None
_config: Optional[Config] = None
_config_update_lock = threading.Lock()


def update_from_config(new_config: Config) -> None:
    logger.info("Going to update config from another Config object: {}", new_config)
    _update_config(new_config)


def update_from_dict(data: Dict[str, Any], log=True) -> None:
    if log:
        logger.info("Going to update config from dict: {}", data)

    try:
        new_config = Config(**data)
    except Exception as ex:
        raise ConfigUpdateError(str(ex))

    _update_config(new_config)


def update_from_yaml(yaml_file_path: Union[str, Path]) -> None:
    logger.info("Going to update config from an yaml file '{}'", yaml_file_path)

    try:
        parsed_config = parse_yaml_config(yaml_file_path)
        update_from_dict(parsed_config, log=False)
    except (ConfigUpdateError, FileNotFoundError):
        raise
    except Exception as ex:
        raise ConfigUpdateError(str(ex))


def parse_yaml_config(yaml_file_path: Union[str, Path]) -> Dict[str, Any]:
    yaml_file_path = Path(yaml_file_path)
    if not yaml_file_path.exists():
        raise FileNotFoundError(f"The config file `{yaml_file_path}` does not exist.")

    with open(yaml_file_path) as f:
        parsed_config = yaml.safe_load(f)
        parsed_config["config_file_path"] = str(yaml_file_path)
        return parsed_config


def load_from_environ() -> None:
    config_file_path = (
        os.environ["EVENTBUS_CONFIG"]
        if "EVENTBUS_CONFIG" in os.environ
        else "config.yml"
    )
    update_from_yaml(config_file_path)


def reload() -> None:
    if _config and _config.config_file_path:
        update_from_yaml(_config.config_file_path)


def reset() -> None:
    global _config
    _config = None


def get() -> Config:
    if _config is None:
        raise ConfigNoneError
    return _config


def get_old() -> Optional[Config]:
    return _old_config


def send_signals() -> None:
    try:
        old_config = _old_config
        new_config = _config

        if not old_config or not new_config:
            # won't send any signal if either old_config and new_config is empty
            return

        signal_sender = "config"

        if old_config != new_config:
            receivers = ConfigSignals.ANY_CHANGE.send(signal_sender)
            logger.info(
                "Config changed, sent CONFIG_CHANGED signal to receivers: {}",
                receivers,
            )

        def compare_two_config(
            old: Dict[str, Any], new: Dict[str, Any]
        ) -> Dict[str, set]:
            old_keys = set(old.keys())
            new_keys = set(new.keys())

            removed = old_keys.difference(new_keys)
            added = new_keys.difference(old_keys)
            changed = set()
            for k in old_keys.intersection(new_keys):
                if old[k] != new[k]:
                    changed.add(k)

            return {"added": added, "removed": removed, "changed": changed}

        if old_config.producers != new_config.producers:
            kwargs = compare_two_config(
                old_config.producers,
                new_config.producers,
            )
            receivers = ConfigSignals.PRODUCER_CHANGE.send(signal_sender, **kwargs)
            logger.info(
                "Config changed, sent CONFIG_PRODUCER_CHANGED signal to receivers: {}, with kwargs: {}",
                receivers,
                kwargs,
            )

        if old_config.consumers != new_config.consumers:
            kwargs = compare_two_config(
                old_config.consumers,
                new_config.consumers,
            )
            receivers = ConfigSignals.CONSUMER_CHANGE.send(signal_sender, **kwargs)
            logger.info(
                "Config changed, sent CONFIG_CONSUMER_CHANGED signal to receivers: {}, with kwargs: {}",
                receivers,
                kwargs,
            )

        if old_config.topic_mapping != new_config.topic_mapping:
            receivers = ConfigSignals.TOPIC_MAPPING_CHANGE.send(signal_sender)
            logger.info(
                "Config changed, sent CONFIG_TOPIC_MAPPING_CHANGED signal to receivers: {}",
                receivers,
            )

    except Exception as ex:
        logger.error(
            "Sent ConfigSignals failed with error: {} {}", type(ex).__name__, ex
        )
        # TODO trigger alert
        raise SendSignalError


def _update_config(config: Config) -> None:
    global _old_config, _config
    _old_config = _config
    _config = config.copy(update={"last_update_time": datetime.now().timestamp()})


def _merge_default_config(data: Dict[str, Any]) -> Dict[str, Any]:
    new_data = data.copy()
    if "default_producer_config" in data:
        for p_name, p_config in data["producers"].items():
            new_data["producers"][p_name] = deep_merge_two_dict(
                data["default_producer_config"], p_config
            )
    if "default_consumer_config" in data:
        for c_name, c_config in data["consumers"].items():
            new_data["consumers"][c_name] = deep_merge_two_dict(
                data["default_consumer_config"], c_config
            )
    return new_data
