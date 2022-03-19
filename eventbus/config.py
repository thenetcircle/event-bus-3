import threading
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

import yaml
from loguru import logger
from pydantic import BaseModel, StrictStr

from eventbus import signals
from eventbus.errors import ConfigNoneError, ConfigSubscribeError, ConfigUpdateError


class Env(str, Enum):
    PROD = "prod"
    STAGE = "stage"
    LAB = "lab"
    DEV = "dev"
    TEST = "test"


class ConfigModel(BaseModel):
    class Config:
        allow_mutation = False


class TopicMapping(ConfigModel):
    topic: StrictStr
    patterns: List[StrictStr]


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


class DefaultConsumerConfig(ConfigModel):
    concurrent_per_partition: int = 1
    kafka_config: Optional[Dict[str, str]] = None
    sink: Optional[HttpSinkConfig] = None


class DefaultKafkaConfig(ConfigModel):
    producer: Optional[Dict[str, str]] = None
    consumer: Optional[Dict[str, str]] = None


class EventProducerConfig(ConfigModel):
    kafka_config: Optional[Dict[str, str]] = None


class EventConsumerConfig(ConfigModel):
    kafka_topics: List[StrictStr]
    kafka_config: Dict[str, str]
    producers: List[str]
    sink: HttpSinkConfig
    include_events: Optional[List[StrictStr]] = None
    exclude_events: Optional[List[StrictStr]] = None
    concurrent_per_partition: int = 1


class HttpAppConfig(ConfigModel):
    producers: List[str]
    max_response_time: int = 10


class Config(ConfigModel):
    env: Env
    debug: bool
    http_app: HttpAppConfig
    event_producers: Dict[str, EventProducerConfig]
    event_consumers: Dict[str, EventConsumerConfig]
    topic_mapping: List[TopicMapping]
    default_kafka_config: Optional[DefaultKafkaConfig] = None


Subscriber = Callable[[], None]

_config: Optional[Config] = None
_config_update_lock = threading.Lock()


def update_from_config(new_config: Config) -> None:
    logger.info("Going to update config from another Config object: {}", new_config)
    _update_config(new_config)


def update_from_dict(data: Dict[str, Any], log=True) -> None:
    if log:
        logger.info("Going to update config from dict: {}", data)

    try:
        new_config = _fill_config(Config(**data))
    except Exception:
        raise ConfigUpdateError

    _update_config(new_config)


def update_from_yaml(yaml_file_path: Union[str, Path]) -> None:
    logger.info("Going to update config from an yaml file '{}'", yaml_file_path)
    yaml_file_path = Path(yaml_file_path)
    if not yaml_file_path.exists():
        raise FileNotFoundError(f"The config file `{yaml_file_path}` does not exist.")

    try:
        with open(yaml_file_path.resolve()) as f:
            parsed_config = yaml.safe_load(f)
            update_from_dict(parsed_config, log=False)
    except ConfigUpdateError:
        raise
    except Exception:
        raise ConfigUpdateError


def reset() -> None:
    global _config
    _config = None


def get() -> Config:
    if _config is None:
        raise ConfigNoneError
    return _config


def _update_config(new_config: Config) -> None:
    with _config_update_lock:
        global _config
        old_config = _config
        _config = new_config

        _send_signals(old_config)


def _fill_config(config: Config) -> Config:
    if config.default_kafka_config:
        default_kafka_producer_config = config.default_kafka_config.producer or {}
        default_kafka_consumer_config = config.default_kafka_config.consumer or {}
    else:
        default_kafka_producer_config = {}
        default_kafka_consumer_config = {}

    config_dict = config.dict()

    for p_name, p_config in config_dict["event_producers"].items():
        config_dict["event_producers"][p_name]["kafka_config"] = {
            **default_kafka_producer_config,
            **(p_config["kafka_config"] or {}),
        }
    for c_name, c_config in config_dict["event_consumers"].items():
        config_dict["event_consumers"][c_name]["kafka_config"] = {
            **default_kafka_consumer_config,
            **(c_config["kafka_config"] or {}),
        }

    return Config(**config_dict)


def _send_signals(old_config: Optional[Config]) -> None:
    try:
        sender = "config"

        new_config = _config
        assert new_config is not None

        if not old_config or old_config != new_config:
            receivers = signals.CONFIG_CHANGED.send(sender)
            logger.info(
                "Config changed, sent CONFIG_CHANGED signal to receivers {}",
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

        if not old_config or old_config.event_producers != new_config.event_producers:
            kwargs = compare_two_config(
                old_config.event_producers if old_config else {},
                new_config.event_producers,
            )
            receivers = signals.CONFIG_PRODUCER_CHANGED.send(sender, **kwargs)
            logger.info(
                "Config changed, sent CONFIG_PRODUCER_CHANGED signal to receivers {}",
                receivers,
            )

        if not old_config or old_config.event_consumers != new_config.event_consumers:
            kwargs = compare_two_config(
                old_config.event_consumers if old_config else {},
                new_config.event_consumers,
            )
            receivers = signals.CONFIG_CONSUMER_CHANGED.send(sender, **kwargs)
            logger.info(
                "Config changed, sent CONFIG_CONSUMER_CHANGED signal to receivers {}",
                receivers,
            )

        if not old_config or old_config.topic_mapping != new_config.topic_mapping:
            receivers = signals.CONFIG_TOPIC_MAPPING_CHANGED.send(sender)
            logger.info(
                "Config changed, sent CONFIG_TOPIC_MAPPING_CHANGED signal to receivers {}",
                receivers,
            )

    except Exception as ex:
        logger.error("Sent ConfigSignals failed with error: {} {}", type(ex), ex)
        raise ConfigSubscribeError
