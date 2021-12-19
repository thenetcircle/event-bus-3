import threading
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Union

import yaml
from loguru import logger
from pydantic import BaseModel, StrictStr

from eventbus.errors import ConfigNoneError, ConfigSubscribeError, ConfigUpdateError


class Env(str, Enum):
    PROD = "prod"
    STAGE = "stage"
    LAB = "lab"
    DEV = "dev"
    TEST = "test"


class TopicMapping(BaseModel):
    topic: StrictStr
    namespaces: List[StrictStr]
    patterns: List[StrictStr]


class KafkaConfig(BaseModel):
    primary_brokers: StrictStr
    secondary_brokers: Optional[StrictStr] = None
    common_config: Optional[Dict[str, str]] = None
    producer_config: Dict[str, str]
    consumer_config: Dict[str, str]


class Config(BaseModel):
    class Config:
        allow_mutation = False

    env: Env
    debug: bool
    kafka: KafkaConfig
    topic_mapping: List[TopicMapping]
    allowed_namespaces: Optional[List[StrictStr]] = None


Subscriber = Callable[[], None]

_config_subscribers: Set[Subscriber] = set()
_config: Optional[Config] = None
_config_update_lock = threading.Lock()


def add_subscriber(*subscribers: Subscriber) -> None:
    global _config_subscribers
    for sub in subscribers:
        _config_subscribers.add(sub)


def remove_subscriber(*subscribers: Subscriber) -> None:
    global _config_subscribers
    for sub in subscribers:
        _config_subscribers.remove(sub)


def call_subscribers() -> None:
    try:
        for subscriber in _config_subscribers:
            subscriber()
    except Exception:
        raise ConfigSubscribeError


def _update_config(new_config: Config) -> None:
    with _config_update_lock:
        global _config
        _config = new_config

        call_subscribers()


def update_from_dict(data: Dict[str, Any]) -> None:
    logger.debug("Going to update config from dict: {}", data)
    try:
        new_config = Config(**data)
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
            update_from_dict(parsed_config)
    except ConfigUpdateError:
        raise
    except Exception:
        raise ConfigUpdateError


def reset() -> None:
    global _config, _config_subscribers
    _config = None
    _config_subscribers = set()


def get() -> Config:
    if _config is None:
        raise ConfigNoneError
    return _config
