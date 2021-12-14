from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Union

import yaml
from loguru import logger
from pydantic import BaseModel, StrictStr, validator

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
    main_brokers: StrictStr
    fallback_brokers: Optional[StrictStr] = None
    common: Optional[Dict[str, str]] = None
    producer: Dict[str, str]
    consumer: Dict[str, str]

    # @validator("common_config")
    # def must_contain_brokers(cls, v):
    #     if "bootstrap.servers" not in v:
    #         raise ValueError("bootstrap.servers is needed for kafka config.")
    #     return v


class Config(BaseModel):
    class Config:
        allow_mutation = False

    env: Env
    allowed_namespaces: List[StrictStr]
    topic_mapping: List[TopicMapping]
    kafka: KafkaConfig


Subscriber = Callable[[], None]

_config_subscribers: Set[Subscriber] = set()
_config: Optional[Config] = None


def add_subscriber(subscriber: Subscriber) -> None:
    global _config_subscribers
    _config_subscribers.add(subscriber)


def remove_subscriber(subscriber: Subscriber) -> None:
    global _config_subscribers
    _config_subscribers.remove(subscriber)


def call_subscribers() -> None:
    try:
        for subscriber in _config_subscribers:
            subscriber()
    except Exception:
        raise ConfigSubscribeError


def _update_config(new_config: Config) -> None:
    global _config
    _config = new_config

    call_subscribers()


def update_from_dict(data: Dict[str, Any]) -> None:
    logger.debug("Going to update config from dict: %s", data)
    try:
        new_config = Config(**data)
    except Exception:
        raise ConfigUpdateError

    _update_config(new_config)


def update_from_yaml(config_path: Union[str, Path]) -> None:
    logger.info("Going to update config from an yaml file '%s'", config_path)
    config_path = Path(config_path)
    if not config_path.exists():
        raise FileNotFoundError(f"The config file `{config_path}` does not exist.")

    try:
        with open(config_path.resolve()) as f:
            parsed_config = yaml.safe_load(f)
            update_from_dict(parsed_config)
    except ConfigUpdateError:
        raise
    except Exception:
        raise ConfigUpdateError


def clean() -> None:
    global _config
    _config = None


def get() -> Config:
    if _config is None:
        raise ConfigNoneError
    return _config
