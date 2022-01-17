import threading
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Union

import yaml
from loguru import logger
from pydantic import BaseModel, Field, StrictStr

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


class KafkaConfig(ConfigModel):
    primary_brokers: StrictStr
    secondary_brokers: Optional[StrictStr] = None
    common_config: Optional[Dict[str, str]] = None
    producer_config: Dict[str, str]
    consumer_config: Dict[str, str]


class ConsumerSinkType(str, Enum):
    HTTP = "http"


class ConsumerSinkConfig(ConfigModel):
    type: ConsumerSinkType = ConsumerSinkType.HTTP
    url: StrictStr
    headers: Optional[Dict[str, str]] = None
    expected_status_codes: List[int] = [200]
    expected_responses: Optional[List[StrictStr]] = None


class ConsumerRetryStrategy(str, Enum):
    BACKOFF = "backoff"


class ConsumerRetryConfig(ConfigModel):
    strategy: ConsumerRetryStrategy = ConsumerRetryStrategy.BACKOFF
    max_retry_times: Optional[int] = Field(default=None, gt=0)


class ConsumerConfig(ConfigModel):
    id: StrictStr
    events: List[StrictStr]
    concurrent_per_partition: int = 1
    sink: ConsumerSinkConfig
    retry: Optional[ConsumerRetryConfig] = None


class Config(ConfigModel):
    env: Env
    debug: bool
    kafka: KafkaConfig
    topic_mapping: List[TopicMapping]
    consumers: List[ConsumerConfig]


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


def update_from_config(new_config: Config) -> None:
    logger.debug("Going to update config another Config object: {}", new_config)
    _update_config(new_config)


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
