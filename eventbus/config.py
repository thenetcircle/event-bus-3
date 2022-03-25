import os
import threading
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml
from loguru import logger
from pydantic import BaseModel, StrictStr

from eventbus.errors import ConfigNoneError, ConfigUpdateError


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


class ProducerConfig(ConfigModel):
    max_retries: int = 3
    kafka_config: Optional[Dict[str, str]] = None


class UseProducersConfig(ConfigModel):
    producer_ids: List[str]
    max_retries: int = 3


class ConsumerConfig(ConfigModel):
    kafka_topics: List[StrictStr]
    kafka_config: Dict[str, str]
    use_producers: UseProducersConfig
    sink: HttpSinkConfig
    include_events: Optional[List[StrictStr]] = None
    exclude_events: Optional[List[StrictStr]] = None
    concurrent_per_partition: int = 1
    send_queue_size: int = 100
    commit_queue_size: int = 50
    tp_queue_size: int = 3
    max_produce_retries = 3
    max_commit_retries = 9
    max_skipped_events = 100


class HttpAppConfig(ConfigModel):
    use_producers: UseProducersConfig
    max_response_time: int = 10


class Config(ConfigModel):
    env: Env
    debug: bool
    http_app: HttpAppConfig
    producers: Dict[str, ProducerConfig]
    consumers: Dict[str, ConsumerConfig]
    topic_mapping: List[TopicMapping]
    default_kafka_config: Optional[DefaultKafkaConfig] = None
    config_file_path: Optional[Union[str, Path]] = None


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
            parsed_config["config_file_path"] = yaml_file_path
            update_from_dict(parsed_config, log=False)
    except ConfigUpdateError:
        raise
    except Exception:
        raise ConfigUpdateError


def load_from_environ() -> None:
    config_file_path = (
        os.environ["EVENTBUS_CONFIG"]
        if "EVENTBUS_CONFIG" in os.environ
        else "config.yml"
    )
    update_from_yaml(config_file_path)


def reset() -> None:
    global _config
    _config = None


def get() -> Config:
    if _config is None:
        raise ConfigNoneError
    return _config


def _update_config(config: Config) -> None:
    with _config_update_lock:
        global _config
        _config = config


def _fill_config(config: Config) -> Config:
    if config.default_kafka_config:
        default_kafka_producer_config = config.default_kafka_config.producer or {}
        default_kafka_consumer_config = config.default_kafka_config.consumer or {}
    else:
        default_kafka_producer_config = {}
        default_kafka_consumer_config = {}

    config_dict = config.dict()

    for p_name, p_config in config_dict["producers"].items():
        config_dict["producers"][p_name]["kafka_config"] = {
            **default_kafka_producer_config,
            **(p_config["kafka_config"] or {}),
        }
    for c_name, c_config in config_dict["consumers"].items():
        config_dict["consumers"][c_name]["kafka_config"] = {
            **default_kafka_consumer_config,
            **(c_config["kafka_config"] or {}),
        }

    return Config(**config_dict)
