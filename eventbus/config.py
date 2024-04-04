import os
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Optional, Union
from string import Template

import yaml
from loguru import logger
from pydantic import StrictStr

from eventbus.errors import ConfigNoneError, ConfigUpdateError
from eventbus.model import EventBusBaseModel, SinkType


class Env(str, Enum):
    PROD = "prod"
    STAGING = "staging"
    LAB = "lab"
    DEV = "dev"
    TEST = "test"


class StatsdConfig(EventBusBaseModel):
    host: StrictStr
    port: int
    prefix: Optional[StrictStr]


class SentryConfig(EventBusBaseModel):
    dsn: StrictStr
    sample_rate: float = 1.0
    traces_sample_rate: float = 0.2


class ZookeeperConfig(EventBusBaseModel):
    hosts: StrictStr
    topic_mapping_path: StrictStr
    story_path: StrictStr
    timeout: float = 10.0


class AppConfig(EventBusBaseModel):
    project_id: StrictStr
    env: Env
    debug: bool
    max_response_time: float = 3.0


class DefaultKafkaParams(EventBusBaseModel):
    producer: Dict[str, Any]
    consumer: Dict[str, Any]


class SinkConfig(EventBusBaseModel):
    type: SinkType
    params: Dict[str, Any]


class Config(EventBusBaseModel):
    app: AppConfig
    zookeeper: ZookeeperConfig
    default_kafka_params: DefaultKafkaParams
    predefined_sinks: Optional[Dict[str, SinkConfig]] = None
    sentry: Optional[SentryConfig] = None
    statsd: Optional[StatsdConfig] = None
    last_update_time: Optional[float] = None
    config_file_path: Optional[str] = None


_old_config: Optional[Config] = None
_config: Optional[Config] = None


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

    with open(yaml_file_path, "r") as file:
        raw_config = file.read()

    template = Template(raw_config)
    filled_config = template.safe_substitute(os.environ)

    parsed_config = yaml.safe_load(filled_config)
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


def _update_config(config: Config) -> None:
    global _old_config, _config
    _old_config = _config
    _config = config.model_copy(update={"last_update_time": datetime.now().timestamp()})
