import json
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, StrictStr

from eventbus.event import EventStatus, KafkaEvent


class EventBusBaseModel(BaseModel):
    class Config:
        frozen = True


# Topic mapping related
class TopicMappingEntry(EventBusBaseModel):
    topic: StrictStr
    patterns: List[StrictStr]


# Sink related
class SinkType(str, Enum):
    HTTP = "HTTP"


class AbsSink(ABC):
    @abstractmethod
    async def init(self):
        raise NotImplementedError

    @abstractmethod
    async def send_event(self, event: KafkaEvent) -> Tuple[KafkaEvent, EventStatus]:
        raise NotImplementedError

    @abstractmethod
    async def close(self):
        raise NotImplementedError


# Transform related
class TransformType(str, Enum):
    FILTER = "FILTER"


class AbsTransform(ABC):
    @abstractmethod
    async def init(self):
        raise NotImplementedError

    @abstractmethod
    async def run(self, event: KafkaEvent) -> Optional[KafkaEvent]:
        raise NotImplementedError

    @abstractmethod
    async def close(self):
        raise NotImplementedError


# Consumer related
class StoryStatus(str, Enum):
    NORMAL = "NORMAL"
    DISABLED = "DISABLED"


class StoryParams(EventBusBaseModel):
    id: StrictStr
    consumer_params: Dict[str, Any]
    sink: Tuple[SinkType, Dict[str, Any]]
    status: StoryStatus = StoryStatus.NORMAL
    transforms: Optional[List[Tuple[TransformType, Dict[str, Any]]]] = None
    concurrent_per_partition: int = 1
    max_commit_retry_times: int = 2


# class StoryInfo(EventBusModel):
#     kafka_topic: StrictStr
#     sink: StrictStr
#     event_poll_interval: float = 1.0
#     include_events: Optional[List[StrictStr]] = None
#     exclude_events: Optional[List[StrictStr]] = None
#     concurrent_per_partition: int = 1
#     send_queue_size: int = 100
#     commit_queue_size: int = 10
#     tp_queue_size: int = 3
#     max_produce_retries = 3
#     max_commit_retries = 2
#     max_skipped_events = 100
#     disabled = False


class HttpSinkMethod(str, Enum):
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"


class HttpSinkParams(EventBusBaseModel):
    url: StrictStr
    method: HttpSinkMethod = HttpSinkMethod.POST
    headers: Optional[Dict[str, str]] = None
    timeout: float = 300  # seconds
    max_retry_times: int = 3
    backoff_retry_step: float = 0.1
    backoff_retry_max_time: float = 60.0


def convert_str_to_topic_mappings(json_data: str) -> List[TopicMappingEntry]:
    json_list = json.loads(json_data)
    return [TopicMappingEntry(**m) for m in json_list]


class FilterTransformParams(EventBusBaseModel):
    include_events: Optional[List[StrictStr]] = None
    exclude_events: Optional[List[StrictStr]] = None
