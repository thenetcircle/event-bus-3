import json
from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, List, Optional, Tuple

from pydantic import BaseModel, StrictStr

from eventbus.event import EventStatus, KafkaEvent


class EventBusBaseModel(BaseModel):
    class Config:
        allow_mutation = False


# Topic mapping related
class TopicMappingEntry(EventBusBaseModel):
    topic: StrictStr
    patterns: List[StrictStr]


# Sink related
class SinkType(str, Enum):
    HTTP = "HTTP"


class AbsSinkParams(EventBusBaseModel):
    id: StrictStr


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
    EVENT_FILTER = "EVENT_FILTER"


class AbsTransformParams(EventBusBaseModel):
    pass


# Consumer related
class StoryStatus(str, Enum):
    INIT = "INIT"
    DISABLED = "DISABLED"


class StoryParams(EventBusBaseModel):
    id: StrictStr
    kafka_topics: List[StrictStr]
    sink: Dict[SinkType, AbsSinkParams]
    status: StoryStatus
    transforms: Optional[Dict[TransformType, AbsTransformParams]] = None


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


def convert_str_to_topic_mappings(json_data: str) -> List[TopicMappingEntry]:
    json_list = json.loads(json_data)
    return [TopicMappingEntry(**m) for m in json_list]
