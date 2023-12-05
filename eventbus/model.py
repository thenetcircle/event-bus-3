import json
from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, StrictStr


class EventBusModel(BaseModel):
    class Config:
        allow_mutation = False


class HttpSinkMethod(str, Enum):
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"


class HttpSinkInfo(EventBusModel):
    url: StrictStr
    method: HttpSinkMethod = HttpSinkMethod.POST
    headers: Optional[Dict[str, str]] = None
    timeout: float = 300  # seconds
    max_retry_times: int = 3
    backoff_retry_step: float = 0.1
    backoff_retry_max_time: float = 60.0


class StoryInfo(EventBusModel):
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


class TopicMapping(EventBusModel):
    topic: StrictStr
    patterns: List[StrictStr]


def convert_str_to_topic_mappings(json_data: str) -> List[TopicMapping]:
    json_list = json.loads(json_data)
    return [TopicMapping(**m) for m in json_list]
