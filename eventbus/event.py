import json
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union

from pydantic import BaseModel, ConfigDict
import aiokafka

from eventbus.errors import EventValidationError


class Event(BaseModel):
    """
    published: YYYY-MM-DD[T]HH:MM[:SS[.ffffff]][Z or [±]HH[:]MM]]]
               int or float as a string (assumed as Unix time)
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    id: str
    title: str
    published: str
    payload: str
    extra: Optional[Dict[str, Any]] = None

    def __str__(self):
        # return f'Event(id="{self.title}"#{self.id})'
        return self.json(exclude=["payload"], exclude_none=True, exclude_defaults=True)


KafkaTP = aiokafka.TopicPartition


class KafkaEvent(Event):
    topic: str
    partition: int
    offset: int
    key: Optional[str] = None

    def __str__(self):
        # return f"KafkaEvent({self.title}#{self.id}@{self.topic}:{self.partition}:{self.offset})"
        return self.json(exclude=["payload"], exclude_none=True, exclude_defaults=True)


class EventStatus(str, Enum):
    DONE = "DONE"
    DEAD_LETTER = "DEAD_LETTER"
    UNKNOWN = "UNKNOWN"


def create_kafka_message(event: Event) -> Tuple[str, str]:
    return event.id, event.payload


def _construct_event_attrs(data: Dict[str, Any], payload: str) -> Dict[str, Any]:
    extra = {}
    if "actor" in data and isinstance(data["actor"], dict) and "id" in data["actor"]:
        extra["actor_id"] = data["actor"]["id"]
    if "object" in data and isinstance(data["object"], dict) and "id" in data["object"]:
        extra["object_id"] = data["object"]["id"]
    if "target" in data and isinstance(data["target"], dict) and "id" in data["target"]:
        extra["target_id"] = data["target"]["id"]

    attrs = {
        "id": data.get("id"),
        "title": data.get("title"),
        "published": data.get("published"),
        "payload": json.dumps(data),
    }
    if len(extra) > 0:
        attrs["extra"] = extra
    return attrs


def parse_request_body(request_body: Union[str, bytes]) -> List[Event]:
    try:
        json_body = json.loads(request_body)
    except Exception:
        raise EventValidationError(f"Request body must not an non-empty Json.")

    def create_event(data: Dict[str, Any]) -> Event:
        try:
            event_attrs = _construct_event_attrs(data, json.dumps(data))
            return Event(**event_attrs)
        except Exception as ex:
            raise EventValidationError(str(ex))

    if isinstance(json_body, list):
        return [create_event(_json) for _json in json_body]
    elif isinstance(json_body, dict):
        return [create_event(json_body)]
    else:
        raise EventValidationError("Invalid format of the event")


def parse_aiokafka_msg(msg) -> KafkaEvent:
    from aiokafka import ConsumerRecord

    assert isinstance(msg, ConsumerRecord)

    payload = msg.value
    if not payload:
        raise EventValidationError(f"Message value must not be empty.")

    try:
        data = json.loads(payload)
    except Exception:
        raise EventValidationError(f"Request body must not an non-empty Json.")

    if not isinstance(data, dict):
        raise EventValidationError("Invalid format of the event")

    event_attrs = _construct_event_attrs(data, payload)
    event_attrs.update(
        {
            "topic": msg.topic,
            "partition": msg.partition,
            "offset": msg.offset,
            "key": msg.key,
        }
    )
    return KafkaEvent(**event_attrs)
