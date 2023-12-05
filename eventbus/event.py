import json
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from confluent_kafka import Message
from pydantic import BaseModel

from eventbus.errors import EventValidationError


class Event(BaseModel):
    """
    published: YYYY-MM-DD[T]HH:MM[:SS[.ffffff]][Z or [±]HH[:]MM]]]
               int or float as a string (assumed as Unix time)
    """

    id: str
    title: str
    published: str
    payload: str

    class Config:
        arbitrary_types_allowed = True
        underscore_attrs_are_private = True

    def __str__(self):
        return f"Event({self.title}#{self.id})"


class KafkaEvent(Event):
    is_subscribed: bool = True
    msg: Message

    @property
    def topic(self) -> Optional[str]:
        return self.msg.topic()

    @property
    def partition(self) -> Optional[int]:
        return self.msg.partition()

    @property
    def offset(self) -> Optional[int]:
        return self.msg.offset()

    def __str__(self):
        return f"KafkaEvent({self.title}#{self.id}@{self.topic}:{self.partition}:{self.offset})"


class EventStatus(str, Enum):
    DONE = "done"
    DEAD_LETTER = "dead_letter"
    DISCARD = "discard"


def create_kafka_message(event: Event) -> Tuple[str, str]:
    return event.id, event.payload


def parse_kafka_message(msg: Message) -> KafkaEvent:
    payload = msg.value()
    if not payload:
        raise EventValidationError(f"Message value must not be empty.")

    try:
        json_body = json.loads(payload)
    except Exception:
        raise EventValidationError(f"Request body must not an non-empty Json.")

    if not isinstance(json_body, dict):
        raise EventValidationError("Invalid format of the event")

    event_attrs = {
        "id": json_body.get("id"),
        "title": json_body.get("title"),
        "published": json_body.get("published"),
        "payload": payload,
        "msg": msg,
    }
    return KafkaEvent(**event_attrs)


# TODO improve performance of this func
def parse_request_body(request_body: str) -> List[Event]:
    try:
        json_body = json.loads(request_body)
    except Exception:
        raise EventValidationError(f"Request body must not an non-empty Json.")

    def create_event(_json: Dict[str, Any]) -> Event:
        try:
            event_attrs = {
                "id": _json.get("id"),
                "title": _json.get("title"),
                "published": _json.get("published"),
                "payload": json.dumps(_json),
            }
            return Event(**event_attrs)
        except Exception as ex:
            raise EventValidationError(str(ex))

    if isinstance(json_body, list):
        return [create_event(_json) for _json in json_body]
    elif isinstance(json_body, dict):
        return [create_event(json_body)]
    else:
        raise EventValidationError("Invalid format of the event")
