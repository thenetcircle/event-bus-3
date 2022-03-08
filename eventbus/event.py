import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from confluent_kafka.cimpl import TIMESTAMP_NOT_AVAILABLE, Message
from pydantic import BaseModel, Field

from eventbus.errors import EventValidationError


class Event(BaseModel):
    """
    published: YYYY-MM-DD[T]HH:MM[:SS[.ffffff]][Z or [±]HH[:]MM]]]
               int or float as a string (assumed as Unix time)
    """

    id: str = Field(min_length=2, max_length=500, regex=r"[\w-]+")
    title: str
    published: datetime
    payload: str

    def __str__(self):
        return f"{self.title}#{self.id}"


class KafkaEvent(Event):

    topic: str
    partition: int
    offset: int
    timestamp: Optional[int]

    def __str__(self):
        return super().__str__() + f"(:{self.topic},{self.partition},{self.offset})"


def parse_kafka_message(msg: Message) -> KafkaEvent:
    try:
        json_body = json.loads(msg.value())
    except Exception:
        raise EventValidationError(f"Request body must not an non-empty Json.")

    if not isinstance(json_body, dict):
        raise EventValidationError("Invalid format of the event")

    msg_timestamp = msg.timestamp()

    event_attrs = {
        "id": json_body.get("id"),
        "title": json_body.get("title"),
        "published": json_body.get("published"),
        "payload": msg.value(),
        "topic": msg.topic(),
        "partition": msg.partition(),
        "offset": msg.offset(),
        "timestamp": (
            msg_timestamp[1] if msg_timestamp[0] != TIMESTAMP_NOT_AVAILABLE else None
        ),
    }
    return KafkaEvent(**event_attrs)


def parse_request_body(request_body: str) -> List[Event]:
    try:
        json_body = json.loads(request_body)
    except Exception:
        raise EventValidationError(f"Request body must not an non-empty Json.")

    def create_event(_json: Dict[str, Any]) -> Event:
        event_attrs = {
            "id": _json.get("id"),
            "title": _json.get("title"),
            "published": _json.get("published"),
            "payload": request_body,
        }
        return Event(**event_attrs)

    if isinstance(json_body, list):
        return [create_event(_json) for _json in json_body]
    elif isinstance(json_body, dict):
        return [create_event(json_body)]
    else:
        raise EventValidationError("Invalid format of the event")
