import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from confluent_kafka.cimpl import Message
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
    summary: Optional[str] = None
    topic: Optional[str] = None

    def __str__(self):
        event_info = f"{self.title}#{self.id}@{self.published}"
        if self.summary:
            event_info += f"{{{self.summary}}}"
        return event_info


class KafkaEvent(BaseModel):
    kafka_msg: Message
    event: Event

    def __str__(self):
        return (
            f"KafkaEvent({self.event} "
            f"from {self.kafka_msg.topic()}[{self.kafka_msg.partition()}]#{self.kafka_msg.offset()})"
        )


def parse_kafka_message(msg: Message) -> KafkaEvent:
    events = parse_request_body(msg.value())
    if len(events) != 1:
        raise EventValidationError("Invalid format of the event")
    return KafkaEvent(event=events[0], kafka_msg=msg)


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
            "summary": _json.get("summary"),
        }
        return Event(**event_attrs)

    if isinstance(json_body, list):
        return [create_event(_json) for _json in json_body]
    elif isinstance(json_body, dict):
        return [create_event(json_body)]
    else:
        raise EventValidationError("Invalid format of the event")
