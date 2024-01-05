import json
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

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

    def __str__(self):
        return f"Event({self.title}#{self.id})"


KafkaTP = aiokafka.TopicPartition


class KafkaEvent(Event):
    tp: KafkaTP
    offset: int
    key: Optional[str] = None

    @property
    def topic(self) -> str:
        return self.tp.topic

    @property
    def partition(self) -> int:
        return self.tp.partition

    def __str__(self):
        return f"KafkaEvent({self.title}#{self.id}@{self.topic}:{self.partition}:{self.offset})"


class EventStatus(str, Enum):
    DONE = "done"
    DEAD_LETTER = "dead_letter"
    DISCARD = "discard"


def create_kafka_message(event: Event) -> Tuple[str, str]:
    return event.id, event.payload


def parse_aiokafka_msg(msg) -> KafkaEvent:
    from aiokafka import ConsumerRecord

    assert isinstance(msg, ConsumerRecord)

    payload = msg.value
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
        "tp": KafkaTP(topic=msg.topic, partition=msg.partition),
        "offset": msg.offset,
        "key": msg.key,
    }
    return KafkaEvent(**event_attrs)


def parse_confluent_kafka_msg(msg) -> KafkaEvent:
    from confluent_kafka import Message

    assert isinstance(msg, Message)

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
