import json
from datetime import datetime
from typing import Any, Dict

from eventbus.event import Event, KafkaEvent


def create_event_from_dict(_dict: Dict[str, Any]) -> Event:
    return Event(
        title=_dict.get("title") or "user.login",
        id=_dict.get("id") or "test_event_1",
        published=_dict.get("published") or str(datetime.now()),
        payload=json.dumps(_dict.get("payload") or {}),
    )


def create_kafka_event_from_dict(_dict: Dict[str, Any]) -> KafkaEvent:
    return KafkaEvent(
        title=_dict.get("title") or "user.login",
        id=_dict.get("id") or "test_event_1",
        published=_dict.get("published") or str(datetime.now()),
        payload=_dict.get("payload") or "{}",
        topic=_dict.get("topic") or "topic1",
        partition=_dict.get("partition") or 1,
        offset=_dict.get("offset") or 1,
        key=_dict.get("key") or "",
    )
