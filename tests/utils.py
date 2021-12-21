from datetime import datetime
from typing import Any

from eventbus.event import Event


def create_event_from_dict(_dict: [str, Any]) -> Event:
    return Event(
        namespace=_dict.get("namespace") or "n1",
        title=_dict.get("title") or "user.login",
        id=_dict.get("id") or "test_event_1",
        published=_dict.get("published") or datetime.now(),
        payload=_dict.get("payload") or "{}",
        summary=_dict.get("summary"),
        topic=_dict.get("topic"),
    )
