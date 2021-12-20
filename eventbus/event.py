import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from eventbus.errors import EventValidationError


class Event(BaseModel):
    id: str = Field(min_length=2, max_length=500, regex=r"[\w-]+")
    title: str
    published: datetime
    payload: str
    namespace: str = "default"
    summary: Optional[str] = None
    topic: Optional[str] = None

    def __str__(self):
        event_info = f"{self.title}#{self.id}@{self.published}"
        if self.summary:
            event_info += f"{{{self.summary}}}"
        return event_info


def parse_request_body(
    request_body: str, namespace: Optional[str] = None
) -> List[Event]:
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
        if namespace:
            event_attrs["namespace"] = namespace
        return Event(**event_attrs)

    if isinstance(json_body, list):
        return [create_event(_json) for _json in json_body]
    elif isinstance(json_body, dict):
        return [create_event(json_body)]
    else:
        raise EventValidationError("Invalid format of the event")
