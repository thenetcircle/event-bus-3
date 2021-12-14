from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel


class Namespace(BaseModel):
    name: str


DEFAULT_NAMESPACE = Namespace(name="default")


class Event(BaseModel):
    namespace: Namespace = DEFAULT_NAMESPACE
    id: str
    published: datetime
    payload: str
    metadata: Optional[Dict[str, Any]] = None
