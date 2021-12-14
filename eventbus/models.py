from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, StrictStr


class Namespace(BaseModel):
    name: StrictStr


DEFAULT_NAMESPACE = Namespace(name="default")


class Event(BaseModel):
    namespace: Namespace = DEFAULT_NAMESPACE
    id: StrictStr
    name: StrictStr
    published: datetime
    payload: StrictStr
    metadata: Optional[Dict[str, Any]] = None
