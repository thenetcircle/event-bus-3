from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional, Tuple

from pydantic import BaseModel

from eventbus.event import EventStatus, KafkaEvent


class EventBusBaseModel(BaseModel):
    class Config:
        frozen = True


class SinkType(str, Enum):
    HTTP = "HTTP"


class AbsSink(ABC):
    @abstractmethod
    async def init(self):
        raise NotImplementedError

    @abstractmethod
    async def send_event(self, event: KafkaEvent) -> Tuple[KafkaEvent, EventStatus]:
        raise NotImplementedError

    @abstractmethod
    async def close(self):
        raise NotImplementedError


class TransformType(str, Enum):
    FILTER = "FILTER"


class AbsTransform(ABC):
    @abstractmethod
    async def init(self):
        raise NotImplementedError

    @abstractmethod
    async def run(self, event: KafkaEvent) -> Optional[KafkaEvent]:
        raise NotImplementedError

    @abstractmethod
    async def close(self):
        raise NotImplementedError
