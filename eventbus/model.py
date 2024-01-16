from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional, Tuple
from dataclasses import dataclass

from pydantic import BaseModel, ConfigDict

from eventbus.event import Event, EventStatus, KafkaEvent


class EventBusBaseModel(BaseModel):
    model_config = ConfigDict(frozen=True)


class SinkType(str, Enum):
    HTTP = "HTTP"


@dataclass(frozen=True)
class SinkResult:
    event: Event
    status: EventStatus
    error: Optional[Exception] = None


class AbsSink(ABC):
    @abstractmethod
    async def init(self):
        raise NotImplementedError

    @abstractmethod
    async def send_event(self, event: Event) -> SinkResult:
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
