from typing import Optional

import anyio

from eventbus.event import Event
from eventbus.producer import KafkaProducer
from eventbus.topic_resolver import TopicResolver


class EventHandler:
    def __init__(self):
        self.topic_resolver: Optional[TopicResolver] = None
        self.producer: Optional[KafkaProducer] = None

    def init(self) -> None:
        """Initialize related components, such as KafkaProducer.
        Better call it after the event loop has been created."""
        self.topic_resolver = TopicResolver()
        self.producer = KafkaProducer()

    async def handler_event(self, event: Event) -> None:
        await anyio.sleep(10)
        print(event)
