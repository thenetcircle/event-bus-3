from typing import Optional

from eventbus.errors import InitError
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

    def teardown(self) -> None:
        if self.producer:
            self.producer.close()

    async def handler_event(self, event: Event) -> None:
        if not self.topic_resolver or not self.producer:
            raise InitError("EventHandler must to be inited before use.")

        if event_topic := self.topic_resolver.resolve(event.title):
            event.topic = event_topic
        msg = await self.producer.produce(event)
        print(msg)
