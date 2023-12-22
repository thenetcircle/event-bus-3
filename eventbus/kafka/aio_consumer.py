from eventbus.model import EventBusBaseModel
from aiokafka import AIOKafkaConsumer


class AioConsumerParams(EventBusBaseModel):
    url: StrictStr
    method: HttpSinkMethod = HttpSinkMethod.POST
    headers: Optional[Dict[str, str]] = None
    timeout: float = 300  # seconds
    max_retry_times: int = 3
    backoff_retry_step: float = 0.1
    backoff_retry_max_time: float = 60.0


class AioConsumer:
    def __init__(self, bootstrap_servers, group_id, topic, loop):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.topic = topic
        self.loop = loop

    async def init(self):
        pass

    async def __aenter__(self):
        self.consumer = AIOKafkaConsumer(
            self.topic,
            loop=self.loop,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset="earliest",
        )
        await self.consumer.start()
        return self.consumer

    async def __aexit__(self, exc_type, exc, tb):
        await self.consumer.stop()
        return True
