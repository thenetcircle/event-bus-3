from eventbus.model import EventBusBaseModel

from eventbus.event import Event, create_kafka_message
from eventbus.errors import InitKafkaProducerError
import asyncio
from aiokafka import AIOKafkaProducer

from loguru import logger
from asyncio import AbstractEventLoop
from typing import Any, Dict, List, Optional


class KafkaProducerParams(EventBusBaseModel):
    client_args: Dict[str, Any]  # AIOKafkaConsumer.__init__ args


class KafkaProducer:
    """
    A wrapper of aiokafka AIOKafkaProducer

    More details: https://aiokafka.readthedocs.io/en/stable/producer.html
    """

    def __init__(self, params: KafkaProducerParams):
        self._params = params
        self._loop: AbstractEventLoop = None
        self._producer: AIOKafkaProducer = None

    async def init(self):
        logger.info("Initing KafkaProducer")
        self._loop = asyncio.get_running_loop()
        self._producer = AIOKafkaProducer(**self._params.client_args)
        await self._producer.start()
        logger.info("Inited KafkaProducer")

    async def close(self):
        try:
            logger.info("Closing KafkaProducer")
            if self._producer:
                await self._producer.stop()
            logger.info("Closed KafkaProducer")
        except Exception as ex:
            logger.error(
                "Closing KafkaProducer failed with error: <{}> {}",
                type(ex).__name__,
                ex,
            )

    async def produce(self, topic: str, event: Event):
        if self._producer is None or self._loop is None:
            logger.error(
                "KafkaProducer is not inited, _producer: {}, _loop: {}",
                self._producer,
                self._loop,
            )
            raise RuntimeError(
                "Need init KafkaProducer before call the produce method."
            )

        try:
            key, value = create_kafka_message(event)
            await self._producer.send_and_wait(topic, value, key=key)
        except Exception as ex:
            logger.error(
                "KafkaProducer produce event failed with error: <{}> {}",
                type(ex).__name__,
                ex,
            )
            raise
