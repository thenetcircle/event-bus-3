import asyncio
from asyncio import AbstractEventLoop
from typing import Any, Dict

from aiokafka import AIOKafkaProducer
from aiokafka.errors import OutOfOrderSequenceNumber, UnknownProducerId
from loguru import logger

from eventbus.event import Event, LogEventStatus, create_kafka_message
from eventbus.model import EventBusBaseModel


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
        logger.info("Initializing KafkaProducer")
        self._loop = asyncio.get_running_loop()
        self._producer = await self._start_new_producer()
        logger.info("KafkaProducer has been initialized")

    async def close(self):
        try:
            logger.info("Closing KafkaProducer")
            if self._producer:
                await self._producer.stop()
            logger.info("KafkaProducer has been closed")
        except Exception as ex:
            logger.exception("Closing KafkaProducer failed")

    async def produce(self, topic: str, event: Event):
        if self._producer is None or self._loop is None:
            logger.error(
                "KafkaProducer has not been initialized, _producer and _loop should not be None."
            )
            raise RuntimeError(
                "Need initialize KafkaProducer before call the produce method."
            )

        with logger.contextualize(topic=topic, event=event):
            try:
                key, value = create_kafka_message(event)
                logger.bind(status=LogEventStatus.TO_KAFKA, key=key).info(
                    "Send Event to Kafka",
                )
                result = await self._producer.send_and_wait(
                    topic, value.encode("utf-8"), key=key.encode("utf-8")
                )
                logger.bind(status=LogEventStatus.IN_KAFKA, result=result).info(
                    "Event send to Kafka successfully",
                )
                return result
            except (OutOfOrderSequenceNumber, UnknownProducerId) as ex:
                logger.exception(f"Get exception: {ex}, restarting producer.")
                await self._restart_producer()
                await self.produce(topic, event)
            except Exception as ex:
                logger.exception("Event send to Kafka failed")
                raise

    async def _restart_producer(self):
        _old_producer = self._producer
        self._producer = await self._start_new_producer()
        await _old_producer.stop()

    async def _start_new_producer(self):
        _new_producer = AIOKafkaProducer(**self._params.client_args)
        await _new_producer.start()
        return _new_producer
