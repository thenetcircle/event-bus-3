import asyncio
from eventbus.model import EventBusBaseModel
from asyncio import AbstractEventLoop
from typing import Any, Dict, List, Optional

from aiokafka import AIOKafkaConsumer, ConsumerRecord, TopicPartition
from aiokafka.abc import ConsumerRebalanceListener
from loguru import logger

from eventbus.event import KafkaTP, KafkaEvent, parse_aiokafka_msg
from eventbus.metrics import stats_client


class KafkaConsumerParams(EventBusBaseModel):
    client_args: Dict[str, Any]  # AIOKafkaConsumer.__init__ args
    topics: Optional[List[str]] = None
    topic_pattern: Optional[str] = None
    poll_timeout: int = 1000  # ms, AIOKafkaConsumer.getmany
    pull_max_records: int = 100


class KafkaConsumer:
    """
    A wrapper of aiokafka AIOKafkaConsumer

    More details: https://aiokafka.readthedocs.io/en/stable/api.html#consumer-class
    """

    def __init__(self, params: KafkaConsumerParams):
        self._params = params
        self._loop: AbstractEventLoop = None
        self._consumer: AIOKafkaConsumer = None

    async def init(self):
        logger.info("Initializing KafkaConsumer")
        self._loop = asyncio.get_running_loop()
        self._consumer = AIOKafkaConsumer(**self._params.client_args)
        self._consumer.subscribe(
            topics=self._params.topics or (),
            pattern=self._params.topic_pattern,
            listener=MyAssignmentListener(),
        )
        await self._consumer.start()
        logger.info("KafkaConsumer has been initialized")

    async def close(self):
        try:
            logger.info("Closing KafkaConsumer")
            if self._consumer:
                await self._consumer.stop()
            logger.info("KafkaConsumer has been closed")
        except Exception as ex:
            logger.exception("Closing AioConsmer failed")

    async def poll(self) -> Dict[KafkaTP, List[KafkaEvent]]:
        msgs: Dict[TopicPartition, List[ConsumerRecord]] = await self._consumer.getmany(
            timeout_ms=self._params.poll_timeout,
            max_records=self._params.pull_max_records,
        )
        if len(msgs) > 0:
            logger.info(
                "Have polled some messages from Kafka(format: [(topic-partitions, amouont)]): {}",
                [(tp, len(records)) for tp, records in msgs.items()],
            )

        results = {}
        for tp, records in msgs.items():
            results[tp] = []
            for record in records:
                try:
                    event = parse_aiokafka_msg(record)
                    results[tp].append(event)

                    logger.bind(event=event).info(
                        "Parsed an event from the Kafka message"
                    )
                except Exception as ex:
                    logger.bind(record=record).exception("Parse a Kafka message failed")

        return results

    async def commit(self, offsets: Optional[Dict[KafkaTP, int]] = None) -> None:
        try:
            await self._consumer.commit(offsets=offsets)
            logger.debug("Committed offsets {}", offsets)
        except Exception as ex:
            logger.exception("Committing offsets to Kafka failed")
            raise


class MyAssignmentListener(ConsumerRebalanceListener):
    """
    Warning When using manual commit it is recommended to provide a ConsumerRebalanceListener
    which will process pending messages in the batch and commit before allowing rejoin.
    If your group will rebalance during processing commit will fail with CommitFailedError,
    as partitions may have been processed by other consumer already.
    """

    def on_partitions_revoked(self, revoked: List[TopicPartition]):
        # stats_client.incr("consumer.partitions.revoked")
        logger.info("Called on_partitions_revoked with partitions: {}", revoked)

    def on_partitions_assigned(self, assigned: List[TopicPartition]):
        # stats_client.incr("consumer.partitions.assigned")
        logger.info("Called on_partitions_assigned with partitions: {}", assigned)
