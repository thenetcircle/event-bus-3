import asyncio
import logging
from asyncio import AbstractEventLoop
from typing import List, Optional

from confluent_kafka import Consumer, KafkaException, Message, TopicPartition
from loguru import logger

from eventbus import config
from eventbus.config import ConsumerConfig
from eventbus.errors import (
    InvalidArgumentError,
    KafkaConsumerClosedError,
    KafkaConsumerPollingError,
)
from eventbus.event import KafkaEvent, parse_kafka_message
from eventbus.metrics import stats_client


class KafkaConsumer:
    def __init__(
        self,
        id: str,
        consumer_conf: ConsumerConfig,
        subscribe_topics: List[str],
        group_id: Optional[str] = None,
        group_instance_id: Optional[str] = None,
    ):
        self._check_config(consumer_conf)

        self._id = id
        self._config = consumer_conf
        self._subscribe_topics = subscribe_topics
        self._group_id = group_id or self._get_default_group_id()
        self._group_instance_id = group_instance_id
        self._real_consumer: Optional[Consumer] = None
        self._loop: AbstractEventLoop = None

        self._is_closed = False
        self._is_fetching_events = False
        self._is_committing_events = False

    @property
    def id(self):
        return self._id

    @property
    def fullname(self) -> str:
        return f"KafkaConsumer#{self.id}"

    @property
    def config(self):
        return self._config

    @property
    def subscribe_topics(self):
        return self._subscribe_topics

    async def init(self) -> None:
        self._loop = asyncio.get_running_loop()

        consumer_kafka_config = self._config.kafka_config.copy()
        if self._group_id:
            consumer_kafka_config["group.id"] = self._group_id
        if self._group_instance_id:
            consumer_kafka_config["group.instance.id"] = self._group_instance_id
        self._real_consumer = Consumer(
            consumer_kafka_config,
            logger=logging.getLogger(self.fullname),
        )
        self._real_consumer.subscribe(
            self.subscribe_topics,
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
            on_lost=self._on_lost,
        )

    async def close(self) -> None:
        if not self._is_closed:
            try:
                logger.info("Closing {}", self.fullname)
                if self._real_consumer:
                    self._real_consumer.close()
                self._is_closed = True
                logger.info("Closed {}", self.fullname)

            except Exception as ex:
                logger.error(
                    "Closing {} failed with error: <{}> {}",
                    self.fullname,
                    type(ex).__name__,
                    ex,
                )

    async def poll(self, timeout: float) -> Optional[KafkaEvent]:
        if self._is_closed:
            logger.error(
                "Trying to poll data from a closed KafkaConsumer",
            )
            raise KafkaConsumerClosedError

        try:
            msg: Message = self._real_consumer.poll(timeout=timeout)
            if msg is None:
                return None

            if msg.error():
                logger.error(
                    "Polled a message from Kafka with this error: {}", msg.error()
                )
                raise KafkaConsumerPollingError(msg.error())
            else:
                logger.debug(
                    "Polled a message from Kafka, topic-partition: {}-{}, offset: {}",
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                )

                if msg.offset() > 4000000000:
                    logger.error(
                        "Polled a message from Kafka with very high offset {}. topic-partition: {}-{}",
                        msg.offset(),
                        msg.topic(),
                        msg.partition(),
                    )

        except RuntimeError as ex:
            logger.error(
                "Polling a message from Kafka failed with runtime exception: <{}> {}",
                type(ex).__name__,
                ex,
            )
            raise KafkaConsumerClosedError(str(ex))

        except Exception as ex:
            logger.error(
                "Polling a message from Kafka failed with exception: <{}> {}",
                type(ex).__name__,
                ex,
            )
            # TODO trigger an alert
            raise

        stats_client.incr("consumer.msg.new")

        try:
            event: KafkaEvent = parse_kafka_message(msg)
        except Exception as ex:
            logger.error(
                "Parsing a message to KafkaEvent failed. "
                'topic: {}, partition: {}, offset: {}, data: "{}", '
                "with error: <{}> {}",
                msg.topic(),
                msg.partition(),
                msg.offset(),
                msg.value(),
                type(ex).__name__,
                ex,
            )
            # TODO trigger error
            # skip this event if parse failed
            raise

        return event

    async def commit(self, *events: KafkaEvent) -> None:
        if self._is_closed:
            logger.error(
                "Trying to commit event to a closed KafkaConsumer",
            )
            raise KafkaConsumerClosedError

        stats_client.incr("consumer.event.commit.new")

        try:
            # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.Consumer.store_offsets
            self._real_consumer.store_offsets(
                offsets=self._get_offsets_from_events(events)
            )

            stats_client.incr("consumer.event.commit.succ")
            logger.info(
                'Consumer group "{}" has stored offsets from events "{}" ',
                self._group_id,
                events,
            )

        except KafkaException as ex:
            logger.error(
                "Storing events offset failed with exception: <{}> {}",
                type(ex).__name__,
                ex,
            )
            raise

    def _on_assign(self, consumer: Consumer, partitions: List[TopicPartition]) -> None:
        logger.info(
            '{} get assigned new TopicPartitions: "{}"',
            self.fullname,
            partitions,
        )

    def _on_revoke(self, consumer: Consumer, partitions: List[TopicPartition]) -> None:
        logger.info(
            '{} get revoked TopicPartitions: "{}"',
            self.fullname,
            partitions,
        )

    def _on_lost(self, consumer: Consumer, partitions: List[TopicPartition]) -> None:
        logger.info(
            '{} lost TopicPartitions: "{}"',
            self.fullname,
            partitions,
        )

    def _get_default_group_id(self):
        return f"event-bus-3-consumer-{config.get().app.project_id}-{config.get().app.env}-{self.id}"

    @staticmethod
    def _check_config(consumer_conf: ConsumerConfig) -> None:
        kafka_config = consumer_conf.kafka_config
        if "bootstrap.servers" not in kafka_config:
            raise InvalidArgumentError('"bootstrap.servers" is needed')
        # if "group.id" not in kafka_config:
        #     raise InvalidArgumentError('"group.id" is needed')

    @staticmethod
    def _get_offsets_from_events(*events: KafkaEvent) -> List[TopicPartition]:
        """Note: By convention, committed offsets reflect the next message to be consumed, not the last message consumed.
        Refer: https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.Consumer.commit"""

        return [
            TopicPartition(event.topic, event.partition, event.offset + 1)
            for event in events
        ]
