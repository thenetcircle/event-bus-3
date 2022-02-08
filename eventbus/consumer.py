import asyncio
import queue
from asyncio import Queue as AsyncQueue
from queue import Queue
from threading import Thread
from typing import Dict, List, Optional, Union

import aiohttp
from confluent_kafka.cimpl import Consumer, Message, TopicPartition
from loguru import logger

from eventbus.config import ConsumerInstanceConfig
from eventbus.errors import EventConsumingError, InitConsumerError
from eventbus.event import KafkaEvent, parse_kafka_message


class KafkaConsumer:
    def __init__(self, consumer_conf: ConsumerInstanceConfig, topics: List[str]):
        # TODO check args, kafka_config, group.id, bootstrap.servers, ...
        self._id = consumer_conf.id
        self._consumer_conf = consumer_conf
        self._subscribed_topics = topics
        self._current_topic_partitions: Dict[str, int] = {}
        self._cancelled = False
        self._internal_consumer: Optional[Consumer] = None
        self._fetch_events_thread: Optional[Thread] = None

    @property
    def id(self) -> str:
        return self._id

    @property
    def subscribed_topics(self) -> List[str]:
        return self._subscribed_topics

    async def start(self) -> None:
        """
        - get topics by listening events and topic mapping
        - start the consumer on the topic-partitions
        - send to sink
        - update offsets on redis, and periodically commit to kafka
        """

        if self._internal_consumer:
            raise InitConsumerError("The Kafka consumer has been already started.")

        self._internal_consumer = Consumer(
            self._consumer_conf.kafka_config, logger=logger
        )
        self._internal_consumer.subscribe(
            self.subscribed_topics, on_assign=self._on_assign, on_revoke=self._on_revoke
        )

        send_queue = Queue(maxsize=100)
        commit_queue = Queue(maxsize=100)

        self._fetch_events_thread = Thread(
            target=self._fetch_events,
            args=(send_queue, commit_queue),
            name=f"consumer-{self.id}-fetch-events-thread",
        )
        self._fetch_events_thread.start()

        await self._send_events(send_queue, commit_queue)

    def close(self) -> None:
        logger.warning('Closing Kafka Consumer "{}"', self.id)
        self._cancelled = True
        self._internal_consumer.close()
        self._internal_consumer = None

    def _fetch_events(
        self,
        send_queue: Queue,
        commit_queue: Queue,
    ) -> None:
        def enqueue_until_cancelled(
            _queue: Queue, _queue_name: str, _kafka_event: KafkaEvent, timeout=0.2
        ):
            while not self._cancelled:
                try:
                    _queue.put(_kafka_event, block=True, timeout=timeout)
                    logger.debug(
                        "A kafka event has been put into the {}, current queue size: {}",
                        _queue_name,
                        _queue.qsize(),
                    )
                except queue.Full:
                    logger.debug("The {} is full, will retry", _queue_name)

        try:
            while not self._cancelled:
                try:
                    msg: Message = self._internal_consumer.poll(timeout=1.0)
                    if msg is None:
                        continue
                    if msg.error():
                        raise EventConsumingError(msg.error())
                    else:
                        logger.debug(
                            "Get a new Kafka Message from {} [{}] at offset {} with key {}",
                            msg.topic(),
                            msg.partition(),
                            msg.offset(),
                            str(msg.key()),
                        )
                        try:
                            kafka_event: KafkaEvent = parse_kafka_message(msg)
                        except Exception as ex:
                            logger.error(
                                'Parsing kafka message: "{}" failed with error: "{}"',
                                msg.value(),
                                ex,
                            )
                            # skip this event if parsed failed
                            continue

                        # if the events from the topics not in subscribed events
                        if kafka_event.event.title not in self._consumer_conf.events:
                            logger.debug(
                                'Get a new event "{}" which is not in subscribed events list, skip it',
                                kafka_event.event.title,
                            )
                            # put the event into the commit_queue
                            enqueue_until_cancelled(
                                commit_queue, "commit_queue", kafka_event
                            )
                        else:
                            # put the event into the send_queue
                            enqueue_until_cancelled(
                                send_queue, "send_queue", kafka_event
                            )

                except Exception as ex:
                    logger.error(
                        'The fetch_events_thread of consumer "{}" get a new exception: {}',
                        self.id,
                        ex,
                    )

        except KeyboardInterrupt:
            logger.warning('The Kafka consumer "{}" aborted by user', self.id)

    async def _send_events(
        self,
        send_queue: Queue,
        commit_queue: Queue,
    ):
        buffer_size = len(self.subscribed_topics) * 3 * 3
        buffer = AsyncQueue(maxsize=buffer_size)

        async with aiohttp.ClientSession() as session:
            while True:
                # filling the buffer
                while True:
                    try:
                        _kafka_event = send_queue.get_nowait()
                        buffer.put_nowait(_kafka_event)
                    except (queue.Empty, asyncio.QueueFull):
                        break

                if buffer.empty():
                    logger.debug("The kafka_events buffer is empty, continue")
                    await asyncio.sleep(0.1)
                    continue

                # sending the events from the buffer
                req_stats = {}
                for kafka_event in buffer:
                    _tp = (
                        kafka_event.kafka_msg.topic()
                        + "_"
                        + str(kafka_event.kafka_msg.partition())
                    )

                    if _tp in req_stats:
                        if (
                            req_stats[_tp]
                            >= self._consumer_conf.concurrent_per_partition
                        ):
                            # skip this event if the partition already have enough reqs on the fly.
                            continue
                        else:
                            req_stats[_tp] += 1
                    else:
                        req_stats[_tp] = 1

                    # TODO del the event from the buffer
                    # TODO send batch requsets together
                    # TODO block until the batch done

                    try:
                        req_func = getattr(
                            session, self._consumer_conf.sink.method.lower()
                        )
                        async with req_func(
                            self._consumer_conf.sink.url, data=kafka_event.event.payload
                        ) as resp:
                            print(resp.status)
                            print(await resp.text())

                    except Exception as ex:
                        logger.error(
                            'Send kafka_event "{}" failed with exception: {}',
                            kafka_event,
                            ex,
                        )

    def _on_assign(self, consumer: Consumer, partitions: List[TopicPartition]) -> None:
        logger.info(
            'KafkaConsumer "{}" get assigned new TopicPartitions: "{}"',
            self.id,
            partitions,
        )

    def _on_revoke(self, consumer: Consumer, partitions: List[TopicPartition]) -> None:
        logger.info(
            'KafkaConsumer "{}" get revoked TopicPartitions: "{}"', self.id, partitions
        )
