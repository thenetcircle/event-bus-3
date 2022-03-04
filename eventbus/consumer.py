import asyncio
from asyncio import Queue
from threading import Thread
from typing import Dict, List, Optional

import aiohttp
import janus
from aiohttp import ClientSession
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

        send_queue = janus.Queue(maxsize=100)
        commit_queue = janus.Queue(maxsize=100)

        self._fetch_events_thread = Thread(
            target=self._fetch_events,
            args=(send_queue.sync_q, commit_queue.sync_q),
            name=f"consumer-{self.id}-fetch-events-thread",
        )
        self._fetch_events_thread.start()

        await self._wait_and_deliver_events(send_queue.async_q, commit_queue.async_q)

    def close(self) -> None:
        logger.warning('Closing Kafka Consumer "{}"', self.id)
        self._cancelled = True
        self._internal_consumer.close()
        self._internal_consumer = None

    def _fetch_events(
        self, send_queue: janus.SyncQueue, commit_queue: janus.SyncQueue
    ) -> None:
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
                            self._enqueue_until_cancelled(
                                commit_queue, "commit_queue", kafka_event
                            )
                        else:
                            # put the event into the send_queue
                            self._enqueue_until_cancelled(
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

    async def _wait_and_deliver_events(
        self,
        send_queue: janus.AsyncQueue,
        commit_queue: janus.AsyncQueue,
    ) -> None:
        tp_queues: Dict[str, Queue] = {}
        tp_queue_size = self._consumer_conf.concurrent_per_partition * 3
        tp_consuming_tasks = []
        async with ClientSession() as client:
            while True:
                new_event: KafkaEvent = await send_queue.get()
                tp = self._get_tp_from_event(new_event)
                if tp in tp_queues:
                    await tp_queues[tp].put(new_event)
                else:
                    tp_queues[tp] = Queue(maxsize=tp_queue_size)
                    await tp_queues[tp].put(new_event)
                    consuming_task = asyncio.create_task(
                        self._consume_tp_queue(client, tp_queues[tp])
                    )
                    tp_consuming_tasks.append(consuming_task)

    async def _consume_tp_queue(self, client: ClientSession, tp_queue: Queue):
        while True:
            concurrent_sending_tasks = []
            for i in range(self._consumer_conf.concurrent_per_partition):
                new_event = await tp_queue.get()
                sending_task = asyncio.create_task(
                    self._send_one_event(client, new_event)
                )
                concurrent_sending_tasks.append(sending_task)

            results = await asyncio.gather(*concurrent_sending_tasks)
            # TODO process the results

    async def _send_one_event(self, client: ClientSession, kafka_event: KafkaEvent):
        try:
            req_func = getattr(client, self._consumer_conf.sink.method.lower())
            req_kwargs = {"data": kafka_event.event.payload}
            if self._consumer_conf.sink.headers:
                req_kwargs["headers"] = self._consumer_conf.sink.headers

            async with req_func(self._consumer_conf.sink.url, **req_kwargs) as resp:
                print(resp.status)
                print(await resp.text())

        except Exception as ex:
            logger.error(
                'Send kafka_event "{}" failed with exception: {}',
                kafka_event,
                ex,
            )

    def _enqueue_until_cancelled(
        self,
        _queue: janus.SyncQueue,
        _queue_name: str,
        _kafka_event: KafkaEvent,
        timeout=0.2,
    ):
        while not self._cancelled:
            try:
                _queue.put(_kafka_event, block=True, timeout=timeout)
                logger.debug(
                    "A kafka event has been put into the {}, current queue size: {}",
                    _queue_name,
                    _queue.qsize(),
                )
            except janus.SyncQueueFull:
                logger.debug("The {} is full, will retry", _queue_name)

    @staticmethod
    def _get_tp_from_event(event: KafkaEvent):
        return event.kafka_msg.topic() + "_" + str(event.kafka_msg.partition())

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
