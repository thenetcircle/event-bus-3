import asyncio
from asyncio import Queue
from datetime import datetime
from enum import Enum
from threading import Thread
from typing import Dict, List, Optional

import aiohttp
import janus
from aiohttp import ClientSession
from confluent_kafka.cimpl import Consumer, Message, TopicPartition
from loguru import logger

from eventbus.config import ConsumerConfig
from eventbus.errors import EventConsumingError, InitConsumerError
from eventbus.event import KafkaEvent, parse_kafka_message


class SendEventResult(str, Enum):
    DONE = "done"
    RETRY_LATER = "retry_later"
    DISCARD = "discard"


class KafkaConsumer:
    def __init__(self, config: ConsumerConfig, topics: List[str]):
        # TODO check args, kafka_config, group.id, bootstrap.servers, ...
        self._id = config.id
        self._config = config
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

        self._internal_consumer = Consumer(self._config.kafka_config, logger=logger)
        self._internal_consumer.subscribe(
            self.subscribed_topics, on_assign=self._on_assign, on_revoke=self._on_revoke
        )

        send_queue = janus.Queue(maxsize=100)
        commit_queue = janus.Queue(maxsize=100)
        retry_queue = janus.Queue(maxsize=100)

        self._fetch_events_thread = Thread(
            target=self._fetch_events,
            args=(send_queue.sync_q, commit_queue.sync_q),
            name=f"consumer-{self.id}-fetch-events-thread",
        )
        self._fetch_events_thread.start()

        await self._wait_and_deliver_events(
            send_queue.async_q, commit_queue.async_q, retry_queue.async_q
        )

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
                        if kafka_event.event.title not in self._config.subscribe_events:
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
        retry_queue: janus.AsyncQueue,
    ) -> None:
        tp_queues: Dict[str, Queue] = {}
        tp_queue_size = self._config.concurrent_per_partition * 3
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
                        self._consume_tp_queue(
                            client, tp_queues[tp], commit_queue, retry_queue
                        )
                    )
                    tp_consuming_tasks.append(consuming_task)

    async def _consume_tp_queue(
        self,
        client: ClientSession,
        tp_queue: Queue,
        commit_queue: janus.AsyncQueue,
        retry_queue: janus.AsyncQueue,
    ):
        while True:
            concurrent_sending_tasks = []
            for i in range(self._config.concurrent_per_partition):
                new_event = await tp_queue.get()
                sending_task = asyncio.create_task(
                    self._send_one_event(client, new_event, commit_queue, retry_queue)
                )
                concurrent_sending_tasks.append(sending_task)

            results = await asyncio.gather(*concurrent_sending_tasks)
            # TODO process the results

    async def _send_one_event(
        self, client: ClientSession, kafka_event: KafkaEvent
    ) -> SendEventResult:
        retry_times = 0
        max_retry_times = self._config.sink.max_retry_times

        req_func = getattr(client, self._config.sink.method.lower())
        req_kwargs = {
            "data": kafka_event.event.payload,
            "timeout": aiohttp.ClientTimeout(total=self._config.sink.timeout),
        }
        if self._config.sink.headers:
            req_kwargs["headers"] = self._config.sink.headers
        req_url = self._config.sink.url

        while True:
            start_time = datetime.now()
            try:
                async with req_func(req_url, **req_kwargs) as resp:
                    if resp.status == 200:
                        resp_body = await resp.text()
                        if resp_body == "ok":
                            logger.info(
                                'That sending an event "{}" to "{}" succeeded in {} seconds after {} times retires',
                                kafka_event,
                                req_url,
                                self._get_cost_time(start_time),
                                retry_times,
                            )
                            return SendEventResult.DONE

                        elif resp_body == "retry":
                            # retry logic
                            if retry_times >= max_retry_times:
                                logger.info(
                                    'That sending an event "{}" to "{}" exceeded max retry times {} in {} seconds',
                                    kafka_event,
                                    req_url,
                                    retry_times,
                                    self._get_cost_time(start_time),
                                )
                                return SendEventResult.RETRY_LATER
                            else:
                                retry_times += 1
                                continue

                        else:
                            # unexpected resp
                            logger.warning(
                                'That sending an event "{}" to "{}" failed in {} seconds because of unexpected response: {}',
                                kafka_event,
                                req_url,
                                self._get_cost_time(start_time),
                                resp_body,
                            )
                            return SendEventResult.RETRY_LATER

                    else:
                        logger.warning(
                            'That sending an event "{}" to "{}" failed in {} seconds because of non-200 status code: {}',
                            kafka_event,
                            req_url,
                            self._get_cost_time(start_time),
                            resp.status,
                        )

                        # non-200 status code, use retry logic
                        if retry_times >= max_retry_times:
                            logger.info(
                                'That sending an event "{}" to "{}" exceeded max retry times {} in {} seconds',
                                kafka_event,
                                req_url,
                                retry_times,
                                self._get_cost_time(start_time),
                            )
                            return SendEventResult.RETRY_LATER
                        else:
                            retry_times += 1
                            continue

            # more details of aiohttp errors can be found here:
            # https://docs.aiohttp.org/en/stable/client_reference.html#aiohttp.ClientPayloadError
            except (
                aiohttp.ClientConnectionError,
                aiohttp.InvalidURL,
                asyncio.exceptions.TimeoutError,
            ) as ex:
                logger.error(
                    'That sending an event "{}" to "{}" failed in {} seconds because of "{}", details: {}',
                    kafka_event,
                    req_url,
                    self._get_cost_time(start_time),
                    type(ex),
                    ex,
                )
                # TODO trigger alert
                # keep retry
                await asyncio.sleep(0.1)

            except (
                aiohttp.ClientResponseError,  # this is mostly the response related error
                aiohttp.ClientPayloadError,  # this is response data error
            ) as ex:
                # since it's response related errors, it could be recovered later by improving the target
                # at least we shouldn't block other subsequence events
                # so just return retry_later
                logger.error(
                    'That sending an event "{}" to "{}" failed in {} seconds because of "{}", details: {}',
                    kafka_event,
                    req_url,
                    self._get_cost_time(start_time),
                    type(ex),
                    ex,
                )
                # TODO trigger alert
                return SendEventResult.RETRY_LATER

            except Exception as ex:
                logger.error(
                    'That sending an event "{}" to "{}" failed in {} seconds because of a unknown exception "{}", details : {}',
                    kafka_event,
                    req_url,
                    self._get_cost_time(start_time),
                    type(ex),
                    ex,
                )
                # TODO trigger alert
                # keep retry until fixed
                await asyncio.sleep(0.1)

            retry_times += 1

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

    def _get_cost_time(self, start_time: datetime) -> float:
        return ((datetime.now()) - start_time).total_seconds()
