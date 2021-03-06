import asyncio
import logging
import re
import time
from asyncio import AbstractEventLoop
from asyncio import Queue as AsyncioQueue
from typing import Dict, List, Optional, Tuple

import janus
from confluent_kafka import Consumer, KafkaException, Message, TopicPartition
from janus import Queue as JanusQueue
from loguru import logger

from eventbus import config
from eventbus.config import ConsumerConfig
from eventbus.errors import ClosedError, ConsumerPollingError, InvalidArgumentError
from eventbus.event import EventProcessStatus, KafkaEvent, parse_kafka_message
from eventbus.producer import EventProducer
from eventbus.sink import HttpSink, Sink


class EventConsumer:
    def __init__(
        self, id: str, consumer_conf: ConsumerConfig, name: Optional[str] = None
    ):
        self._id = id
        self._name = name or id

        if consumer_conf.disabled:
            raise RuntimeError(
                f"{self.fullname} is disabled, not allowed to be constructed."
            )
        self._config = consumer_conf

        logger.info(
            'Constructing a new EventConsumer with id: "{}", name: "{}", consumer_conf: {}',
            id,
            name,
            consumer_conf,
        )

        self._consumer = KafkaConsumer(self.id, consumer_conf, name=self.name)
        self._sink: Sink = HttpSink(self.name, consumer_conf)
        self._cancelling = False
        self._wait_task = None
        self._tp_tasks = []
        self._send_queue: JanusQueue[KafkaEvent] = None
        self._commit_queue: JanusQueue[Tuple[KafkaEvent, EventProcessStatus]] = None

    @property
    def id(self) -> str:
        return self._id

    @property
    def name(self) -> str:
        return self._name

    @property
    def fullname(self) -> str:
        return f"EventConsumer#{self.name}"

    async def init(self) -> None:
        await self._consumer.init()
        await self._sink.init()
        self._send_queue = JanusQueue(maxsize=self._config.send_queue_size)
        self._commit_queue = JanusQueue(maxsize=self._config.commit_queue_size)

    async def run(
        self,
    ) -> None:
        try:
            logger.info("Running {}", self.fullname)

            start_time = time.time()

            self._wait_task = asyncio.create_task(self._wait_events())

            await asyncio.gather(
                self._consumer.fetch_events(self._send_queue),
                self._wait_task,
                self._consumer.commit_events(self._commit_queue),
            )
        except Exception as ex:
            logger.error(
                "{}'s run failed by error: <{}> {}",
                self.fullname,
                type(ex).__name__,
                ex,
            )
            raise

        finally:
            await self.cancel()
            logger.warning(
                "{} runs over after {} seconds",
                self.fullname,
                time.time() - start_time,
            )

    async def cancel(self) -> None:
        if not self._cancelling:
            self._cancelling = True
            logger.info("Cancelling {}", self.fullname)

            cancelling_tasks = [self._consumer.close(), self._sink.close()]

            self._wait_task.cancel()
            cancelling_tasks.append(self._wait_task)
            for tp_task in self._tp_tasks:
                tp_task.cancel()
                cancelling_tasks.append(tp_task)

            cancel_results = await asyncio.gather(
                *cancelling_tasks, return_exceptions=True
            )

            logger.info(
                "Cancelled {}. send_queue: {}; commit_queue: {}. cancel_results: {}",
                self.fullname,
                self._send_queue.async_q.qsize(),
                self._commit_queue.async_q.qsize(),
                cancel_results,
            )
            self._cancelling = False

        else:
            while self._cancelling:
                await asyncio.sleep(0.1)

    async def _wait_events(
        self,
    ) -> None:
        self._tp_tasks = []
        tp_queues: Dict[str, AsyncioQueue] = {}

        try:
            while True:
                event: KafkaEvent = await self._send_queue.async_q.get()

                tp_name = self._get_topic_partition_str(event)
                if tp_name not in tp_queues:
                    tp_queues[tp_name] = AsyncioQueue(
                        maxsize=self._config.tp_queue_size
                    )
                    tp_task = asyncio.create_task(
                        self._send_tp_events(tp_name, tp_queues[tp_name])
                    )
                    self._tp_tasks.append(tp_task)

                await tp_queues[tp_name].put(event)

        except asyncio.CancelledError:
            logger.info(
                'method "_wait_events" of {} is cancelled. send_queue: {}',
                self.fullname,
                self._send_queue.async_q.qsize(),
            )

        except Exception as ex:
            logger.error(
                'method "_wait_events" of {} is aborted by <{}> {}. send_queue: {}',
                self.fullname,
                type(ex).__name__,
                ex,
                self._send_queue.async_q.qsize(),
            )
            raise

    async def _send_tp_events(
        self,
        tp_name: str,
        tp_queue: AsyncioQueue,
    ) -> None:
        logger.info(
            'method "_send_tp_events" of tp#{} of {} is starting',
            tp_name,
            self.fullname,
        )

        try:
            while True:
                send_tasks = []
                for i in range(self._config.concurrent_per_partition):
                    if i == 0:
                        # only wait for the first event
                        event = await tp_queue.get()
                    else:
                        try:
                            event = tp_queue.get_nowait()
                        except asyncio.QueueEmpty:
                            break

                    if event.is_subscribed:
                        task = asyncio.create_task(self._sink.send_event(event))
                    else:
                        # If the event is not subscribed, just use a future to represent the process result
                        task = asyncio.get_running_loop().create_future()
                        task.set_result((event, EventProcessStatus.DISCARD))

                    send_tasks.append(task)

                results = await asyncio.gather(*send_tasks)
                for send_result in results:
                    await self._commit_queue.async_q.put(send_result)

        except asyncio.CancelledError:
            logger.info(
                'method "_send_tp_events" of tp#{} of {} is cancelled. tp_queue: {}',
                tp_name,
                self.fullname,
                tp_queue.qsize(),
            )

        except Exception as ex:
            logger.error(
                'method "_send_tp_events" of tp#{} of {} is aborted by <{}> {}. tp_queue: {}',
                tp_name,
                self.fullname,
                type(ex).__name__,
                ex,
                tp_queue.qsize(),
            )
            raise

    @staticmethod
    def _get_topic_partition_str(event: KafkaEvent):
        return event.topic + "_" + str(event.partition)


class KafkaConsumer:
    def __init__(
        self,
        id: str,
        consumer_conf: ConsumerConfig,
        name: Optional[str] = None,
    ):
        self._id = id
        self._name = name or id
        self._check_config(consumer_conf)
        self._config = consumer_conf
        self._kafka_group_id = self._get_group_id()
        self._is_closed = False
        self._internal_consumer: Optional[Consumer] = None
        self._is_fetching_events = False
        self._is_committing_events = False
        self._event_producer = EventProducer(
            f"consumer_{id}", consumer_conf.use_producers
        )
        self._loop: AbstractEventLoop = None

    @property
    def id(self):
        return self._id

    @property
    def name(self):
        return self._name

    @property
    def fullname(self) -> str:
        return f"KafkaConsumer#{self.name}"

    async def init(self) -> None:
        self._loop = asyncio.get_running_loop()

        consumer_kafka_config = self._config.kafka_config.copy()
        consumer_kafka_config["group.id"] = self._kafka_group_id
        if self.name != self.id:
            consumer_kafka_config["group.instance.id"] = self.name
        self._internal_consumer = Consumer(
            consumer_kafka_config,
            logger=logging.getLogger(self.fullname),
        )

        self._internal_consumer.subscribe(
            self._config.kafka_topics,
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
            on_lost=self._on_lost,
        )
        await self._event_producer.init()

    async def close(self) -> None:
        if not self._is_closed:
            try:
                self._is_closed = True
                logger.info("Closing {}", self.fullname)

                while self._is_committing_events:
                    await asyncio.sleep(0.1)

                if self._internal_consumer:
                    self._internal_consumer.close()

                while self._is_fetching_events:
                    await asyncio.sleep(0.1)

                logger.info("Going to close Producer {}", self._event_producer.fullname)

                await self._event_producer.close()

                logger.info("Closed {}", self.fullname)

            except Exception as ex:
                logger.error(
                    "Closing {} failed with error: <{}> {}",
                    self.fullname,
                    type(ex).__name__,
                    ex,
                )

    async def fetch_events(
        self,
        send_queue: JanusQueue[KafkaEvent],
    ):
        logger.info('method "fetch_events" of {} is starting', self.fullname)

        try:
            self._is_fetching_events = True
            await asyncio.get_running_loop().run_in_executor(
                None, self._internal_fetch_events, send_queue
            )
        except ClosedError:
            logger.info('method "fetch_events" of {} is closed', self.fullname)
        except Exception as ex:
            logger.error(
                'method "fetch_events" of {} is aborted by: <{}> {}',
                self.fullname,
                type(ex).__name__,
                ex,
            )
            raise
        finally:
            self._is_fetching_events = False

    async def commit_events(
        self, commit_queue: JanusQueue[Tuple[KafkaEvent, EventProcessStatus]]
    ):
        logger.info('method "commit_events" of {} is starting', self.fullname)

        try:
            self._is_committing_events = True
            await asyncio.get_running_loop().run_in_executor(
                None, self._internal_commit_events, commit_queue
            )
        except ClosedError:
            logger.info('method of "commit_events" of {} is closed', self.fullname)

        except Exception as ex:
            logger.error(
                'method "commit_events" of {} is aborted by: <{}> {}',
                self.fullname,
                type(ex).__name__,
                ex,
            )
            raise
        finally:
            self._is_committing_events = False

    def _internal_fetch_events(self, send_queue: JanusQueue) -> None:
        current_skipped_events = 0

        while self._check_closed():
            try:
                msg: Message = self._internal_consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise ConsumerPollingError(msg.error())
                else:
                    logger.debug(
                        "Get a new Kafka Message from topic: {}-{}, offset: {}",
                        msg.topic(),
                        msg.partition(),
                        msg.offset(),
                    )

                    if msg.offset() > 4000000000:
                        logger.error(
                            "Got a message from Kafka with very high offset {}. From topic: {} and partition: {}",
                            msg.offset(),
                            msg.topic(),
                            msg.partition(),
                        )
            except RuntimeError as ex:
                raise ClosedError(str(ex))
            except Exception as ex:
                logger.error(
                    "Pulling events from Kafka is failed with exception: <{}> {}, will retry",
                    type(ex).__name__,
                    ex,
                )
                # TODO trigger an alert
                continue

            try:
                event: KafkaEvent = parse_kafka_message(msg)
            except Exception as ex:
                logger.error(
                    'It\'s failed when parsing a kafka message (topic: {}, partition: {}, offset: {}, data: "{}") '
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
                continue

            if not self._is_subscribed_event(event):
                if current_skipped_events >= self._config.max_skipped_events:
                    event.is_subscribed = False
                    current_skipped_events = 0
                else:
                    current_skipped_events += 1
                    continue

            while self._check_closed():
                try:
                    send_queue.sync_q.put(event, block=True, timeout=0.2)
                    logger.debug(
                        "A kafka event has been put into send_queue, current queue size is {}",
                        send_queue.sync_q.qsize(),
                    )
                    break
                except janus.SyncQueueFull:
                    pass

    def _internal_commit_events(self, _commit_queue: JanusQueue) -> None:
        # if the consumer is closed and no more pending events, then quit
        while self._check_closed(_commit_queue.sync_q.empty()):
            try:
                event, status = _commit_queue.sync_q.get(block=True, timeout=0.2)
            except janus.SyncQueueEmpty:
                continue

            # ---

            if status == EventProcessStatus.DONE:
                pass

            elif status == EventProcessStatus.DISCARD:
                pass

            elif status == EventProcessStatus.RETRY_LATER:
                current_produce_retries = 0
                while True:
                    try:
                        topic = self._get_retry_topic(event)
                        produce_future = asyncio.run_coroutine_threadsafe(
                            self._event_producer.produce(topic, event), self._loop
                        )
                        produce_result = produce_future.result()
                        break

                    except Exception as ex:
                        if current_produce_retries >= self._config.max_produce_retries:
                            # TODO trigger alerts here or inside producer
                            raise
                        current_produce_retries += 1

            # ---

            current_commit_retries = 0
            while True:
                try:
                    self._internal_consumer.store_offsets(message=event.msg)
                    logger.info(
                        'Consumer group "{}" has stored offsets of event "{}" '
                        "after {} times retries.",
                        self._kafka_group_id,
                        event,
                        current_commit_retries,
                    )
                    break

                except KafkaException as ex:
                    logger.warning(
                        "Storing an event offset failed after {} times retires with exception: <{}> {}",
                        current_commit_retries,
                        type(ex).__name__,
                        ex,
                    )
                    if current_commit_retries >= self._config.max_commit_retries:
                        # TODO need trigger alert here
                        raise

                    current_commit_retries += 1

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

    def _is_subscribed_event(self, event: KafkaEvent) -> bool:
        def match_event_title(patterns) -> bool:
            for p in patterns:
                if re.match(re.compile(p, re.I), event.title):
                    return True
            return False

        if self._config.include_events:
            if not match_event_title(self._config.include_events):
                return False

        if self._config.exclude_events:
            return not match_event_title(self._config.exclude_events)

        return True

    def _check_closed(self, extra_condition: bool = True) -> bool:
        if self._is_closed and extra_condition:
            raise ClosedError

        return True

    def _get_group_id(self):
        return (
            self._config.kafka_config["group.id"]
            if "group.id" in self._config.kafka_config
            else f"event-bus-3-consumer-{config.get().app.project_id}-{config.get().app.env}-{self.id}"
        )

    @staticmethod
    def _get_retry_topic(event: KafkaEvent) -> str:
        return re.sub(r"^(event-v[0-9]-)?", "\\1retry-", event.topic)

    @staticmethod
    def _get_offsets_from_events(*events: KafkaEvent) -> List[TopicPartition]:
        """Note: By convention, committed offsets reflect the next message to be consumed, not the last message consumed.
        Refer: https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.Consumer.commit"""

        return [
            TopicPartition(event.topic, event.partition, event.offset + 1)
            for event in events
        ]

    @staticmethod
    def _check_config(consumer_conf: ConsumerConfig) -> None:
        kafka_config = consumer_conf.kafka_config
        if "bootstrap.servers" not in kafka_config:
            raise InvalidArgumentError('"bootstrap.servers" is needed')
        # if "group.id" not in kafka_config:
        #     raise InvalidArgumentError('"group.id" is needed')
