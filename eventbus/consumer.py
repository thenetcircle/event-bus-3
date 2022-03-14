import asyncio
import re
import time
from asyncio import Queue as AsyncioQueue
from typing import Dict, List, Optional, Tuple

import janus
from confluent_kafka.cimpl import Consumer, Message, TopicPartition
from janus import Queue as JanusQueue
from loguru import logger

from eventbus.config import ConsumerConfig
from eventbus.errors import ClosedError, EventConsumingError, InvalidArgumentError
from eventbus.event import EventProcessStatus, KafkaEvent, parse_kafka_message
from eventbus.sink import HttpSink, Sink


class ConsumerCoordinator:
    def __init__(self, config: ConsumerConfig):
        self._config = config
        self._consumer = KafkaConsumer(config)
        self._sink: Sink = HttpSink(config)
        self._running = False
        self._wait_task = None
        self._tp_tasks = []
        self._send_queue: JanusQueue[KafkaEvent] = None
        self._commit_queue: JanusQueue[Tuple[KafkaEvent, EventProcessStatus]] = None

    async def init(self) -> None:
        await self._consumer.init()
        await self._sink.init()
        self._send_queue = JanusQueue(maxsize=100)
        self._commit_queue = JanusQueue(maxsize=100)

    async def run(
        self,
    ) -> None:
        start_time = time.time()
        self._running = True

        try:
            self._wait_task = asyncio.create_task(self._wait_events())

            await asyncio.gather(
                self._consumer.fetch_events(self._send_queue),
                self._wait_task,
                self._consumer.commit_events(self._commit_queue),
            )

            logger.warning(
                "ConsumerCoordinator runs over after {} seconds",
                time.time() - start_time,
            )
        finally:
            await self.cancel()

    async def cancel(self) -> None:
        if self._running:
            logger.warning('Cancelling ConsumerCoordinator of "{}"', self._config.id)
            self._running = False
            self._wait_task.cancel()
            for t in self._tp_tasks:
                t.cancel()
            await asyncio.gather(self._consumer.close(), self._sink.close())

            logger.info(
                'ConsumerCoordinator of Consumer "{}" has been cancelled, current queue stats: send_queue: {}, commit_queue: {}',
                self._config.id,
                self._send_queue.async_q.qsize(),
                self._commit_queue.async_q.qsize(),
            )

    async def _wait_events(
        self,
    ) -> None:
        self._tp_tasks = []
        tp_queues: Dict[str, AsyncioQueue] = {}
        tp_queue_size = self._config.concurrent_per_partition * 3

        while self._running:
            event: KafkaEvent = await self._send_queue.async_q.get()

            tp_name = self._get_topic_partition_str(event)
            if tp_name not in tp_queues:
                tp_queues[tp_name] = AsyncioQueue(maxsize=tp_queue_size)
                tp_task = asyncio.create_task(
                    self._send_tp_events(tp_name, tp_queues[tp_name])
                )
                self._tp_tasks.append(tp_task)

            await tp_queues[tp_name].put(event)

    async def _send_tp_events(
        self,
        tp_name: str,
        tp_queue: AsyncioQueue,
    ) -> None:
        logger.debug(f'Start consuming tp_queue "{tp_name}"')

        while self._running:
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

                # TODO refactor this to skip bunch of this sort of events from very beginning
                if self._is_listening_event(event):
                    task = asyncio.create_task(self._sink.send_event(event))
                else:
                    # If the event is not in listening events, just use a future to represent the process result
                    task = asyncio.get_running_loop().create_future()
                    task.set_result((event, EventProcessStatus.DISCARD))

                send_tasks.append(task)

            try:
                results = await asyncio.gather(*send_tasks)
                for send_result in results:
                    await self._commit_queue.async_q.put(send_result)
            except asyncio.CancelledError:
                pass

    def _is_listening_event(self, event: KafkaEvent) -> bool:
        listening_event = self._config.listening_events
        if not listening_event:
            return True
        for ev_pattern in listening_event:
            if re.match(re.compile(ev_pattern, re.I), event.title):
                return True
        return False

    @staticmethod
    def _get_topic_partition_str(event: KafkaEvent):
        return event.topic + "_" + str(event.partition)


class KafkaConsumer:
    def __init__(self, config: ConsumerConfig):
        self._check_config(config)
        self._config = config
        self._closed = False
        self._internal_consumer: Optional[Consumer] = None
        self._is_commit_running = False

    async def init(self) -> None:
        self._internal_consumer = Consumer(self._config.kafka_config, logger=logger)
        self._internal_consumer.subscribe(
            self._config.listening_topics,
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
        )

    async def fetch_events(
        self,
        send_queue: JanusQueue[KafkaEvent],
    ):
        await asyncio.get_running_loop().run_in_executor(
            None, self._internal_fetch_events, send_queue
        )

    async def commit_events(
        self, commit_queue: JanusQueue[Tuple[KafkaEvent, EventProcessStatus]]
    ):
        self._is_commit_running = True
        await asyncio.get_running_loop().run_in_executor(
            None, self._internal_commit_events, commit_queue
        )
        self._is_commit_running = False

    async def close(self) -> None:
        if self._internal_consumer:
            logger.warning('Closing KafkaConsumer "{}"', self._config.id)

            self._closed = True

            while (
                self._is_commit_running
            ):  # wait for the pending events to be committed
                await asyncio.sleep(0.1)

            self._internal_consumer.close()
            self._internal_consumer = None

            logger.info('KafkaConsumer "{}" has closed', self._config.id)

    def _internal_fetch_events(self, send_queue: JanusQueue) -> None:
        try:
            while self._check_closed():
                try:
                    msg: Message = self._internal_consumer.poll(timeout=1.0)
                    if msg is None:
                        continue
                    if msg.error():
                        raise EventConsumingError(msg.error())
                    else:
                        logger.debug(
                            "Get a new Kafka Message from topic: {}-{}, offset: {}",
                            msg.topic(),
                            msg.partition(),
                            msg.offset(),
                        )
                except Exception as ex:
                    logger.error(
                        "Pull events from Kafka failed with exception: {}, will retry",
                        type(ex),
                    )
                    time.sleep(0.1)

                try:
                    event: KafkaEvent = parse_kafka_message(msg)
                except Exception as ex:
                    logger.error(
                        'Parse kafka message: "{}" failed with error: "{}"',
                        msg.value(),
                        type(ex),
                    )
                    # TODO trigger error
                    # skip this event if parse failed
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

        except (KeyboardInterrupt, ClosedError) as ex:
            logger.warning(
                '_internal_send_events of KafkaConsumer "{}" is aborted by "{}"',
                self._config.id,
                type(ex),
            )

    def _internal_commit_events(self, _commit_queue: JanusQueue) -> None:
        try:
            commit_queue = _commit_queue.sync_q

            # if the consumer is closed and no more pending events, then quit
            while self._check_closed(commit_queue.empty()):
                try:
                    event, status = commit_queue.get(block=True, timeout=0.2)
                except janus.SyncQueueEmpty:
                    continue

                try:
                    if status == EventProcessStatus.DONE:
                        pass

                    elif status == EventProcessStatus.RETRY_LATER:
                        # TODO send to another topic
                        pass

                    elif status == EventProcessStatus.DISCARD:
                        pass

                    self._internal_consumer.commit(
                        offsets=self._get_topic_partitions(event)
                    )
                except Exception as ex:
                    logger.error(
                        "Pull events from Kafka failed with exception: {}, will retry",
                        type(ex),
                    )
                    time.sleep(0.1)

        except (KeyboardInterrupt, ClosedError) as ex:
            logger.warning(
                '_internal_commit_events of KafkaConsumer "{}" is aborted by "{}"',
                self._config.id,
                type(ex),
            )

    def _on_assign(self, consumer: Consumer, partitions: List[TopicPartition]) -> None:
        logger.info(
            'KafkaConsumer "{}" get assigned new TopicPartitions: "{}"',
            self._config.id,
            partitions,
        )

    def _on_revoke(self, consumer: Consumer, partitions: List[TopicPartition]) -> None:
        logger.info(
            'KafkaConsumer "{}" get revoked TopicPartitions: "{}"',
            self._config.id,
            partitions,
        )

    def _check_closed(self, extra: bool = True) -> bool:
        if self._closed and extra:
            raise ClosedError

        return True

    @staticmethod
    def _get_topic_partitions(*events: KafkaEvent) -> List[TopicPartition]:
        return [
            TopicPartition(event.topic, event.partition, event.offset)
            for event in events
        ]

    @staticmethod
    def _check_config(config: ConsumerConfig) -> None:
        kafka_config = config.kafka_config
        if "bootstrap.servers" not in kafka_config:
            raise InvalidArgumentError('"bootstrap.servers" is needed')
        if "group.id" not in kafka_config:
            raise InvalidArgumentError('"group.id" is needed')

        # enable.auto.commit
        # auto.offset.reset
        # Note that ‘enable.auto.offset.store’ must be set to False when using this API. <- https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.Consumer.store_offsets
