import asyncio
import re
import time
from asyncio import Queue as AsyncioQueue
from enum import Enum
from typing import Dict, List, Optional, Tuple

import janus
from confluent_kafka.cimpl import Consumer, Message, TopicPartition
from janus import Queue as JanusQueue
from loguru import logger

from eventbus.config import ConsumerConfig
from eventbus.errors import ClosedError, EventConsumingError
from eventbus.event import KafkaEvent, parse_kafka_message
from eventbus.sink import HttpSink, Sink


class ProcessStatus(str, Enum):
    DONE = "done"
    RETRY_LATER = "retry_later"
    DISCARD = "discard"


class ConsumerCoordinator:
    def __init__(self, config: ConsumerConfig, topics: List[str]):
        # TODO check args, kafka_config, group.id, bootstrap.servers, ...
        self._config = config
        self._consumer = KafkaConsumer(config, topics)
        self._sink: Sink = HttpSink(config)

    async def run(self) -> None:
        send_queue: JanusQueue[KafkaEvent] = JanusQueue(maxsize=100)
        commit_queue: JanusQueue[Tuple[KafkaEvent, ProcessStatus]] = JanusQueue(
            maxsize=100
        )

        try:
            await self._consumer.init()
            await self._sink.init()

            done, pending = await asyncio.wait(
                {
                    self._consumer.fetch_events(send_queue),
                    self._wait_and_send_events(send_queue, commit_queue),
                    self._consumer.commit_events(commit_queue),
                }
            )

            logger.warning(
                "ConsumerCoordinator runs over. done: {}, pending: {}", done, pending
            )
        finally:
            await self.cancel()

    async def cancel(self) -> None:
        logger.warning(
            'Cancelling ConsumerCoordinator of Consumer "{}"', self._config.id
        )
        done, pending = await asyncio.wait(
            {self._consumer.close(), self._sink.close()}, timeout=300
        )
        logger.info("ConsumerCoordinator cancelled. done: {}", done)
        if pending:
            logger.warning("ConsumerCoordinator cancelled. pending: {}", pending)

    async def _wait_and_send_events(
        self,
        send_queue: JanusQueue[KafkaEvent],
        commit_queue: JanusQueue[Tuple[KafkaEvent, ProcessStatus]],
    ) -> None:
        tp_queues: Dict[str, AsyncioQueue[KafkaEvent]] = {}
        tp_queue_size = self._config.concurrent_per_partition * 3
        tp_tasks = []

        while True:
            new_event: KafkaEvent = await send_queue.async_q.get()
            tp = self._get_topic_partition_str(new_event)
            if tp in tp_queues:
                await tp_queues[tp].put(new_event)
            else:
                tp_queues[tp] = AsyncioQueue(maxsize=tp_queue_size)
                await tp_queues[tp].put(new_event)
                tp_task = asyncio.create_task(
                    self._send_one_partition_events(tp_queues[tp], commit_queue)
                )
                tp_tasks.append(tp_task)

    async def _send_one_partition_events(
        self,
        tp_queue: AsyncioQueue[KafkaEvent],
        commit_queue: JanusQueue[Tuple[KafkaEvent, ProcessStatus]],
    ):
        while True:
            current_tasks = []
            try:
                for i in range(self._config.concurrent_per_partition):
                    if i == 0:
                        # only wait for the first event
                        new_event = await tp_queue.get()
                    else:
                        new_event = tp_queue.get_nowait()

                    if self._is_listening_event(new_event):
                        sending_task = asyncio.create_task(
                            self._sink.send_event(new_event)
                        )
                    else:
                        # If the event is not in listening events, just use a future to represent the process result
                        sending_task = asyncio.get_event_loop().create_future()
                        sending_task.set_result((new_event, ProcessStatus.DISCARD))

                    current_tasks.append(sending_task)
            except asyncio.QueueEmpty:
                pass

            results = await asyncio.gather(*current_tasks)
            for send_result in results:
                await commit_queue.async_q.put(send_result)

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
    def __init__(self, config: ConsumerConfig, topics: List[str]):
        # TODO check args, kafka_config, group.id, bootstrap.servers, ...
        self._config = config
        self._closed = False
        self._subscribed_topics = topics
        self._internal_consumer: Optional[Consumer] = None

    async def init(self) -> None:
        self._internal_consumer = Consumer(self._config.kafka_config, logger=logger)
        self._internal_consumer.subscribe(
            self.subscribed_topics, on_assign=self._on_assign, on_revoke=self._on_revoke
        )

    async def fetch_events(
        self,
        send_queue: JanusQueue[KafkaEvent],
        commit_queue: JanusQueue[Tuple[KafkaEvent, ProcessStatus]],
    ):
        await asyncio.to_thread(self._internal_fetch_events, send_queue)
        logger.info('The KafkaConsumer "{}" fetch_events quit.', self._config.id)

    async def commit_events(
        self, commit_queue: JanusQueue[Tuple[KafkaEvent, ProcessStatus]]
    ):
        await asyncio.to_thread(self._internal_commit_events, commit_queue)
        logger.info('The KafkaConsumer "{}" commit_events quit.', self._config.id)

    async def close(self) -> None:
        logger.warning('Closing Kafka Consumer "{}"', self._config.id)
        self._closed = True
        await asyncio.sleep(3)  # wait for the threads to complete their jobs
        self._internal_consumer.close()
        self._internal_consumer = None

        # TODO commit events haven't been procssed completely

    def _internal_fetch_events(self, send_queue: JanusQueue) -> None:
        try:
            while not self._closed:
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

                        while not self._closed:
                            try:
                                send_queue.sync_q.put(event, block=True, timeout=0.2)
                                logger.debug(
                                    "A kafka event has been put into send_queue, current queue size is {}",
                                    send_queue.sync_q.qsize(),
                                )
                            except janus.SyncQueueFull:
                                logger.debug("The send_queue is full, will retry")

                except Exception as ex:
                    logger.error(
                        "Pull events from Kafka failed with exception: {}, will retry",
                        type(ex),
                    )
                    time.sleep(0.1)

        except KeyboardInterrupt:
            logger.warning(
                'The Kafka consumer _internal_send_events "{}" aborted by user',
                self._config.id,
            )

    def _internal_commit_events(self, commit_queue: JanusQueue) -> None:
        try:
            while self._check_if_close():
                event: KafkaEvent = None
                status: ProcessStatus = None

                while self._check_if_close():
                    try:
                        event, status = commit_queue.sync_q.get(block=True, timeout=0.2)
                    except janus.SyncQueueEmpty:
                        pass

                try:
                    self._internal_consumer.commit(
                        offsets=self._get_topic_partitions(event)
                    )

                    if status == ProcessStatus.DONE:
                        pass

                    elif status == ProcessStatus.RETRY_LATER:
                        # TODO send to another topic
                        pass

                    elif status == ProcessStatus.DISCARD:
                        pass
                except Exception as ex:
                    logger.error(
                        "Pull events from Kafka failed with exception: {}, will retry",
                        type(ex),
                    )
                    time.sleep(0.1)

        except (KeyboardInterrupt, ClosedError):
            logger.warning(
                'The Kafka consumer _internal_commit_events "{}" aborted by user',
                self._config.id,
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

    def _check_if_close(self) -> bool:
        if self._closed:
            raise ClosedError

        return not self._closed

    @staticmethod
    def _get_topic_partitions(self, *events: KafkaEvent) -> List[TopicPartition]:
        return [
            TopicPartition(event.topic, event.partition, event.offset)
            for event in events
        ]
