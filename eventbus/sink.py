import asyncio
from asyncio import Queue
from typing import Dict

import aiohttp
import janus
from loguru import logger

from eventbus.config import ConsumerInstanceConfig
from eventbus.event import KafkaEvent


class Sink:
    pass


class HttpSink(Sink):
    def __init__(self, consumer_conf: ConsumerInstanceConfig):
        self._consumer_conf = consumer_conf
        self._max_buffer_size = self._consumer_conf.sink.buffer_size or 3 * 3 * 3

    async def wait_and_deliver_events(
        self,
        send_queue: janus.AsyncQueue,
        commit_queue: janus.AsyncQueue,
    ) -> None:
        tp_queues: Dict[str, Queue] = {}
        tp_queue_size = self._consumer_conf.concurrent_per_partition * 3
        tp_consuming_tasks = []
        async with aiohttp.ClientSession() as aiohttp_session:
            while True:
                new_event: KafkaEvent = await send_queue.get()
                tp = self.get_tp_from_event(new_event)
                if tp in tp_queues:
                    await tp_queues[tp].put(new_event)
                else:
                    tp_queues[tp] = Queue(maxsize=tp_queue_size)
                    consuming_task = asyncio.create_task(
                        self.consume_tp_queue(aiohttp_session, tp_queues[tp])
                    )
                    tp_consuming_tasks.append(consuming_task)

    async def consume_tp_queue(
        self, aiohttp_session: aiohttp.ClientSession, tp_queue: Queue
    ):
        while True:
            concurrent_sending_tasks = []
            for i in range(self._consumer_conf.concurrent_per_partition):
                new_event = await tp_queue.get()
                sending_task = asyncio.create_task(
                    self._send_one_event(aiohttp_session, new_event)
                )
                concurrent_sending_tasks.append(sending_task)

            results = await asyncio.gather(*concurrent_sending_tasks)
            # TODO process the results

    async def _send_one_event(
        self, aiohttp_session: aiohttp.ClientSession, kafka_event: KafkaEvent
    ):
        try:
            req_func = getattr(aiohttp_session, self._consumer_conf.sink.method.lower())
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

    @staticmethod
    def get_tp_from_event(self, event: KafkaEvent):
        return event.kafka_msg.topic() + "_" + str(event.kafka_msg.partition())

    # async def _fill_buffer_from_queue(
    #     self, send_queue: janus.AsyncQueue, buffer: Dict[str, List[KafkaEvent]]
    # ) -> None:
    #     buffer_size = sum([len(v) for k, v in buffer.items()])
    #     # filling the buffer
    #     while True:
    #         try:
    #             if buffer_size >= self._max_buffer_size:
    #                 break
    #
    #             _kafka_event = await send_queue.get()
    #
    #             _tp = (
    #                 _kafka_event.kafka_msg.topic()
    #                 + "_"
    #                 + str(_kafka_event.kafka_msg.partition())
    #             )
    #             if _tp in buffer:
    #                 buffer[_tp].append(_kafka_event)
    #             else:
    #                 buffer[_tp] = [_kafka_event]
    #             buffer_size += 1
    #
    #         except queue.Empty:
    #             if buffer_size == 0:
    #                 logger.debug("The kafka_events buffer is empty, continue")
    #                 await asyncio.sleep(0.1)
    #             else:
    #                 # if the buffer is not empty, but the queue is empty, then break the loop
    #                 break

    # async def _send_buffer_events(
    #     self,
    #     session: aiohttp.ClientSession,
    #     buffer: Dict[str, List[KafkaEvent]],
    #     commit_queue: janus.AsyncQueue,
    # ) -> None:
    #     tasks = set()
    #     # sending the events from the buffer
    #     for buffer_key, kafka_events in buffer.items():
    #         for i in range(self._consumer_conf.concurrent_per_partition):
    #             if len(kafka_events) == 0:
    #                 break
    #             kafka_event = kafka_events.pop(0)
    #             tasks.add(self._send_one_event(session, kafka_event))
    #
    #     done, pending = await asyncio.wait(tasks)
