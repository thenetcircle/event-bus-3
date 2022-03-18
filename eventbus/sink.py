import asyncio
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, Tuple

import aiohttp
from aiohttp import ClientSession
from loguru import logger

from eventbus.config import EventConsumerConfig
from eventbus.event import EventProcessStatus, KafkaEvent


class Sink(ABC):
    @abstractmethod
    async def init(self):
        raise NotImplementedError

    @abstractmethod
    async def send_event(
        self, event: KafkaEvent
    ) -> Tuple[KafkaEvent, EventProcessStatus]:
        raise NotImplementedError

    @abstractmethod
    async def close(self):
        raise NotImplementedError


class HttpSink(Sink):
    def __init__(self, consumer_conf: EventConsumerConfig):
        self._config = consumer_conf
        self._client: Optional[ClientSession] = None

        self._max_retry_times = self._config.sink.max_retry_times
        self._timeout = aiohttp.ClientTimeout(total=self._config.sink.timeout)

    async def init(self):
        self._client = ClientSession()

    async def send_event(
        self, event: KafkaEvent
    ) -> Tuple[KafkaEvent, EventProcessStatus]:
        retry_times = 0
        req_func = getattr(self._client, self._config.sink.method.lower())
        req_url = self._config.sink.url
        req_kwargs = {
            "data": event.payload,
            "timeout": self._timeout,
        }
        if self._config.sink.headers:
            req_kwargs["headers"] = self._config.sink.headers

        while True:
            start_time = datetime.now()
            try:
                async with req_func(req_url, **req_kwargs) as resp:
                    if resp.status == 200:
                        resp_body = await resp.text()
                        if resp_body == "ok":
                            logger.info(
                                'That sending an event "{}" to "{}" succeeded in {} seconds after {} times retires',
                                event,
                                req_url,
                                self._get_cost_time(start_time),
                                retry_times,
                            )
                            return event, EventProcessStatus.DONE

                        elif resp_body == "retry":
                            # retry logic
                            if retry_times >= self._max_retry_times:
                                logger.info(
                                    'That sending an event "{}" to "{}" exceeded max retry times {} in {} seconds',
                                    event,
                                    req_url,
                                    retry_times,
                                    self._get_cost_time(start_time),
                                )
                                return event, EventProcessStatus.RETRY_LATER
                            else:
                                retry_times += 1
                                continue

                        else:
                            # unexpected resp
                            logger.warning(
                                'That sending an event "{}" to "{}" failed in {} seconds because of unexpected response: {}',
                                event,
                                req_url,
                                self._get_cost_time(start_time),
                                resp_body,
                            )
                            return event, EventProcessStatus.RETRY_LATER

                    else:
                        logger.warning(
                            'That sending an event "{}" to "{}" failed in {} seconds because of non-200 status code: {}',
                            event,
                            req_url,
                            self._get_cost_time(start_time),
                            resp.status,
                        )

                        # non-200 status code, use retry logic
                        if retry_times >= self._max_retry_times:
                            logger.info(
                                'That sending an event "{}" to "{}" exceeded max retry times {} in {} seconds',
                                event,
                                req_url,
                                retry_times,
                                self._get_cost_time(start_time),
                            )
                            return event, EventProcessStatus.RETRY_LATER
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
                    event,
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
                    event,
                    req_url,
                    self._get_cost_time(start_time),
                    type(ex),
                    ex,
                )
                # TODO trigger alert
                return event, EventProcessStatus.RETRY_LATER

            except Exception as ex:
                logger.error(
                    'That sending an event "{}" to "{}" failed in {} seconds because of a unknown exception "{}", details : {}',
                    event,
                    req_url,
                    self._get_cost_time(start_time),
                    type(ex),
                    ex,
                )
                # TODO trigger alert
                # keep retry until fixed
                await asyncio.sleep(0.1)

            retry_times += 1

    async def close(self):
        if self._client:
            logger.warning('Closing HttpSink "{}"', self._config.id)

            await self._client.close()
            self._client = None

            logger.info('HttpSink "{}" has closed', self._config.id)

    @staticmethod
    def _get_cost_time(start_time: datetime) -> float:
        return ((datetime.now()) - start_time).total_seconds()
