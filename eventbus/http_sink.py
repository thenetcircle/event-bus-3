import asyncio
from datetime import datetime
from enum import Enum
from typing import Dict, Optional, Tuple

import aiohttp
from aiohttp import ClientSession
from loguru import logger
from pydantic import StrictStr

from eventbus.event import EventStatus, KafkaEvent
from eventbus.metrics import stats_client
from eventbus.model import AbsSink, EventBusBaseModel


class HttpSinkMethod(str, Enum):
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"


class HttpSinkParams(EventBusBaseModel):
    url: StrictStr
    method: HttpSinkMethod = HttpSinkMethod.POST
    headers: Optional[Dict[str, str]] = None
    timeout: float = 300  # seconds
    max_retry_times: int = 3
    backoff_retry_step: float = 0.1
    backoff_retry_max_time: float = 60.0


class HttpSink(AbsSink):
    def __init__(self, sink_params: HttpSinkParams):
        self._params = sink_params
        self._client: Optional[ClientSession] = None

        self._max_retry_times = self._params.max_retry_times
        self._timeout = aiohttp.ClientTimeout(total=self._params.timeout)

    async def init(self):
        logger.info("Initing HttpSink")
        self._client = ClientSession()
        logger.info("Inited HttpSink")

    async def send_event(self, event: KafkaEvent) -> Tuple[KafkaEvent, EventStatus]:
        retry_times = 0
        req_func = getattr(self._client, self._params.method.lower())
        req_url = self._params.url
        req_kwargs = {
            "data": event.payload,
            "timeout": self._timeout,
        }
        if self._params.headers:
            req_kwargs["headers"] = self._params.headers

        stats_client.incr("consumer.event.send.new")

        while True:
            start_time = datetime.now()
            try:
                async with req_func(req_url, **req_kwargs) as resp:
                    _cost_time = self._get_cost_time(start_time)
                    stats_client.timing(
                        "consumer.event.send.time", int(_cost_time * 1000)
                    )

                    if resp.status == 200:
                        resp_body = await resp.text()
                        if resp_body == "ok":
                            logger.info(
                                'Sending an event "{}" to "{}" succeeded in {} seconds after {} times retires',
                                event,
                                req_url,
                                _cost_time,
                                retry_times,
                            )
                            stats_client.incr("consumer.event.send.done")
                            return event, EventStatus.DONE

                        elif resp_body == "retry":
                            # retry logic
                            if retry_times >= self._max_retry_times:
                                logger.info(
                                    'Sending an event "{}" to "{}" exceeded max retry times {} in {} seconds',
                                    event,
                                    req_url,
                                    retry_times,
                                    _cost_time,
                                )
                                stats_client.incr("consumer.event.send.retry")
                                return event, EventStatus.DEAD_LETTER
                            else:
                                retry_times += 1
                                continue

                        else:
                            # unexpected resp
                            logger.warning(
                                'Sending an event "{}" to "{}" failed in {} seconds because of unexpected response: {}',
                                event,
                                req_url,
                                _cost_time,
                                resp_body,
                            )
                            stats_client.incr("consumer.event.send.retry")
                            return event, EventStatus.DEAD_LETTER

                    else:
                        logger.warning(
                            'Sending an event "{}" to "{}" failed in {} seconds because of non-200 status code: {}',
                            event,
                            req_url,
                            _cost_time,
                            resp.status,
                        )

                        # non-200 status code, use retry logic
                        if retry_times >= self._max_retry_times:
                            logger.info(
                                'Sending an event "{}" to "{}" exceeded max retry times {} in {} seconds',
                                event,
                                req_url,
                                retry_times,
                                _cost_time,
                            )
                            stats_client.incr("consumer.event.send.retry")
                            return event, EventStatus.DEAD_LETTER
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
                sleep_time = self._get_backoff_sleep_time(retry_times)
                logger.error(
                    'Sending an event "{}" to "{}" failed in {} seconds after {} retries '
                    'because of "{}", details: {}, '
                    "will retry after {} seconds",
                    event,
                    req_url,
                    self._get_cost_time(start_time),
                    retry_times,
                    type(ex),
                    ex,
                    sleep_time,
                )
                # TODO trigger alert
                # keep retry
                await asyncio.sleep(sleep_time)

            except (
                aiohttp.ClientResponseError,  # this is mostly the response related error
                aiohttp.ClientPayloadError,  # this is response data error
            ) as ex:
                # since it's response related errors, it could be recovered later by improving the target
                # at least we shouldn't block other subsequence events
                # so just return retry_later
                logger.error(
                    'Sending an event "{}" to "{}" failed in {} seconds after {} retries '
                    'because of "{}", details: {}',
                    event,
                    req_url,
                    self._get_cost_time(start_time),
                    retry_times,
                    type(ex),
                    ex,
                )
                # TODO trigger alert
                stats_client.incr("consumer.event.send.retry")
                return event, EventStatus.DEAD_LETTER

            except Exception as ex:
                sleep_time = self._get_backoff_sleep_time(retry_times)
                logger.error(
                    'Sending an event "{}" to "{}" failed in {} seconds after {} retries '
                    'because of a unknown exception "{}", details : {}, '
                    "will retry after {} seconds",
                    event,
                    req_url,
                    self._get_cost_time(start_time),
                    retry_times,
                    type(ex),
                    ex,
                    sleep_time,
                )
                # TODO trigger alert
                # keep retry until fixed
                await asyncio.sleep(sleep_time)

            retry_times += 1

    async def close(self):
        if self._client:
            logger.info("Closing HttpSink")

            await self._client.close()
            self._client = None

            logger.info("Closed HttpSink")

    def _get_backoff_sleep_time(self, retry_times: int) -> float:
        return min(
            [
                (retry_times + 1) * self._params.backoff_retry_step,
                self._params.backoff_retry_max_time,
            ]
        )

    @staticmethod
    def _get_cost_time(start_time: datetime) -> float:
        return ((datetime.now()) - start_time).total_seconds()
