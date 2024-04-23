import asyncio
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional, Tuple

import aiohttp
from aiohttp import ClientSession
from loguru import logger
from pydantic import StrictStr

from eventbus.event import Event, EventStatus
from eventbus.metrics import stats_client
from eventbus.model import AbsSink, EventBusBaseModel, SinkResult, SinkType


class HttpSinkMethod(str, Enum):
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"


class HttpSinkParams(EventBusBaseModel):
    url: StrictStr
    method: HttpSinkMethod = HttpSinkMethod.POST
    headers: Optional[Dict[str, str]] = None
    timeout: float = 300  # seconds, the timeout of each request or each retry
    max_retry_times: int = 3
    exp_backoff_factor: float = 0.1
    exp_backoff_max_delay: float = 60.0


class HttpSink(AbsSink):
    def __init__(self, sink_params: HttpSinkParams):
        self._client: Optional[ClientSession] = None
        self.update_params(sink_params)

    async def init(self):
        logger.info("Initializing HttpSink")
        self._client = ClientSession()
        logger.info("HttpSink has been initialized")

    def get_sink_type(self) -> SinkType:
        return SinkType.HTTP

    def update_params(self, sink_params: HttpSinkParams):
        logger.info("Updating HttpSink params: {}", sink_params)
        self._params = sink_params
        self._max_retry_times = self._params.max_retry_times
        self._timeout = aiohttp.ClientTimeout(total=self._params.timeout)

    async def send_event(self, event: Event) -> SinkResult:
        send_times = 0
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
            send_times += 1
            start_time = datetime.now()
            try:
                async with req_func(req_url, **req_kwargs) as resp:
                    cost_time = self._get_cost_time(start_time)
                    stats_client.timing(
                        "consumer.event.send.time", int(cost_time * 1000)
                    )

                    with logger.contextualize(
                        event=event,
                        req_url=req_url,
                        cost_time=cost_time,
                        send_times=send_times,
                    ):
                        if resp.status == 200:
                            resp_body = await resp.text()
                            resp_body_lower = resp_body.lower()

                            if resp_body_lower == "ok":
                                logger.info(
                                    "Event send successfully",
                                )
                                stats_client.incr("consumer.event.send.done")
                                return SinkResult(event, EventStatus.DONE)

                            elif resp_body_lower == "retry":
                                if send_times > self._max_retry_times:
                                    logger.info(
                                        "Event send failed with retry response, and exceed the max retry times: {} , will send to dead letter queue.",
                                        self._max_retry_times,
                                    )
                                    stats_client.incr("consumer.event.send.dead_letter")
                                    return SinkResult(
                                        event,
                                        EventStatus.DEAD_LETTER,
                                        Exception("Maximum Retries"),
                                    )
                                else:
                                    stats_client.incr("consumer.event.send.retry")
                                    logger.info(
                                        "Event send failed with retry response, retry now",
                                    )
                                    continue

                            elif resp_body_lower == "exponential_backoff_retry":
                                if send_times > self._max_retry_times:
                                    stats_client.incr("consumer.event.send.dead_letter")
                                    logger.info(
                                        "Event send failed with exponential_backoff_retry response, and exceeded the max retry times: {}, will send to dead letter queue.",
                                        self._max_retry_times,
                                    )
                                    return SinkResult(
                                        event,
                                        EventStatus.DEAD_LETTER,
                                        Exception("Maximum Retries"),
                                    )
                                else:
                                    stats_client.incr("consumer.event.send.retry")
                                    sleep_time = self._get_exp_backoff_delay(send_times)
                                    logger.info(
                                        "Event send failed with exponential_backoff_retry response, will retry after {} seconds",
                                        sleep_time,
                                    )
                                    await asyncio.sleep(sleep_time)
                                    continue

                            else:  # unexpected resp
                                logger.warning(
                                    "Event send failed with unexpected response: {}, will send to dead letter queue.",
                                    resp_body,
                                )
                                stats_client.incr("consumer.event.send.dead_letter")
                                return SinkResult(
                                    event,
                                    EventStatus.DEAD_LETTER,
                                    Exception(f"Unexpected Response: {resp_body}"),
                                )

                        else:  # unexpected status code
                            logger.warning(
                                "Event send failed with non-200 status code: {}, will send to dead letter queue.",
                                resp.status,
                            )
                            stats_client.incr("consumer.event.send.dead_letter")
                            return SinkResult(
                                event,
                                EventStatus.DEAD_LETTER,
                                Exception(f"Non-200 Status Code: {resp.status}"),
                            )

            # more details of aiohttp errors can be found here:
            # https://docs.aiohttp.org/en/stable/client_reference.html#aiohttp.ClientPayloadError
            except (
                aiohttp.ClientConnectionError,
                aiohttp.InvalidURL,
                asyncio.exceptions.TimeoutError,
            ) as ex:
                sleep_time = self._get_exp_backoff_delay(send_times)
                logger.bind(
                    event=event,
                    req_url=req_url,
                    cost_time=self._get_cost_time(start_time),
                    send_times=send_times,
                ).exception(
                    "Event send failed, will retry after {} seconds", sleep_time
                )
                stats_client.incr("consumer.event.send.retry")
                await asyncio.sleep(sleep_time)

            except (
                aiohttp.ClientResponseError,  # this is mostly the response related error
                aiohttp.ClientPayloadError,  # this is response data error
            ) as ex:
                # since it's response related errors, it could be recovered later by improving the target
                # at least we shouldn't block other subsequence events
                # so just return retry_later
                logger.bind(
                    event=event,
                    req_url=req_url,
                    cost_time=self._get_cost_time(start_time),
                    send_times=send_times,
                ).exception("Event send failed, will send to dead letter queue.")
                stats_client.incr("consumer.event.send.dead_letter")
                return SinkResult(event, EventStatus.DEAD_LETTER, ex)

            except Exception as ex:
                sleep_time = self._get_exp_backoff_delay(send_times)
                logger.bind(
                    event=event,
                    req_url=req_url,
                    cost_time=self._get_cost_time(start_time),
                    send_times=send_times,
                ).exception(
                    "Event send failed, will retry after {} seconds", sleep_time
                )
                stats_client.incr("consumer.event.send.retry")
                # keep retry until fixed
                await asyncio.sleep(sleep_time)

    async def close(self):
        if self._client:
            logger.info("Closing HttpSink")

            await self._client.close()
            self._client = None

            logger.info("HttpSink has been closed")

    def _get_exp_backoff_delay(self, send_times: int) -> float:
        return min(
            2 ** (send_times - 1) * self._params.exp_backoff_factor,
            self._params.exp_backoff_max_delay,
        )
        # return min(
        #     [
        #         send_times * self._params.backoff_retry_step,
        #         self._params.backoff_retry_max_time,
        #     ]
        # )

    def _get_cost_time(self, start_time: datetime) -> float:
        return ((datetime.now()) - start_time).total_seconds()
