#!/usr/bin/env python

import argparse
import asyncio
import json
import statistics
import time
from datetime import datetime
from typing import Any, Dict

import aiohttp
import uvicorn
from loguru import logger
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Route


async def startup():
    logger.info("The app is starting up")


async def shutdown():
    logger.info("The app is shutting down")


msgs: Dict[int, Dict[str, Any]] = {}
dateformat = "%Y-%m-%dT%H:%M:%S.%f"


async def start(request):
    eb_url = request.query_params["eb_url"]
    concurrent_size = (
        int(request.query_params["cc_size"])
        if "concurrent_size" in request.query_params
        else 30
    )
    req_num = (
        int(request.query_params["req_num"])
        if "req_num" in request.query_params
        else 100
    )

    async def send_one_req(session: aiohttp.ClientSession, data: Any, id: int):
        start_time = time.time()
        async with session.post(eb_url, data=data) as resp:
            msgs[id]["ct"] = time.time() - start_time
            logger.info("Resp: {} - {}", resp.status, await resp.text())

    async with aiohttp.ClientSession() as session:
        tasks = []
        for i in range(100, req_num + 100):
            pb = datetime.now()
            new_msg = {
                "id": i,
                "title": f"stress_test.event_{i}",
                "published": pb.strftime(dateformat),
            }
            msgs[i] = {"pb": pb}
            data = json.dumps(new_msg)
            tasks.append(asyncio.create_task(send_one_req(session, data, i)))

            if len(tasks) >= concurrent_size:
                await asyncio.gather(*tasks)
                tasks = []

        if tasks:
            await asyncio.gather(*tasks)

    stats = [m["ct"] for _, m in msgs.items()]
    return PlainTextResponse(
        f"Total: {len(msgs)}\n Max: {max(stats)}\n Median: {statistics.median(stats)}\n Mean: {statistics.mean(stats)}\n Min: {min(stats)}"
    )


async def receive_events(request):
    request_body = await request.body()
    data = json.loads(request_body)
    print(json.dumps(data, indent=4))
    # if isinstance(data, dict):
    #     id = int(data["id"])
    #     # new_pb = datetime.strptime(data["published"], dateformat)
    #     new_pb = datetime.now()
    #     msgs[id]["rt"] = (new_pb - msgs[id]["pb"]).microseconds * pow(10, -6)
    return PlainTextResponse("ok")


async def status(request):

    ct_stats = [m["ct"] for _, m in msgs.items() if "ct" in m]
    rt_stats = [m["rt"] for _, m in msgs.items() if "rt" in m]

    status = f"""
Request Count: {len(ct_stats)}
Request Time:
- Max: {max(ct_stats)}
- Median: {statistics.median(ct_stats)}
- Mean: {statistics.mean(ct_stats)}
- Min: {min(ct_stats)}
Received Count: {len(rt_stats)}
Received Time:
- Max: {max(rt_stats)}
- Median: {statistics.median(rt_stats)}
- Mean: {statistics.mean(rt_stats)}
- Min: {min(rt_stats)}
"""

    return PlainTextResponse(status)


def home(request):
    return PlainTextResponse("ok")


async def reset(request):
    global msgs
    msgs = {}
    return PlainTextResponse("done")


routes = [
    Route("/", home),
    Route("/start", start),
    Route("/receive_events", receive_events, methods=["POST"]),
    Route("/status", status),
    Route("/reset", reset),
]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EventBus 3 stress test script")
    parser.add_argument("-p", "--port", type=int, help="Listening port", default=8081)
    args = parser.parse_args()

    app = Starlette(
        debug=False,
        routes=routes,
        on_startup=[startup],
        on_shutdown=[shutdown],
    )
    logger.info(
        "The app is starting, you must set callback address to {} with POST, then call {} with GET",
        f"http://hostname:{args.port}/receive_events",
        f"http://hostname:{args.port}/start",
    )
    uvicorn.run(app, host="0.0.0.0", port=args.port)
