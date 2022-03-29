import asyncio
import functools
import os
import signal
import sys
from asyncio import AbstractEventLoop
from multiprocessing import Process
from time import sleep, time
from typing import List

from loguru import logger

from eventbus import config, config_watcher
from eventbus.config_watcher import watch_config_file
from eventbus.consumer import EventConsumer
from eventbus.errors import EventConsumerNotFoundError


def setup_logger():
    logger.remove()
    logger.add(
        sys.stdout,
        level="INFO",
        format="{thread} "
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> "
        "| <level>{level: <8}</level> "
        "| <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> "
        "- <level>{message}</level>",
    )


def consumer_main(consumer_id: str, config_file_path: str):
    setup_logger()

    config.update_from_yaml(config_file_path)

    if consumer_id not in config.get().consumers:
        logger.error('Consumer id "{}" can not be found from the configs', consumer_id)
        # TODO trigger alert
        raise EventConsumerNotFoundError

    consumer_conf = config.get().consumers[consumer_id]
    consumer = EventConsumer(consumer_id, consumer_conf)

    # run consumer
    asyncio.run(consumer_run(consumer))


async def consumer_run(consumer: EventConsumer):
    loop = asyncio.get_event_loop()

    # add signals handlers
    term_callback = functools.partial(consumer_term_callback, consumer, loop)
    loop.add_signal_handler(signal.SIGTERM, term_callback)
    loop.add_signal_handler(signal.SIGINT, term_callback)
    loop.add_signal_handler(
        signal.SIGUSR1,
        functools.partial(consumer_update_config_callback, consumer),
    )

    await consumer.init()
    await consumer.run()


def consumer_term_callback(
    consumer: EventConsumer,
    loop: AbstractEventLoop,
):
    logger.info("Get TERM signals, going to terminate the consumer {}.", consumer.id)
    asyncio.run_coroutine_threadsafe(consumer.cancel(), loop)


def consumer_update_config_callback(consumer: EventConsumer):
    logger.info(
        "Get Config Updated signals, going to reload the config for consumer {}.",
        consumer.id,
    )
    config.reload()
    if config.get_last():
        config_watcher.send_signals(config.get_last(), config.get())


def main():
    setup_logger()

    args = sys.argv[1:]
    if len(args) > 0:
        config.load_from_args(args)
    else:
        config.load_from_environ()

    grace_term_period = 10
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    consumer_procs: List[Process] = []
    for consumer_id, _ in config.get().consumers.items():
        p = Process(
            target=consumer_main,
            name=f"Consumer#{consumer_id}",
            args=(consumer_id, str(config.get().config_file_path.resolve())),
            daemon=True,
        )
        p.start()
        consumer_procs.append(p)

    watch_config_file(config.get().config_file_path, checking_interval=10)
    local_config_last_update_time = config.get_last_update_time()

    def signal_handler(signalname):
        def f(signal_received, frame):
            raise KeyboardInterrupt(f"{signalname} received")

        return f

    signal.signal(signal.SIGTERM, signal_handler("SIGTERM"))
    signal.signal(signal.SIGINT, signal_handler("SIGINT"))

    def get_alive_procs() -> List[Process]:
        return [p for p in consumer_procs if p.is_alive()]

    try:
        while alive_procs := get_alive_procs():
            # check if config get changed
            if config.get_last_update_time() > local_config_last_update_time:
                for p in alive_procs:
                    logger.warning("Sending SIGUSR1 to {}", p)
                    os.kill(p.pid, signal.SIGUSR1)
                local_config_last_update_time = config.get_last_update_time()

            sleep(0.1)

    except KeyboardInterrupt:
        logger.warning("Caught KeyboardInterrupt! Stopping consumers...")

    finally:
        for p in get_alive_procs():
            logger.warning("Sending SIGTERM to {}", p)
            os.kill(p.pid, signal.SIGTERM)

        t = time()
        while alive_procs := get_alive_procs():
            if time() > t + grace_term_period:
                for p in alive_procs:
                    logger.warning("Sending SIGKILL to {}", p)
                    p.kill()
            sleep(0.01)

    logger.warning("Main process quit.")


if __name__ == "__main__":
    main()
