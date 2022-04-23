import asyncio
import os
import signal
from multiprocessing import Process
from time import sleep, time
from typing import Dict, List, Set

from loguru import logger

from eventbus import config
from eventbus.config_watcher import watch_config_file
from eventbus.consumer import EventConsumer
from eventbus.errors import EventConsumerNotFoundError
from eventbus.utils import setup_logger


def consumer_main(consumer_id: str, config_file_path: str):
    config.update_from_yaml(config_file_path)
    setup_logger()

    if consumer_id not in config.get().consumers:
        logger.error('Consumer id "{}" can not be found from the configs', consumer_id)
        # TODO trigger alert
        raise EventConsumerNotFoundError

    consumer_conf = config.get().consumers[consumer_id]
    consumer = EventConsumer(consumer_id, consumer_conf)

    # run consumer
    asyncio.run(run_consumer(consumer))


async def run_consumer(consumer: EventConsumer):
    loop = asyncio.get_event_loop()

    def term_callback():
        logger.info(
            "Get TERM signals, going to terminate the consumer {}.", consumer.id
        )
        asyncio.run_coroutine_threadsafe(consumer.cancel(), loop)

    def update_config_callback():
        logger.info(
            "Get Config Updated signals, going to reload the config for consumer {}.",
            consumer.id,
        )
        config.reload()
        config.send_signals()

    # add signals handlers
    loop.add_signal_handler(signal.SIGTERM, term_callback)
    loop.add_signal_handler(signal.SIGINT, term_callback)
    loop.add_signal_handler(signal.SIGUSR1, update_config_callback)

    await consumer.init()
    await consumer.run()


def main():
    import argparse

    parser = argparse.ArgumentParser(description="EventBus 3 Consumer")
    parser.add_argument(
        "-c",
        "--config_file",
        type=str,
        help="Config file path. If not specified, it will look for environment variable `EVENTBUS_CONFIG`",
    )
    args = parser.parse_args()

    if args.config_file:
        config.update_from_yaml(args.config_file)
    else:
        config.load_from_environ()

    setup_logger()

    # --- handler system signals ---

    grace_term_period = 10
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    consumer_procs: Dict[str, Process] = {}

    def start_new_consumer(consumer_id):
        if consumer_id in consumer_procs and consumer_procs[consumer_id].is_alive():
            # TODO trigger alert on all errors
            logger.error(
                "Consumer#{} already in consumer_procs and is alive, something wrong happened",
                consumer_id,
            )
            return

        logger.info("Starting new consumer {}", consumer_id)
        p = Process(
            target=consumer_main,
            name=f"Consumer#{consumer_id}",
            args=(consumer_id, config.get().config_file_path),
            daemon=True,
        )
        p.start()
        consumer_procs[consumer_id] = p

    for consumer_id, _ in config.get().consumers.items():
        start_new_consumer(consumer_id)

    def signal_handler(signalname):
        def f(signal_received, frame):
            raise KeyboardInterrupt(f"{signalname} received")

        return f

    signal.signal(signal.SIGTERM, signal_handler("SIGTERM"))
    signal.signal(signal.SIGINT, signal_handler("SIGINT"))

    # --- handler config change signals ---

    def handle_producer_config_change_signal(
        sender, added: Set[str], removed: Set[str], changed: Set[str]
    ):
        if changed:
            for _, proc in consumer_procs.items():
                if proc.is_alive():
                    logger.warning("Sending SIGUSR1 to {}", proc)
                    os.kill(proc.pid, signal.SIGUSR1)

    def handle_consumer_config_change_signal(
        sender, added: Set[str], removed: Set[str], changed: Set[str]
    ):
        for new_cid in added:
            start_new_consumer(new_cid)

        removed_cids = removed.intersection(list(consumer_procs.keys()))
        for cid in removed_cids:
            p = consumer_procs[cid]
            if p.is_alive():
                logger.warning("Sending SIGTERM to {}", p)
                os.kill(p.pid, signal.SIGTERM)

        changed_cids = changed.intersection(list(consumer_procs.keys()))
        for cid in changed_cids:
            p = consumer_procs[cid]
            if p.is_alive():
                logger.warning("Sending SIGTERM to {}", p)
                os.kill(p.pid, signal.SIGTERM)

                t = time()
                while p.is_alive():
                    if time() > t + grace_term_period:
                        logger.warning("Sending SIGKILL to {}", p)
                        p.kill()
                    sleep(0.01)

            # waiting for all other consumers to be terminated before start new one
            sleep(15)
            start_new_consumer(cid)

    config.ConfigSignals.PRODUCER_CHANGE.connect(handle_producer_config_change_signal)
    config.ConfigSignals.CONSUMER_CHANGE.connect(handle_consumer_config_change_signal)

    # --- monitor config change and sub-processes ---

    def get_alive_procs() -> List[Process]:
        return [proc for proc in list(consumer_procs.values()) if proc.is_alive()]

    local_config_last_update_time = config.get().last_update_time
    watch_config_file(config.get().config_file_path, checking_interval=5)

    try:
        while alive_procs := get_alive_procs():
            if (
                config.get().last_update_time > local_config_last_update_time
            ):  # if config get updated by another thread
                config.send_signals()
                local_config_last_update_time = config.get().last_update_time

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
