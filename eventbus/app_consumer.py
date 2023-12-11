import asyncio
import os
import signal
import socket
from multiprocessing import Process
from time import sleep, time
from typing import Dict, List, Set

from loguru import logger

from eventbus import config
from eventbus.config_watcher import watch_config_file
from eventbus.errors import StoryDisabledError
from eventbus.metrics import stats_client
from eventbus.model import StoryParams, StoryStatus
from eventbus.story import Story
from eventbus.utils import setup_logger
from eventbus.zoo_client import ZooClient


def story_main(config_file_path: str, story_params: StoryParams):
    config.update_from_yaml(config_file_path)
    setup_logger()
    stats_client.init(config.get())

    if story_params.status == StoryStatus.DISABLED:
        logger.error(
            'Consumer "{}" can not be run, because it is already disabled, ',
            story_params.id,
        )
        # TODO trigger alert
        raise StoryDisabledError

    # run consumer
    asyncio.run(run_story(story_params))


async def run_story(story_params: StoryParams):
    loop = asyncio.get_event_loop()
    story = Story(story_params)

    def term_callback():
        logger.info("Get TERM signals, going to terminate {}.", story.fullname)
        asyncio.run_coroutine_threadsafe(story.close(), loop)

    # add signals handlers
    loop.add_signal_handler(signal.SIGTERM, term_callback)
    loop.add_signal_handler(signal.SIGINT, term_callback)

    await story.init()
    await story.run()


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

    # setup_zookeeper
    zoo_client = ZooClient()
    asyncio.run(zoo_client.init())

    # --- handler system signals ---

    grace_term_period = 10
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    consumer_procs: Dict[str, Process] = {}

    def start_new_consumer(consumer_id: str):
        if consumer_id in consumer_procs and consumer_procs[consumer_id].is_alive():
            # TODO trigger alert on all errors
            logger.error(
                "Consumer#{} already in consumer_procs and is alive, something wrong happened",
                consumer_id,
            )
            return

        logger.info("Starting new consumer {}", consumer_id)
        p = Process(
            target=story_main,
            name=f"Consumer#{consumer_id}_{socket.gethostname()}",
            args=(consumer_id, config.get().config_file_path),
            daemon=True,
        )
        p.start()
        consumer_procs[consumer_id] = p

    def stop_consumer(consumer_id: str, waiting_seconds: int):
        p = consumer_procs[consumer_id]
        if p.is_alive():
            logger.warning("Sending SIGTERM to {}", p)
            os.kill(p.pid, signal.SIGTERM)

            t = time()
            while p.is_alive():
                if time() > t + waiting_seconds:
                    logger.warning("Sending SIGKILL to {}", p)
                    p.kill()
                sleep(0.1)

    for consumer_id, consumer_conf in config.get().consumers.items():
        if not consumer_conf.disabled:
            start_new_consumer(consumer_id)

    def signal_handler(signalname):
        def f(signal_received, frame):
            raise KeyboardInterrupt(f"{signalname} received")

        return f

    signal.signal(signal.SIGTERM, signal_handler("SIGTERM"))
    signal.signal(signal.SIGINT, signal_handler("SIGINT"))

    # --- handler config change signals ---

    # def handle_producer_config_change_signal(
    #     sender, added: Set[str], removed: Set[str], changed: Set[str]
    # ):
    #     if changed:
    #         for _, p in consumer_procs.items():
    #             if p.is_alive():
    #                 logger.warning("Sending SIGUSR1 to {}", p)
    #                 os.kill(p.pid, signal.SIGUSR1)
    #
    # config.ConfigSignals.PRODUCER_CHANGE.connect(handle_producer_config_change_signal)

    def handle_consumer_config_change_signal(
        sender, added: Set[str], removed: Set[str], changed: Set[str]
    ):
        for new_cid in added:
            start_new_consumer(new_cid)

        removed_cids = removed.intersection(list(consumer_procs.keys()))
        for cid in removed_cids:
            stop_consumer(cid, waiting_seconds=grace_term_period)

        changed_cids = changed.intersection(list(consumer_procs.keys()))
        for cid in changed_cids:
            stop_consumer(cid, waiting_seconds=grace_term_period)
            if not config.get().consumers[cid].disabled:
                start_new_consumer(cid)

    config.ConfigSignals.CONSUMER_CHANGE.connect(handle_consumer_config_change_signal)

    # --- monitor config change and sub-processes ---

    def get_alive_procs() -> List[Process]:
        return [p for p in list(consumer_procs.values()) if p.is_alive()]

    local_config_last_update_time = config.get().last_update_time
    watch_config_file(config.get().config_file_path, checking_interval=3)

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
