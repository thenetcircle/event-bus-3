import asyncio
import json
import os
import signal
import socket
from multiprocessing import Process
from time import sleep, time
from typing import Dict, List, Optional, Set, Tuple
from kazoo.protocol.states import WatchedEvent, ZnodeStat

from loguru import logger

from eventbus import config, zoo_data_parser
from eventbus.config_watcher import watch_config_file
from eventbus.metrics import stats_client
from eventbus.model import StoryParams, StoryStatus
from eventbus.story import Story
from eventbus.utils import setup_logger
from eventbus.zoo_client import ZooClient
from eventbus.zoo_data_parser import ZooDataParser


def story_main(config_file_path: str, story_params: StoryParams):
    config.update_from_yaml(config_file_path)
    setup_logger()
    stats_client.init(config.get())
    story = Story(story_params)
    # run story
    asyncio.run(run_story(story))


async def run_story(story: Story):
    loop = asyncio.get_event_loop()

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
    zoo_client.init()
    zoo_data_parser = ZooDataParser(zoo_client)

    # --- handler system signals ---

    grace_term_period = 10
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    story_procs: Dict[str, Tuple[int, Process]] = {}

    def start_new_story(znode_version: int, story_params: StoryParams):
        story_id = story_params.id
        if story_params.status == StoryStatus.DISABLED:
            logger.info('Story "{}" is disabled, skip to start.', story_id)
            return
        if story_id in story_procs and story_procs[story_id][1].is_alive():
            # TODO trigger alert on all errors
            logger.error(
                "Consumer#{} already in consumer_procs and is alive, something went wrong!",
                story_id,
            )
            return

        logger.info("Starting new story proc {}", story_params)
        p = Process(
            target=story_main,
            name=f"Story#{story_id}_{socket.gethostname()}",
            args=(config.get().config_file_path, story_params),
            daemon=True,
        )
        p.start()
        story_procs[story_id] = (znode_version, p)

    def stop_story(story_id: str, waiting_seconds: int):
        if story_id not in story_procs:
            return
        p = story_procs[story_id][1]
        if p and p.is_alive():
            logger.warning("Sending SIGTERM to {}", p)
            if p.pid:
                os.kill(p.pid, signal.SIGTERM)

            t = time()
            while p.is_alive():
                if time() > t + waiting_seconds:
                    logger.warning("Sending SIGKILL to {}", p)
                    p.kill()
                sleep(0.1)

    def watch_story_changes(
        story_id: str, data: bytes, stats: ZnodeStat, event: Optional[WatchedEvent]
    ):
        try:
            if pdata := story_procs.get(story_id):
                if pdata[0] < stats.version:
                    logger.info("Story {} data changed, restart it.", story_id)
                    if story_params := zoo_data_parser.get_story_params(
                        story_id, data, stats
                    ):
                        logger.info("New story params: {}", story_params)
                        stop_story(story_id, grace_term_period)
                        logger.info("Story {} stopped.", story_id)
                        start_new_story(stats.version, story_params)
                else:
                    logger.info(
                        "Story {} data changed, but version is not newer.", story_id
                    )

            else:
                if story_params := zoo_data_parser.get_story_params(
                    story_id, data, stats
                ):
                    start_new_story(stats.version, story_params)
        except Exception as ex:
            logger.error("Failed to process story data change: {}", ex)

    def watch_story_list(story_ids: List[str]):
        add_list = set(story_ids).difference(list(story_procs.keys()))
        remove_list = set(story_procs.keys()).difference(story_ids)
        logger.info("Story list changed: add={}, remove={}", add_list, remove_list)

        for story_id in add_list:

            def wrapped_watch_story_changes(data, stats, event):
                watch_story_changes(story_id, data, stats, event)

            zoo_client.watch_data(
                config.get().zookeeper.story_path + "/" + story_id,
                wrapped_watch_story_changes,
            )

        for story_id in remove_list:
            stop_story(story_id, grace_term_period)

    zoo_client.watch_children(config.get().zookeeper.story_path, watch_story_list)

    def signal_handler(signalname):
        def f(signal_received, frame):
            raise KeyboardInterrupt(f"{signalname} received")

        return f

    signal.signal(signal.SIGTERM, signal_handler("SIGTERM"))
    signal.signal(signal.SIGINT, signal_handler("SIGINT"))

    # --- monitor config change and sub-processes ---

    def get_alive_procs() -> List[Process]:
        return [p for _, p in list(story_procs.values()) if p.is_alive()]

    try:
        # waiting for all procs to quit
        while alive_procs := get_alive_procs():
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
