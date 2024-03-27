import asyncio
import os
import signal
import socket
from multiprocessing import Process
from time import sleep, time
from typing import Dict, List, Optional, Tuple

from kazoo.protocol.states import WatchedEvent, ZnodeStat
from loguru import logger

from eventbus import config
from eventbus.metrics import stats_client
from eventbus.story import Story, StoryParams, StoryStatus
from eventbus.utils import setup_logger
from eventbus.zoo_client import ZooClient
from eventbus.zoo_data_parser import ZooDataParser


def story_main(config_file_path: str, story_params: StoryParams):
    config.update_from_yaml(config_file_path)
    setup_logger()
    stats_client.init()
    stats_client.incr("app.consumer.story.init")

    story = Story(story_params)
    # run story
    asyncio.run(run_story(story))


async def run_story(story: Story):
    loop = asyncio.get_event_loop()

    def term_callback():
        logger.info("Get TERM signals, going to terminate Story.")
        asyncio.run_coroutine_threadsafe(story.close(), loop)

    # add signals handlers
    loop.add_signal_handler(signal.SIGTERM, term_callback)
    loop.add_signal_handler(signal.SIGINT, term_callback)

    await story.init()
    try:
        await story.run()
    finally:
        stats_client.incr("app.consumer.story.quit")
        await story.close()
        await asyncio.sleep(5)


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
    stats_client.init()

    stats_client.incr("app.consumer.init")

    # setup_zookeeper
    zoo_client = ZooClient(
        hosts=config.get().zookeeper.hosts, timeout=config.get().zookeeper.timeout
    )
    zoo_client.init()
    zoo_data_parser = ZooDataParser(zoo_client)

    # --- handler system signals ---

    grace_term_period = 10
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    story_procs: Dict[str, Tuple[int, StoryParams, Process]] = {}
    story_is_changing = True

    def start_new_story(znode_version: int, story_params: StoryParams):
        story_id = story_params.id
        if story_params.status == StoryStatus.DISABLED:
            logger.info('Story "{}" is disabled, skip to start.', story_id)
            return
        if story_id in story_procs and story_procs[story_id][2].is_alive():
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
        story_procs[story_id] = (znode_version, story_params, p)

    def stop_story(story_id: str, waiting_seconds: int):
        if story_id not in story_procs:
            return
        p = story_procs[story_id][2]
        if p and p.is_alive():
            if p.pid:
                logger.warning("Sending SIGTERM to {}", p)
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
        nonlocal story_is_changing
        story_is_changing = True
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
        finally:
            story_is_changing = False

    def watch_story_list(story_ids: List[str]):
        add_list = set(story_ids).difference(list(story_procs.keys()))
        remove_list = set(story_procs.keys()).difference(story_ids)
        logger.info("Story list changed: add={}, remove={}", add_list, remove_list)

        for story_id in add_list:
            zoo_client.watch_data(
                config.get().zookeeper.story_path + "/" + story_id,
                lambda data, stats, event: watch_story_changes(
                    story_id, data, stats, event
                ),
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
        return [p for _, _, p in list(story_procs.values()) if p.is_alive()]

    try:
        while True:
            while story_is_changing:
                sleep(0.2)

            for story_id in list(story_procs.keys()):
                zversion, story_params, p = story_procs[story_id]
                if not p.is_alive():
                    if p.exitcode:  # if not exit normally, restart
                        logger.info(
                            "Story {} exited abnormally with code {}, will restart!",
                            story_id,
                            p.exitcode,
                        )
                        start_new_story(zversion, story_params)
                    else:
                        logger.info("Story {} process exited normally!", story_id)
                        story_procs.pop(story_id)
            if len(story_procs) > 0:
                sleep(0.2)
            else:
                break

    except KeyboardInterrupt:
        logger.warning("Caught KeyboardInterrupt! Stopping consumers...")

    finally:
        for p in get_alive_procs():
            if p.pid:
                logger.warning("Sending SIGTERM to {}", p)
                os.kill(p.pid, signal.SIGINT)

        t = time()
        while alive_procs := get_alive_procs():
            if time() > t + grace_term_period:
                for p in alive_procs:
                    logger.warning("Sending SIGKILL to {}", p)
                    p.kill()
            sleep(0.01)

    stats_client.incr("app.consumer.quit")
    logger.warning("Main process quit.")


if __name__ == "__main__":
    main()
