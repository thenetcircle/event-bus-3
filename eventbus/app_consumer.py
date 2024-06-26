import asyncio
import sys
import os
import signal
import socket
import multiprocessing
from multiprocessing import Process
from time import sleep, time
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from functools import partial

from kazoo.protocol.states import WatchedEvent, ZnodeStat, EventType
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
    except Exception as ex:
        logger.exception("Process of story {} is quitting on code 1", story._params.id)
        sys.exit(1)
    finally:
        stats_client.incr("app.consumer.story.quit")
        await story.close()
        await logger.complete()
        await asyncio.sleep(3)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="EventBus 3 Consumer")
    parser.add_argument(
        "-c",
        "--config_file",
        type=str,
        help="Config file path. If not specified, it will look for environment variable `EB_CONF_FILE`",
    )
    args = parser.parse_args()

    multiprocessing.set_start_method("spawn")

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

    @dataclass
    class StoryProcess:
        params: StoryParams
        process: Process
        znode_version: int
        v2_runner: str

    story_procs: Dict[str, StoryProcess] = {}
    story_is_changing = True

    def start_new_story(story_params: StoryParams, znode_version: int, v2_runner: str):
        story_id = story_params.id
        if story_params.status == StoryStatus.DISABLED:
            logger.info('Story "{}" is disabled, skip to start.', story_id)
            return
        if story_id in story_procs and story_procs[story_id].process.is_alive():
            logger.bind(story_params=story_params).error(
                "Consumer already in consumer_procs and is alive, something went wrong!",
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
        story_procs[story_id] = StoryProcess(
            params=story_params,
            process=p,
            znode_version=znode_version,
            v2_runner=v2_runner,
        )

    def stop_story(story_id: str, waiting_seconds: int):
        if story_id not in story_procs:
            return
        p = story_procs[story_id].process
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
        v2_runner: str,
        story_id: str,
        data: bytes,
        stats: ZnodeStat,
        event: Optional[WatchedEvent],
    ):
        logger.debug("data: {}, stats: {}, event: {}", data, stats, event)
        if event is not None and event.type != EventType.CHANGED:
            return

        nonlocal story_is_changing
        story_is_changing = True
        try:
            if sp_data := story_procs.get(story_id):
                if (
                    sp_data.v2_runner == v2_runner
                ):  # Check if the changes are from the same runner
                    logger.info("Story {} data changed, restart it.", story_id)
                    if story_params := zoo_data_parser.create_story_params(story_id):
                        logger.info("New story params: {}", story_params)
                        stop_story(story_id, grace_term_period)
                        logger.info("Story {} stopped.", story_id)
                        start_new_story(story_params, stats.version, v2_runner)
                else:
                    logger.warning(
                        "Story {} data changed from runner {}, but not from the same runner {}, skip it.",
                        story_id,
                        v2_runner,
                        sp_data.v2_runner,
                    )

            else:
                if story_params := zoo_data_parser.create_story_params(story_id):
                    start_new_story(story_params, stats.version, v2_runner)
        except Exception as ex:
            logger.exception("Failed to process story data change")
        finally:
            story_is_changing = False

    def watch_story_list(v2_runner: str, v2_runner_path: str, story_ids: List[str]):
        current_list = []
        for sid, sp_data in story_procs.items():
            if sp_data.v2_runner == v2_runner:
                current_list.append(sid)

        add_list = set(story_ids).difference(current_list)
        remove_list = set(current_list).difference(story_ids)
        logger.info(
            "Stories which are assigned to runner {} are changed: add={}, remove={}",
            v2_runner,
            add_list,
            remove_list,
        )

        for story_id in add_list:
            zoo_client.watch_data(
                f"{v2_runner_path}/{story_id}",
                partial(watch_story_changes, v2_runner, story_id),
            )

        if len(remove_list) > 0:
            for story_id in remove_list:
                stop_story(story_id, grace_term_period)

    for v2_runner, v2_runner_path in zoo_data_parser.get_v2_runner_stories_path():
        zoo_client.watch_children(
            v2_runner_path,
            partial(watch_story_list, v2_runner, v2_runner_path),
        )

    def signal_handler(signalname):
        def f(signal_received, frame):
            raise KeyboardInterrupt(f"{signalname} received")

        return f

    signal.signal(signal.SIGTERM, signal_handler("SIGTERM"))
    signal.signal(signal.SIGINT, signal_handler("SIGINT"))

    # --- monitor config change and sub-processes ---

    def get_alive_procs() -> List[Process]:
        return [
            sp_data.process
            for sp_data in list(story_procs.values())
            if sp_data.process.is_alive()
        ]

    try:
        while True:
            while story_is_changing:
                sleep(0.2)

            for story_id in list(story_procs.keys()):
                sp_data = story_procs[story_id]
                if not sp_data.process.is_alive():
                    if sp_data.process.exitcode == 1:  # if not exit normally, restart
                        logger.info(
                            "Story {} exited abnormally with code {}, will restart!",
                            story_id,
                            sp_data.process.exitcode,
                        )
                        stats_client.incr("app.consumer.story.quit.abnormal")
                        sleep(3)
                        start_new_story(
                            sp_data.params, sp_data.znode_version, sp_data.v2_runner
                        )
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
