import asyncio
import os
import time
from pathlib import Path
from threading import Thread
from typing import Union

from loguru import logger

from eventbus import config


async def load_and_watch_file_from_environ(
    checking_interval: float = 10,
) -> None:
    config_file_path = (
        os.environ["EVENTBUS_CONFIG"]
        if "EVENTBUS_CONFIG" in os.environ
        else "config.yml"
    )
    await load_and_watch_file(config_file_path, checking_interval)


async def load_and_watch_file(
    config_file_path: Union[str, Path],
    checking_interval: float = 10,
) -> None:
    config_file_path = Path(config_file_path)
    if not config_file_path.exists():
        raise FileNotFoundError(f"The config file `{config_file_path}` does not exist.")
    if not checking_interval > 0:
        raise ValueError("checking_interval must bigger than 0")

    logger.info("Start watching config file '{}'", config_file_path)

    # load the config file first in current thread
    config.update_from_yaml(config_file_path)

    watch_file_thread = Thread(
        target=_watch_file,
        daemon=True,
        args=(config_file_path, checking_interval, asyncio.get_running_loop()),
    )
    watch_file_thread.start()


def _watch_file(
    config_file_path: Path, checking_interval: float, loop: asyncio.AbstractEventLoop
) -> None:
    last_update_time = os.path.getmtime(config_file_path.resolve())

    while True:
        try:
            new_update_time = os.path.getmtime(config_file_path.resolve())

            if last_update_time < new_update_time:
                logger.info(
                    'config file "{}" is detected changed, last_update_time: {}, new_update_time: {}',
                    config_file_path,
                    last_update_time,
                    new_update_time,
                )
                loop.call_soon_threadsafe(_update_config, config_file_path)
                last_update_time = new_update_time

            time.sleep(checking_interval)
        except Exception as ex:
            logger.error(
                '_watch_file on config file "{}" quit because of error: <{}> {}',
                config_file_path,
                type(ex).__name__,
                ex,
            )
            # TODO trigger alert


def _update_config(updated_config_file_path: Path) -> None:
    logger.info('_update_config get a new config file "{}"', updated_config_file_path)

    try:
        config.update_from_yaml(updated_config_file_path)
    except Exception as ex:
        logger.error(
            '_update_config updating config from file "{}" failed with error: <{}> {}',
            updated_config_file_path,
            type(ex).__name__,
            ex,
        )
        # TODO trigger alert
