import asyncio
import re
import time
from datetime import datetime
from pathlib import Path
from threading import Thread
from typing import Optional, Union

from loguru import logger

from eventbus import config


async def async_watch_config_file(
    config_file_path: Union[str, Path],
    checking_interval: float = 10,
) -> None:
    watch_config_file(
        config_file_path, checking_interval, loop=asyncio.get_running_loop()
    )


def watch_config_file(
    config_file_path: Union[str, Path],
    checking_interval: float = 10,
    loop: Optional[asyncio.AbstractEventLoop] = None,
) -> None:
    config_file_path = Path(config_file_path)
    if not config_file_path.exists():
        raise FileNotFoundError(f"The config file `{config_file_path}` does not exist.")
    if not checking_interval > 0:
        raise ValueError("checking_interval must bigger than 0")

    logger.info("Start watching config file '{}'", config_file_path)

    watch_file_thread = Thread(
        target=_watch_file,
        daemon=True,
        name="config_watcher",
        args=(config_file_path, checking_interval, loop),
    )
    watch_file_thread.start()


def _watch_file(
    config_file_path: Path,
    checking_interval: float,
    loop: Optional[asyncio.AbstractEventLoop] = None,
) -> None:
    logger.info(
        '_watch_file on config file "{}" started, with checking_interval: {}',
        config_file_path,
        checking_interval,
    )
    last_update_time: int = config.get().last_update_time

    while True:
        try:
            new_update_time = 0
            with open(config_file_path.resolve()) as f:
                first_line = f.readline()
                if _match := re.match(r"last_update_time: ([0-9]+)", first_line):
                    new_update_time = int(_match.group(1))
                else:
                    logger.error(
                        "No last_update_time found in config file {}", config_file_path
                    )

            if new_update_time > last_update_time:
                logger.info(
                    'config file "{}" is detected changed, last_update_time: {}, new_update_time: {}',
                    config_file_path,
                    datetime.fromtimestamp(last_update_time),
                    datetime.fromtimestamp(new_update_time),
                )

                config.update_from_yaml(config_file_path)
                if loop:
                    loop.call_soon_threadsafe(config.send_signals)

                last_update_time = new_update_time

            elif new_update_time != 0 and new_update_time != last_update_time:
                logger.warning(
                    "Get new_update_time {} but less than last_update_time {}",
                    new_update_time,
                    last_update_time,
                )

            time.sleep(checking_interval)
        except Exception as ex:
            logger.error(
                '_watch_file on config file "{}" quit because of error: <{}> {}',
                config_file_path,
                type(ex).__name__,
                ex,
            )
            # TODO trigger alert
