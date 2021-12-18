import os
import threading
import time
from pathlib import Path
from typing import Optional, Union

from loguru import logger

from eventbus import config
from eventbus.errors import ConfigWatchingError

_last_update_time: Optional[float] = None
_watch_file_thread: Optional[threading.Thread] = None


def _do_watch_file(config_file_path: Path, checking_interval: float) -> None:
    global _last_update_time
    while True:
        file_update_time = os.path.getmtime(config_file_path.resolve())
        if _last_update_time is None or _last_update_time < file_update_time:
            config.update_from_yaml(config_file_path)
            _last_update_time = file_update_time
        time.sleep(checking_interval)


def watch_file(
    config_file_path: Union[str, Path], checking_interval: float = 10
) -> None:
    config_file_path = Path(config_file_path)
    if not config_file_path.exists():
        raise FileNotFoundError(f"The config file `{config_file_path}` does not exist.")
    if not checking_interval > 0:
        raise ValueError("checking_interval must bigger than 0")

    global _last_update_time
    global _watch_file_thread

    if _watch_file_thread is not None:
        raise ConfigWatchingError("The config watcher already watching another config.")

    logger.info("Start watching config file '{}'", config_file_path)

    # load the config file first in current thread
    _last_update_time = os.path.getmtime(config_file_path.resolve())
    config.update_from_yaml(config_file_path)

    _watch_file_thread = threading.Thread(
        target=_do_watch_file,
        name="config_watching",
        args=(config_file_path, checking_interval),
        daemon=True,
    )
    _watch_file_thread.start()
