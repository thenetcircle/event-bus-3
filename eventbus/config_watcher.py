import os
import threading
import time
from pathlib import Path
from typing import Optional, Union

from loguru import logger

from eventbus import config

_last_update_time: Optional[float] = None


def _watch_file(config_file: Path, checking_interval: float) -> None:
    global _last_update_time
    while True:
        file_update_time = os.path.getmtime(config_file.resolve())
        if _last_update_time is None or _last_update_time < file_update_time:
            config.update_from_yaml(config_file)
            _last_update_time = file_update_time
        time.sleep(checking_interval)


def start_watching(
    config_file: Union[str, Path], checking_interval: float = 10
) -> None:
    config_file = Path(config_file)
    if not config_file.exists():
        raise FileNotFoundError(f"The config file `{config_file}` does not exist.")
    if not checking_interval > 0:
        raise ValueError("checking_interval must bigger than 0")

    logger.info("Start watching config file '{}'", config_file)

    config_watching_thread = threading.Thread(
        target=_watch_file,
        name="config_watching",
        args=(config_file, checking_interval),
        daemon=True,
    )
    config_watching_thread.start()
