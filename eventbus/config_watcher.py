import asyncio
import os
import time
from pathlib import Path
from threading import Thread
from typing import Any, Dict, Optional, Union

from loguru import logger

from eventbus import config, signals
from eventbus.config import Config
from eventbus.errors import SendSignalError


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


def send_signals(old_config: Config, new_config: Config) -> None:
    try:
        signal_sender = "config"

        if old_config != new_config:
            receivers = signals.CONFIG_CHANGED.send(signal_sender)
            logger.info(
                "Config changed, sent CONFIG_CHANGED signal to receivers {}",
                receivers,
            )

        def compare_two_config(
            old: Dict[str, Any], new: Dict[str, Any]
        ) -> Dict[str, set]:
            old_keys = set(old.keys())
            new_keys = set(new.keys())

            removed = old_keys.difference(new_keys)
            added = new_keys.difference(old_keys)
            changed = set()
            for k in old_keys.intersection(new_keys):
                if old[k] != new[k]:
                    changed.add(k)

            return {"added": added, "removed": removed, "changed": changed}

        if old_config.producers != new_config.producers:
            kwargs = compare_two_config(
                old_config.producers,
                new_config.producers,
            )
            receivers = signals.CONFIG_PRODUCER_CHANGED.send(signal_sender, **kwargs)
            logger.info(
                "Config changed, sent CONFIG_PRODUCER_CHANGED signal to receivers {}",
                receivers,
            )

        if old_config.consumers != new_config.consumers:
            kwargs = compare_two_config(
                old_config.consumers,
                new_config.consumers,
            )
            receivers = signals.CONFIG_CONSUMER_CHANGED.send(signal_sender, **kwargs)
            logger.info(
                "Config changed, sent CONFIG_CONSUMER_CHANGED signal to receivers {}",
                receivers,
            )

        if old_config.topic_mapping != new_config.topic_mapping:
            receivers = signals.CONFIG_TOPIC_MAPPING_CHANGED.send(signal_sender)
            logger.info(
                "Config changed, sent CONFIG_TOPIC_MAPPING_CHANGED signal to receivers {}",
                receivers,
            )

    except Exception as ex:
        logger.error("Sent ConfigSignals failed with error: {} {}", type(ex), ex)
        # TODO trigger alert
        raise SendSignalError


def _watch_file(
    config_file_path: Path,
    checking_interval: float,
    loop: Optional[asyncio.AbstractEventLoop] = None,
) -> None:
    last_update_time = os.path.getmtime(config_file_path.resolve())
    logger.info(
        '_watch_file on config file "{}" started, with checking_interval: {}, last_update_time: {}',
        config_file_path,
        checking_interval,
        last_update_time,
    )

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

                config.update_from_yaml(config_file_path)

                if loop:
                    old_config = config.get_last()
                    new_config = config.get()
                    if old_config and new_config:
                        loop.call_soon_threadsafe(send_signals, old_config, new_config)

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
