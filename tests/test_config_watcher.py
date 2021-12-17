import re
import time
from pathlib import Path

import pytest

from eventbus import config, config_watcher
from eventbus.errors import ConfigNoneError


def test_watching(tmpdir):
    with pytest.raises(ConfigNoneError):
        config.get()

    origin_config_file = Path(__file__).parent / "config.yml"
    with open(origin_config_file, "r") as f:
        origin_config_data = f.read()

    config_file = tmpdir / "config.yml"
    with open(config_file, "w") as f:
        f.write(origin_config_data)

    config_watcher.start_watching(config_file, checking_interval=0.5)

    time.sleep(1)  # waiting for the config_file to be loaded
    assert config.get().env == config.Env.TEST

    with open(config_file, "w") as f:
        new_config_data = re.sub(r"env: test", "env: prod", origin_config_data)
        f.write(new_config_data)
    time.sleep(1)  # waiting for the config_file to be reloaded
    assert config.get().env == config.Env.PROD

    time.sleep(1)  # waiting if another reloading happened


def test_notify_subscriber():
    # existed subscriber
    # delayed subscriber
    pass
