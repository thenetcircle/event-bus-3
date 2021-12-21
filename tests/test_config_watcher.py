import re
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from eventbus import config, config_watcher
from eventbus.errors import ConfigNoneError


@pytest.mark.noconfig
def test_watch_file(tmpdir, mocker):
    with pytest.raises(ConfigNoneError):
        config.get()

    origin_config_file = Path(__file__).parent / "config.yml"
    with open(origin_config_file, "r") as f:
        origin_config_data = f.read()

    config_file = tmpdir / "config.yml"
    with open(config_file, "w") as f:
        f.write(origin_config_data)

    sub1: MagicMock = mocker.MagicMock()
    sub2: MagicMock = mocker.MagicMock()
    config.add_subscriber(sub1)
    config.add_subscriber(sub2)

    config_watcher.watch_file(config_file, checking_interval=0.1)

    time.sleep(0.3)  # waiting for the config_file to be loaded
    assert config.get().env == config.Env.TEST
    sub1.assert_called_once()
    sub2.assert_called_once()
    sub1.reset_mock()
    sub2.reset_mock()

    # add some other subscribers after watching
    sub3: MagicMock = mocker.MagicMock()
    config.add_subscriber(sub3)

    class Sub5:
        def __init__(self):
            self.attr1 = "attr1"
            self.call_times = 0

        def sub_method(self):
            self.call_times += 1
            assert self.attr1 == "attr1"

    sub5 = Sub5()
    config.add_subscriber(sub5.sub_method)

    with open(config_file, "w") as f:
        new_config_data = re.sub(r"env: test", "env: prod", origin_config_data)
        f.write(new_config_data)
    time.sleep(0.3)  # waiting for the config_file to be reloaded
    assert config.get().env == config.Env.PROD
    sub1.assert_called_once()
    sub2.assert_called_once()
    sub3.assert_called_once()
    sub1.reset_mock()
    sub2.reset_mock()
    sub3.reset_mock()
    assert sub5.call_times == 1

    time.sleep(0.3)  # waiting if another reloading happened
    assert config.get().env == config.Env.PROD
    sub1.assert_not_called()
    sub2.assert_not_called()
    sub3.assert_not_called()
    assert sub5.call_times == 1
