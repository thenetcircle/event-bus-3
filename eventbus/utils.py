import sys

from loguru import logger

from eventbus import config


def setup_logger():
    logger.remove()

    if config.get().debug:
        logger.add(sys.stderr, level="DEBUG")
    else:
        logger.add(sys.stderr, level="INFO")
