import sys

from loguru import logger

from eventbus import config


def setup_logger():
    logger.remove()

    format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> "
        "| <level>{level: <8}</level> "
        "| <cyan>{process.name}</cyan>:<cyan>{thread.name}</cyan> "
        "| <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> "
        "- <level>{message}</level>"
    )

    if config.get().debug:
        logger.add(sys.stderr, level="DEBUG", format=format)
    else:
        logger.add(sys.stderr, level="INFO", format=format)

    if config.get().sentry_dsn:
        # TODO test
        import logging

        import sentry_sdk
        from sentry_sdk.integrations.logging import EventHandler

        sentry_sdk.init(config.get().sentry_dsn)
        logger.add(
            EventHandler(level=logging.ERROR),
            diagnose=True,
            level="ERROR",
        )
