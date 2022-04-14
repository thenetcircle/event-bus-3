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

    import logging

    class InterceptHandler(logging.Handler):
        def emit(self, record):
            # Get corresponding Loguru level if it exists
            try:
                level = logger.level(record.levelname).name
            except ValueError:
                level = record.levelno

            # Find caller from where originated the logged message
            frame, depth = logging.currentframe(), 2
            while frame.f_code.co_filename == logging.__file__:
                frame = frame.f_back
                depth += 1

            logger.opt(depth=depth, exception=record.exc_info).log(
                level, record.getMessage()
            )

    logging.basicConfig(handlers=[InterceptHandler()], level=0)

    if config.get().sentry_dsn:
        import sentry_sdk
        from sentry_sdk.integrations.logging import EventHandler

        sentry_sdk.init(config.get().sentry_dsn)
        logger.add(
            EventHandler(level=logging.ERROR),
            diagnose=True,
            level="ERROR",
        )
