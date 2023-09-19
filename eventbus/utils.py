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

    if config.get().app.debug:
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

    if config.get().app.sentry:
        logger.info(
            "Detected sentry_dsh: {}, going to set up Sentry.",
            config.get().app.sentry.dsn,
        )

        # https://docs.sentry.io/platforms/python/guides/logging/
        import sentry_sdk
        from sentry_sdk.integrations.logging import (
            BreadcrumbHandler,
            EventHandler,
            LoggingIntegration,
        )

        # from sentry_sdk.integrations.logging import LoggingIntegration
        # sentry_logging = LoggingIntegration(
        #     level=logging.INFO,  # Capture info and above as breadcrumbs
        #     event_level=logging.ERROR,  # Send errors as events
        # )
        # https://docs.sentry.io/platforms/python/configuration/options/
        sentry_sdk.init(
            dsn=config.get().app.sentry.dsn,
            debug=config.get().app.debug,
            environment=str(config.get().app.env),
            sample_rate=config.get().app.sentry.sample_rate,
            traces_sample_rate=config.get().app.sentry.traces_sample_rate,
            integrations=[
                LoggingIntegration(level=None, event_level=None),
            ],
        )

        # logger.add(
        #     BreadcrumbHandler(level=logging.DEBUG),
        #     diagnose=False,
        #     level=logging.DEBUG,
        # )

        logger.add(
            EventHandler(level=logging.ERROR),
            diagnose=True,
            level="ERROR",
        )


def deep_merge_two_dict(dict1, dict2):
    for key, val in dict1.items():
        if isinstance(val, dict):
            dict2_node = dict2.setdefault(key, {})
            deep_merge_two_dict(val, dict2_node)
        else:
            if key not in dict2:
                dict2[key] = val
    return dict2
