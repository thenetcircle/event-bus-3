import inspect
import json
import sys

from loguru import logger

from eventbus import config


def setup_logger():
    logger.remove()

    format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> "
        "| <level>{level: <8}</level> "
        "| <cyan>{process.name}</cyan> "  # :<cyan>{thread.name}</cyan>
        "| <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> "
        "- <level>{message}</level>"
    )

    short_format = "{level} | {name}:{function}:{line} | {message}"

    def my_short_format(record):
        return short_format

    # def my_log_serializer(record):
    #     return (
    #         json.dumps(
    #             {
    #                 "time": record["time"].strftime("%Y-%m-%d %H:%M:%S"),
    #                 "level": record["level"].name,
    #                 "message": record["message"],
    #                 "func_string": f"{record['name']}:{record['function']}:{record['line']}",
    #                 "data": record["extra"],
    #             },
    #             default=str,
    #             ensure_ascii=False,
    #         )
    #         + "\n"
    #     )

    if config.get().app.debug:
        logger.add(
            sys.stderr,
            level="DEBUG",
            format=my_short_format,
            serialize=True,
            enqueue=True,
            backtrace=True,
            diagnose=True,
        )
    else:
        logger.add(
            sys.stderr,
            level="INFO",
            format=my_short_format,
            serialize=True,
            enqueue=True,
            backtrace=False,
            diagnose=False,
        )

    import logging

    class InterceptHandler(logging.Handler):
        def emit(self, record: logging.LogRecord):
            # Get corresponding Loguru level if it exists
            level: str | int
            try:
                level = logger.level(record.levelname).name
            except ValueError:
                level = record.levelno

            # Find caller from where originated the logged message
            frame, depth = inspect.currentframe(), 0
            while frame and (
                depth == 0 or frame.f_code.co_filename == logging.__file__
            ):
                frame = frame.f_back
                depth += 1

            logger.opt(depth=depth, exception=record.exc_info).log(
                level, record.getMessage()
            )

    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)

    if config.get().sentry:
        logger.info(
            "Detected sentry_dsn: {}, going to set up Sentry.",
            config.get().sentry.dsn,
        )

        # https://docs.sentry.io/platforms/python/guides/logging/
        import sentry_sdk

        # from sentry_sdk.integrations.logging import LoggingIntegration
        from sentry_sdk.integrations.loguru import LoggingLevels, LoguruIntegration

        # from sentry_sdk.integrations.logging import LoggingIntegration
        # sentry_logging = LoggingIntegration(
        #     level=logging.INFO,  # Capture info and above as breadcrumbs
        #     event_level=logging.ERROR,  # Send errors as events
        # )
        # https://docs.sentry.io/platforms/python/configuration/options/
        # https://docs.sentry.io/platforms/python/integrations/loguru/
        sentry_sdk.init(
            dsn=config.get().sentry.dsn,
            debug=config.get().app.debug,
            environment=str(config.get().app.env),
            sample_rate=config.get().sentry.sample_rate,
            traces_sample_rate=config.get().sentry.traces_sample_rate,
            max_breadcrumbs=50,
            integrations=[
                LoguruIntegration(
                    level=LoggingLevels.INFO.value,
                    event_level=LoggingLevels.ERROR.value,
                ),
                # LoggingIntegration(level=None, event_level=logging.ERROR),
            ],
            default_integrations=False,
        )

        # logger.add(
        #     BreadcrumbHandler(level=logging.DEBUG),
        #     diagnose=False,
        #     level=logging.DEBUG,
        # )

        # logger.add(
        #     EventHandler(level=logging.ERROR),
        #     diagnose=True,
        #     level="ERROR",
        # )


def deep_merge_two_dict(dict1, dict2):
    for key, val in dict1.items():
        if isinstance(val, dict):
            dict2_node = dict2.setdefault(key, {})
            deep_merge_two_dict(val, dict2_node)
        else:
            if key not in dict2:
                dict2[key] = val
    return dict2
