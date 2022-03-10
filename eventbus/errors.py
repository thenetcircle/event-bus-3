class InitError(Exception):
    pass


class ConfigNoneError(Exception):
    pass


class ConfigUpdateError(Exception):
    pass


class ConfigSubscribeError(Exception):
    pass


class ConfigWatchingError(Exception):
    pass


class EventValidationError(Exception):
    pass


class EventProducingError(Exception):
    # TODO add root cause
    pass


class InitProducerError(Exception):
    pass


class InitConsumerError(Exception):
    pass


class EventConsumingError(Exception):
    pass


class InvalidArgumentError(Exception):
    pass


class ClosedError(Exception):
    pass
