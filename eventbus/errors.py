class EventBusError(Exception):
    pass


class InitError(EventBusError):
    pass


class ConfigNoneError(EventBusError):
    pass


class ConfigUpdateError(EventBusError):
    pass


class SendSignalError(EventBusError):
    pass


class ConfigWatchingError(EventBusError):
    pass


class EventValidationError(EventBusError):
    pass


class NoMatchedKafkaTopicError(EventBusError):
    pass


class EventProducingError(EventBusError):
    # TODO add root cause
    pass


class InitProducerError(EventBusError):
    pass


class InitConsumerError(EventBusError):
    pass


class EventConsumerNotFoundError(EventBusError):
    pass


class ConsumerPollingError(EventBusError):
    pass


class InvalidArgumentError(EventBusError):
    pass


class ClosedError(EventBusError):
    pass
