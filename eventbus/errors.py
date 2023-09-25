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


class InitKafkaProducerError(EventBusError):
    pass


class InitConsumerError(EventBusError):
    pass


class EventConsumerNotFoundError(EventBusError):
    pass


class KafkaConsumerPollingError(EventBusError):
    pass


class KafkaConsumerClosedError(EventBusError):
    pass


class InvalidArgumentError(EventBusError):
    pass
