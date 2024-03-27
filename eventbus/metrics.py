from statsd import StatsClient


class StatsClientProxy:
    def __init__(self):
        self.client: StatsClient = None

    def init(self):
        from eventbus import config

        _config = config.get()
        if _config.statsd:
            self.client = StatsClient(
                host=_config.statsd.host,
                port=_config.statsd.port,
                prefix=_config.statsd.prefix,
            )

    def incr(self, key: str):
        if self.client:
            self.client.incr(key)

    def decr(self, key: str):
        if self.client:
            self.client.decr(key)

    def timing(self, key: str, ms: float):
        if self.client:
            self.client.timing(key, ms)

    def gauge(self, key: str, value: int):
        if self.client:
            self.client.gauge(key, value)

    def set(self, key: str, value: int):
        if self.client:
            self.client.set(key, value)


stats_client = StatsClientProxy()
