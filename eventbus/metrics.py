from statsd import StatsClient
from eventbus import config


class StatsClientProxy:
    def __init__(self):
        self.client: StatsClient = None

    def init(self):
        statsd_config = config.get().statsd
        if statsd_config:
            prefix = statsd_config.prefix
            if config.get().app.env == config.Env.STAGING:
                prefix = f"{prefix}.staging" if prefix else "staging"

            self.client = StatsClient(
                host=statsd_config.host,
                port=statsd_config.port,
                prefix=prefix,
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
