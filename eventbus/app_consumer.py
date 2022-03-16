from eventbus import config, config_watcher
from eventbus.topic_resolver import TopicResolver

config_watcher.watch_file_from_environ()
topic_resolver = TopicResolver()


config.get().consumers
