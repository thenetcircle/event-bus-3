from blinker import signal

CONFIG_CHANGED = signal("config_changed")
CONFIG_PRODUCER_CHANGED = signal("config_producer_changed")
CONFIG_TOPIC_MAPPING_CHANGED = signal("config_topic_mapping_changed")
CONFIG_CONSUMER_CHANGED = signal("config_consumer_changed")
