from eventbus import model


def test_convert_str_to_topic_mappings():
    thestr = '[{"topic": "event-v2-happ-greenarrow", "patterns": ["greenarrow\\\\..*"]}, {"topic": "event-v2-happ-default", "patterns": [".*"]}]'

    assert model.convert_str_to_topic_mappings(thestr) == [
        model.TopicMappingEntry(
            topic="event-v2-happ-greenarrow", patterns=["greenarrow\\..*"]
        ),
        model.TopicMappingEntry(topic="event-v2-happ-default", patterns=[".*"]),
    ]
