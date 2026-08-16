from pipeline.ingest.eventhub_replay import frame_event, frame_events


def test_frame_event_keys_on_session_id():
    line = '{"cond": "wifi-N2-trial1", "sid": "abc", "ts": 100}'
    ev = frame_event(line)
    assert ev["partition_key"] == "abc"
    assert ev["body"] == line


def test_frame_event_falls_back_to_condition():
    ev = frame_event('{"cond": "cellular-N2-trial1", "ts": 5}')
    assert ev["partition_key"] == "cellular-N2-trial1"


def test_frame_events_skips_blank_lines():
    lines = [
        '{"sid": "a", "ts": 1}',
        "",
        "   ",
        '{"sid": "b", "ts": 2}',
    ]
    events = frame_events(lines)
    assert [e["partition_key"] for e in events] == ["a", "b"]
