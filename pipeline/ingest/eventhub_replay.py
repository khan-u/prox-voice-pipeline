"""Replay the real-network JSONL stream as discrete events.

Each line of wsn-remote-reports.jsonl becomes one event, partitioned by session
id so a session's reports stay ordered on the same partition. Framing is pure
and testable; the producer that ships these events (Event Hubs or a local
fallback) is layered on top.
"""
import json
import os


def frame_event(line):
    """Frame one JSONL line as an event: a partition key plus the raw body.

    The session id keys the partition so per-session ordering is preserved; if a
    line lacks one it falls back to the condition tag, then a constant.
    """
    record = json.loads(line)
    partition_key = record.get("sid") or record.get("cond") or "unknown"
    return {"partition_key": partition_key, "body": line.strip()}


def frame_events(lines):
    """Frame an iterable of JSONL lines, skipping blanks. Returns a list of
    event dicts in input order."""
    events = []
    for line in lines:
        if not line.strip():
            continue
        events.append(frame_event(line))
    return events


class LocalProducer:
    """Fallback producer: append event bodies to a local file, one per line.

    Lets the streaming path run end-to-end with no cloud subscription — the
    output file is a stand-in for the Event Hub partition log.
    """

    def __init__(self, path):
        self.path = path
        self.sent = 0

    def send(self, events):
        with open(self.path, "a", encoding="utf-8") as f:
            for ev in events:
                f.write(ev["body"] + "\n")
                self.sent += 1
        return self.sent


class EventHubProducer:
    """Ship events to Azure Event Hubs, batched by partition key.

    Imports azure-eventhub lazily so the module loads without the SDK; only
    constructing this class requires it.
    """

    def __init__(self, connection_str, eventhub_name):
        from azure.eventhub import EventHubProducerClient

        self._client = EventHubProducerClient.from_connection_string(
            connection_str, eventhub_name=eventhub_name
        )
        self.sent = 0

    def send(self, events):
        from azure.eventhub import EventData

        for ev in events:
            batch = self._client.create_batch(partition_key=ev["partition_key"])
            batch.add(EventData(ev["body"]))
            self._client.send_batch(batch)
            self.sent += 1
        return self.sent


def make_producer(config, local_path="pipeline/lake/stream/events.log"):
    """Select a producer from config: Event Hubs when an azure connection is
    configured, otherwise the local file fallback."""
    conn = config.get("eventhub_conn_str")
    name = config.get("eventhub_name")
    if config.get("mode") == "azure" and conn and name:
        return EventHubProducer(conn, name)
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    return LocalProducer(local_path)


def replay(lines, producer):
    """Frame `lines` and send them via `producer`. Returns the count sent."""
    return producer.send(frame_events(lines))
