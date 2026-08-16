"""Replay the real-network JSONL stream as discrete events.

Each line of wsn-remote-reports.jsonl becomes one event, partitioned by session
id so a session's reports stay ordered on the same partition. Framing is pure
and testable; the producer that ships these events (Event Hubs or a local
fallback) is layered on top.
"""
import json


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
