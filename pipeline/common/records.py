"""Record-type classification and provenance stamping for bronze ingestion.

Every raw telemetry file is named `wsn-<experiment>-...<timestamp>.json` (or the
append-only `wsn-remote-reports.jsonl` stream). The bronze layer stamps each
landed record with where it came from so silver/gold can filter by experiment
and trace a value back to its source file. All functions here are pure.
"""
import re

# Filename prefix -> canonical record type. The prefix is the token between the
# leading `wsn-` and the next `-`.
_PREFIX_TYPES = {
    "audiogap": "audiogap",
    "dtx": "dtx",
    "gracegap": "gracegap",
    "mobility": "mobility",
    "remote": "realnet",
    "sensing": "sensing",
    "voice": "voice",
}

_PREFIX = re.compile(r"^wsn-([a-z]+)")
_PEER_COUNT = re.compile(r"-N(\d+)")


def classify(filename):
    """Return the canonical record type for a telemetry filename.

    Raises ValueError for a name that does not match the `wsn-<experiment>`
    convention, so an unexpected file is rejected at ingest rather than landing
    as an untyped record.
    """
    match = _PREFIX.match(filename)
    if not match or match.group(1) not in _PREFIX_TYPES:
        raise ValueError(f"unrecognized telemetry filename: {filename!r}")
    return _PREFIX_TYPES[match.group(1)]


def peer_count(filename):
    """Extract the N-peer count from a voice filename, or None if absent."""
    match = _PEER_COUNT.search(filename)
    return int(match.group(1)) if match else None


def provenance(filename, ingested_at):
    """Build the provenance columns stamped onto every bronze record.

    `ingested_at` is passed in (not read from the clock) so ingestion is
    deterministic and testable.
    """
    return {
        "source_file": filename,
        "record_type": classify(filename),
        "peer_count": peer_count(filename),
        "ingested_at": ingested_at,
    }
