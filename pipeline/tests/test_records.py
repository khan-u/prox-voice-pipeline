import pytest

from pipeline.common.records import classify, peer_count, provenance


def test_classify_maps_prefixes_and_rejects_unknown():
    assert classify("wsn-audiogap-2026-08-11T01-00-26-422Z.json") == "audiogap"
    assert classify("wsn-voice-N3-summary-2026-08-11T00-06-37-170Z.json") == "voice"
    assert classify("wsn-remote-reports.jsonl") == "realnet"
    with pytest.raises(ValueError):
        classify("energy-screenshots")


def test_provenance_stamps_type_and_peer_count():
    prov = provenance(
        "wsn-voice-N4-summary-2026-08-11T00-07-35-443Z.json", "2026-08-17T00:00:00Z"
    )
    assert prov == {
        "source_file": "wsn-voice-N4-summary-2026-08-11T00-07-35-443Z.json",
        "record_type": "voice",
        "peer_count": 4,
        "ingested_at": "2026-08-17T00:00:00Z",
    }
    # non-voice records carry no peer count
    assert peer_count("wsn-sensing-idle-2026-08-11T00-00-00-000Z.json") is None
