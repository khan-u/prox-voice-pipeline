from pipeline.spark.transforms import explode_realnet_links


def _bronze_stream(spark, payloads):
    rows = [
        (p, "wsn-remote-reports.jsonl", "realnet", "2026-08-17T00:00:00Z")
        for p in payloads
    ]
    return spark.createDataFrame(
        rows, ["payload", "source_file", "record_type", "ingested_at"]
    )


def test_explode_realnet_links_one_row_per_link(spark):
    # A report with two links, and one with an empty links array.
    two = (
        '{"cond": "cellular-N3-trial1", "sid": "s1", "ts": 100, "peers": 2, '
        '"links": ['
        '{"index": 1, "setupMs": 900, "rttMs": 40, "candidateType": "host", '
        '"latRawMs": 70, "glare": 0}, '
        '{"index": 2, "setupMs": 10165, "rttMs": 102, "candidateType": "relay", '
        '"latRawMs": -1, "glare": 2}]}'
    )
    none = '{"cond": "wifi-N2-trial1", "sid": "s2", "ts": 100, "peers": 1, "links": []}'
    df = _bronze_stream(spark, [two, none])

    facts = explode_realnet_links(df)
    rows = {(r["session_id"], r["link_index"]): r for r in facts.collect()}

    # two exploded links from s1, plus one null-link row from s2 (not dropped)
    assert (r_ := rows[("s1", 2)])["candidate_type"] == "relay"
    assert r_["rtt_ms"] == 102 and r_["setup_ms"] == 10165
    assert rows[("s1", 1)]["candidate_type"] == "host"
    assert rows[("s2", None)]["setup_ms"] is None
    assert rows[("s1", 1)]["condition"] == "cellular-N3-trial1"
    assert all(r["client"] is None for r in facts.collect())


def test_explode_realnet_links_keeps_only_latest_report_per_session(spark):
    # Same (cond, sid) reported twice; the newer report (ts=200) supersedes the
    # older one, which had a different link that must not appear.
    old = (
        '{"cond": "cellular-N2-STUN-trial1", "sid": "s9", "ts": 100, '
        '"links": [{"index": 1, "setupMs": 5000, "candidateType": "host", '
        '"rttMs": 200, "glare": 0}]}'
    )
    new = (
        '{"cond": "cellular-N2-STUN-trial1", "sid": "s9", "ts": 200, '
        '"links": [{"index": 1, "setupMs": 5714, "candidateType": "host", '
        '"rttMs": 101, "glare": 1}]}'
    )
    facts = explode_realnet_links(_bronze_stream(spark, [old, new])).collect()
    assert len(facts) == 1
    assert facts[0]["setup_ms"] == 5714 and facts[0]["rtt_ms"] == 101
