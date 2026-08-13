from pipeline.ingest.land_raw import discover_exports, land_json_exports


def test_discover_exports_keeps_json_drops_stream_and_junk():
    names = [
        "wsn-voice-N2-summary-2026-08-11T00-05-41-191Z.json",
        "wsn-audiogap-2026-08-11T01-00-26-422Z.json",
        "wsn-remote-reports.jsonl",   # stream, landed separately
        "energy-screenshots",         # directory, not a telemetry file
    ]
    assert discover_exports(names) == [
        "wsn-audiogap-2026-08-11T01-00-26-422Z.json",
        "wsn-voice-N2-summary-2026-08-11T00-05-41-191Z.json",
    ]


def test_land_json_exports_stamps_provenance(spark, tmp_path):
    data_root = tmp_path / "data"
    data_root.mkdir()
    # Pretty-printed multi-line JSON: each export must land as exactly ONE row,
    # not one row per line.
    (data_root / "wsn-voice-N3-summary-2026-08-11T00-06-37-170Z.json").write_text(
        '{\n  "N": 3,\n  "perClient": []\n}\n'
    )
    (data_root / "wsn-audiogap-2026-08-11T01-00-26-422Z.json").write_text(
        '{\n  "valid": true\n}\n'
    )
    bronze_root = str(tmp_path / "bronze")

    n = land_json_exports(spark, str(data_root), bronze_root, "2026-08-17T00:00:00Z")
    assert n == 2

    rows = spark.read.format("delta").load(bronze_root + "/raw_json").collect()
    assert len(rows) == 2   # one row per file despite multi-line payloads
    by_type = {r["record_type"]: r for r in rows}
    assert set(by_type) == {"voice", "audiogap"}
    assert by_type["voice"]["peer_count"] == 3
    assert by_type["audiogap"]["peer_count"] is None
    assert all(r["ingested_at"] == "2026-08-17T00:00:00Z" for r in rows)
    assert '"N": 3' in by_type["voice"]["payload"]
