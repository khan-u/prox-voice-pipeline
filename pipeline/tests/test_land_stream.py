from pipeline.ingest.land_raw import land_jsonl_stream


def test_land_jsonl_stream_dedups_redelivered_lines(spark, tmp_path):
    data_root = tmp_path / "data"
    data_root.mkdir()
    line_a = '{"cond": "wifi-N2-trial1", "sid": "aaa", "ts": 100, "kpis": {}}'
    line_b = '{"cond": "wifi-N2-trial1", "sid": "bbb", "ts": 200, "kpis": {}}'
    # line_a redelivered verbatim, plus a blank line the loader must skip
    (data_root / "wsn-remote-reports.jsonl").write_text(
        "\n".join([line_a, line_b, line_a, ""]) + "\n"
    )
    bronze_root = str(tmp_path / "bronze")

    n = land_jsonl_stream(spark, str(data_root), bronze_root, "2026-08-17T00:00:00Z")
    assert n == 2   # three data lines, one a duplicate

    rows = spark.read.format("delta").load(bronze_root + "/raw_stream").collect()
    assert {r["sid"] for r in rows} == {"aaa", "bbb"}
    assert all(r["record_type"] == "realnet" for r in rows)
    assert all(r["ingested_at"] == "2026-08-17T00:00:00Z" for r in rows)


def test_land_jsonl_stream_absent_is_noop(spark, tmp_path):
    data_root = tmp_path / "empty"
    data_root.mkdir()
    assert land_jsonl_stream(spark, str(data_root), str(tmp_path / "b"), "x") == 0
