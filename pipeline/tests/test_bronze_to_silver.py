from pipeline.ingest.land_raw import land_json_exports, land_jsonl_stream
from pipeline.spark.bronze_to_silver import build_link_fact
from pipeline.spark.transforms import FACT_COLUMNS


def test_build_link_fact_unions_voice_and_realnet(spark, tmp_path):
    data = tmp_path / "data"
    data.mkdir()
    (data / "wsn-voice-N2-summary-2026-08-11T00-05-41-191Z.json").write_text(
        '{"N": 2, "perClient": ['
        '{"client": "wsn0", "kpis": {"setupMedianMs": 694, "latRawMedianMs": 70, '
        '"glareTotal": 0, "candidateMix": {"host": 1, "srflx": 0, "relay": 0}}}]}'
    )
    (data / "wsn-remote-reports.jsonl").write_text(
        '{"cond": "cellular-N2-trial1", "sid": "s1", "peers": 1, '
        '"links": [{"index": 2, "setupMs": 10165, "rttMs": 102, '
        '"candidateType": "relay", "latRawMs": -1, "glare": 2}]}\n'
    )
    bronze = str(tmp_path / "bronze")
    silver = str(tmp_path / "silver")
    land_json_exports(spark, str(data), bronze, "2026-08-17T00:00:00Z")
    land_jsonl_stream(spark, str(data), bronze, "2026-08-17T00:00:00Z")

    n = build_link_fact(spark, bronze, silver)
    assert n == 2   # one voice client fact + one realnet link fact

    fact = spark.read.format("delta").load(silver + "/link_fact")
    assert fact.columns == FACT_COLUMNS
    by_type = {r["record_type"]: r for r in fact.collect()}
    assert by_type["voice"]["client"] == "wsn0"
    assert by_type["voice"]["candidate_type"] == "host"
    assert by_type["realnet"]["candidate_type"] == "relay"
    assert by_type["realnet"]["rtt_ms"] == 102
