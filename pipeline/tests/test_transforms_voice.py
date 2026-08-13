from pipeline.spark.transforms import flatten_voice_kpis


def _bronze_json(spark, payloads):
    rows = [(p, f"wsn-voice-N{i}-summary.json", "voice", "2026-08-17T00:00:00Z")
            for i, p in enumerate(payloads, start=2)]
    return spark.createDataFrame(
        rows, ["payload", "source_file", "record_type", "ingested_at"]
    )


def test_flatten_voice_kpis_one_row_per_client_with_normalized_candidate(spark):
    payload = (
        '{"N": 2, "perClient": ['
        '{"client": "wsn0", "kpis": {"setupMedianMs": 694, "latRawMedianMs": 70, '
        '"glareTotal": 0, "candidateMix": {"host": 1, "srflx": 0, "relay": 0}}}, '
        '{"client": "wsn1", "kpis": {"setupMedianMs": 394, "latRawMedianMs": 71, '
        '"glareTotal": 2, "candidateMix": {"host": 0, "srflx": 0, "relay": 1}}}]}'
    )
    facts = flatten_voice_kpis(_bronze_json(spark, [payload]))
    rows = {r["client"]: r for r in facts.collect()}

    assert rows["wsn0"]["setup_ms"] == 694
    assert rows["wsn0"]["candidate_type"] == "host"
    assert rows["wsn0"]["n_peers"] == 2
    assert rows["wsn0"]["link_index"] is None
    # relay in the mix normalizes to the relay tier
    assert rows["wsn1"]["candidate_type"] == "relay"
    assert rows["wsn1"]["glare"] == 2
