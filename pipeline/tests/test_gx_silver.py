from pipeline.quality.expectations.silver_fact import validate

COLUMNS = ["record_type", "source_file", "candidate_type", "n_peers", "glare"]


def test_silver_suite_passes_clean_data(spark):
    df = spark.createDataFrame(
        [
            ("voice", "wsn-voice-N2-summary.json", "host", 2, 0),
            ("realnet", "wsn-remote-reports.jsonl", "relay", 1, 2),
        ],
        COLUMNS,
    )
    assert validate(df).success is True


def test_silver_suite_flags_bad_candidate_type(spark):
    df = spark.createDataFrame(
        [
            ("voice", "wsn-voice-N2-summary.json", "host", 2, 0),
            ("realnet", "wsn-remote-reports.jsonl", "bogus", 1, 2),
        ],
        COLUMNS,
    )
    assert validate(df).success is False
