from pipeline.quality.expectations.gold_marts import run_checkpoint, validate_mart


def _kpi(spark, ice):
    return spark.createDataFrame(
        [(2, ice, 74, 495)],
        ["n_peers", "ice_success_pct", "m2e_ms", "setup_median_ms"],
    )


def test_checkpoint_passes_clean_marts(spark):
    marts = {
        "kpi_vs_n": _kpi(spark, 100),
        "sensing_suppression": spark.createDataFrame(
            [("idle", 91)], ["scenario", "suppression_median_pct"]
        ),
        "realnet_conditions": spark.createDataFrame(
            [("cellular", "relay", 131)], ["access", "path", "rtt_median_ms"]
        ),
    }
    per_mart, overall = run_checkpoint(marts)
    assert overall is True
    assert all(per_mart.values())


def test_checkpoint_flags_out_of_range_percentage(spark):
    # ICE success of 150% is impossible -> the kpi_vs_n suite must fail.
    assert validate_mart("kpi_vs_n", _kpi(spark, 150)).success is False
