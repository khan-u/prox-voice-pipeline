from pipeline.spark.transforms import (
    normalize_audiogap,
    normalize_dtx,
    normalize_gracegap,
    normalize_mobility,
    normalize_sensing,
)


def _df(spark, record_type, payloads):
    rows = [(p, f"wsn-{record_type}-{i}.json", record_type, "2026-08-17T00:00:00Z")
            for i, p in enumerate(payloads)]
    return spark.createDataFrame(
        rows, ["payload", "source_file", "record_type", "ingested_at"]
    )


def test_normalize_mobility_flags_complete_cases(spark):
    complete = ('{"phases": {"initialForm": {"ms": 800}, '
                '"teardown": {"ms": 20}, "reconnect": {"ms": 2400}}}')
    partial = ('{"phases": {"initialForm": {"ms": 800}, '
               '"teardown": {"ms": -1}, "reconnect": {"ms": 25}}}')
    rows = sorted(
        normalize_mobility(_df(spark, "mobility", [complete, partial])).collect(),
        key=lambda r: r["form_ms"] or -1,
    )
    by_complete = {r["complete_case"]: r for r in rows}
    assert by_complete[True]["reconnect_ms"] == 2400
    assert by_complete[True]["teardown_ms"] == 20
    assert by_complete[False]["teardown_ms"] == -1


def test_normalize_scalar_experiments(spark):
    ag = normalize_audiogap(_df(spark, "audiogap", [
        '{"valid": true, "audioStopAfterOutMs": 30, '
        '"reconnectAudioGapMs": 2400, "totalSilenceMs": 9000}',
        '{"audioStopAfterOutMs": 5}',   # missing valid -> False
    ])).collect()
    ag_by_valid = {r["valid"]: r for r in ag}
    assert ag_by_valid[True]["total_silence_ms"] == 9000
    assert False in ag_by_valid

    gg = normalize_gracegap(_df(spark, "gracegap", [
        '{"absenceMs": 6000, "graceMs": 1000, "reconnectGapMs": 280, '
        '"holdKbps": 0, "stopAfterOutMs": 40}'
    ])).collect()[0]
    assert (gg["absence_ms"], gg["grace_ms"], gg["reconnect_gap_ms"]) == (6000, 1000, 280)

    dtx = {(r["source"], r["dtx"]): r for r in normalize_dtx(_df(spark, "dtx", [
        '{"source": "speech", "dtx": true, "uplinkKbps": 18}',
        '{"source": "speech", "dtx": false, "uplinkKbps": 24}',
    ])).collect()}
    assert dtx[("speech", True)]["uplink_kbps"] == 18
    assert dtx[("speech", False)]["uplink_kbps"] == 24

    sens = {r["client"]: r for r in normalize_sensing(_df(spark, "sensing", [
        '{"scenario": "idle", "clients": {"wsn0": {"stats": {"suppressedPct": 91}}, '
        '"wsn1": {"stats": {"suppressedPct": 88}}}}'
    ])).collect()}
    assert sens["wsn0"]["scenario"] == "idle"
    assert sens["wsn0"]["suppressed_pct"] == 91
    assert sens["wsn1"]["suppressed_pct"] == 88
