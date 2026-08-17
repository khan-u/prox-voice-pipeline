"""Parity gate: the gold marts must agree with an independent reduction.

aggregate.py is the reference reducer for the study. Here we recompute its
headline numbers straight from the raw JSON with plain Python — a second,
independent implementation — and assert the Spark/dbt gold marts match within a
rounding tolerance. A disagreement means the pipeline drifted from the oracle and
is surfaced, never silently reconciled.
"""
import glob
import json
import os
import statistics as st

TOL = 1  # medians differ by at most 1 ms/pct from half-up vs banker's rounding.


def _median(values):
    return round(st.median(values)) if values else None


def compare_values(expected, actual, tol=TOL):
    """True if both are present and within tol, or both absent."""
    if expected is None and actual is None:
        return True
    if expected is None or actual is None:
        return False
    return abs(expected - actual) <= tol


# --- Reference reductions (independent of the Spark pipeline) ---------------

def reference_dtx(records):
    """{(source, dtx_on): median uplink} from raw dtx records."""
    groups = {}
    for d in records:
        if d.get("uplinkKbps", -1) >= 0:
            groups.setdefault((d["source"], bool(d["dtx"])), []).append(d["uplinkKbps"])
    return {k: _median(v) for k, v in groups.items()}


def reference_sensing(records):
    """{scenario: median suppressedPct} from raw sensing records."""
    groups = {}
    for d in records:
        sc = d.get("scenario")
        if not sc:
            continue
        for c in (d.get("clients") or {}).values():
            pct = (c.get("stats") or {}).get("suppressedPct")
            if isinstance(pct, (int, float)):
                groups.setdefault(sc, []).append(pct)
    return {sc: _median(v) for sc, v in groups.items()}


def _load(data_root, pattern):
    out = []
    for f in glob.glob(os.path.join(data_root, pattern)):
        with open(f, encoding="utf-8") as fh:
            out.append(json.load(fh))
    return out


def run_parity(spark, gold_db, data_root):
    """Compare gold dtx_uplink and sensing_suppression against the reference.

    Returns (ok, report) where report lists each check with expected/actual.
    """
    report = []

    ref_dtx = reference_dtx(_load(data_root, "wsn-dtx-*.json"))
    gold_dtx = {
        (r["source"], bool(r["dtx"])): r["uplink_median_kbps"]
        for r in spark.read.format("delta").load(f"{gold_db}/dtx_uplink").collect()
    }
    for key, expected in ref_dtx.items():
        actual = gold_dtx.get(key)
        report.append({
            "check": f"dtx_uplink{key}", "expected": expected, "actual": actual,
            "ok": compare_values(expected, actual),
        })

    ref_sens = reference_sensing(_load(data_root, "wsn-sensing-*.json"))
    gold_sens = {
        r["scenario"]: r["suppression_median_pct"]
        for r in spark.read.format("delta").load(f"{gold_db}/sensing_suppression").collect()
    }
    for scenario, expected in ref_sens.items():
        actual = gold_sens.get(scenario)
        report.append({
            "check": f"sensing[{scenario}]", "expected": expected, "actual": actual,
            "ok": compare_values(expected, actual),
        })

    ok = all(row["ok"] for row in report)
    return ok, report
