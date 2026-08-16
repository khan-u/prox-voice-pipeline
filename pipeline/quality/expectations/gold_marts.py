"""Great Expectations suites for the gold marts, plus a checkpoint.

Each mart gets a suite asserting its percentages stay in range and its
categorical columns hold known values. `run_checkpoint` validates a set of marts
together and reports per-mart success, the way a GX checkpoint bundles
validations before publish.
"""
import great_expectations as gx

from pipeline.quality.expectations.runner import run_suite


def kpi_vs_n_expectations():
    return [
        gx.expectations.ExpectColumnValuesToNotBeNull(column="n_peers"),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="ice_success_pct", min_value=0, max_value=100
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="m2e_ms", min_value=0, max_value=1000
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="setup_median_ms", min_value=0
        ),
    ]


def sensing_expectations():
    return [
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="scenario", value_set=["idle", "walk", "conv", "churn"]
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="suppression_median_pct", min_value=0, max_value=100
        ),
    ]


def realnet_expectations():
    return [
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="path", value_set=["direct", "relay"]
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="access", value_set=["cellular", "home", "other"]
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="rtt_median_ms", min_value=0, max_value=5000
        ),
    ]


# Mart name -> the expectations builder for its suite.
GOLD_SUITES = {
    "kpi_vs_n": kpi_vs_n_expectations,
    "sensing_suppression": sensing_expectations,
    "realnet_conditions": realnet_expectations,
}


def validate_mart(name, df):
    """Validate a single gold mart DataFrame against its suite."""
    return run_suite(df, f"gold_{name}", GOLD_SUITES[name]())


def run_checkpoint(marts):
    """Validate several gold marts together.

    `marts` maps a mart name (a key of GOLD_SUITES) to its DataFrame. Returns
    (per_mart_success: dict, overall_success: bool).
    """
    per_mart = {name: validate_mart(name, df).success for name, df in marts.items()}
    return per_mart, all(per_mart.values())
