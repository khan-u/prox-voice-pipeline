"""Great Expectations suite for the silver link-fact table.

Guards the connection-level grain: known record types and candidate types, a
sensible cluster size, non-negative glare, and provenance always present.
"""
import great_expectations as gx

from pipeline.quality.expectations.runner import run_suite

SUITE = "silver_link_fact"


def expectations():
    return [
        gx.expectations.ExpectColumnValuesToNotBeNull(column="source_file"),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="record_type", value_set=["voice", "realnet"]
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="candidate_type", value_set=["host", "srflx", "relay"]
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="n_peers", min_value=0
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="glare", min_value=0
        ),
    ]


def validate(df):
    """Validate a silver link_fact DataFrame; return the GX result."""
    return run_suite(df, SUITE, expectations())
