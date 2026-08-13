from pyspark.sql.types import ArrayType

from pipeline.spark import schemas


def test_remote_report_nests_link_array():
    top = {f.name: f.dataType for f in schemas.REMOTE_REPORT.fields}
    assert {"cond", "sid", "ts", "kpis", "links"} <= set(top)
    assert isinstance(top["links"], ArrayType)
    link_fields = {f.name for f in top["links"].elementType.fields}
    assert {"setupMs", "rttMs", "candidateType"} <= link_fields


def test_silver_fact_is_one_row_per_link_grain():
    fields = {f.name for f in schemas.SILVER_FACT.fields}
    # dimension columns that trace a link back to its source and condition
    assert {"record_type", "source_file", "condition", "session_id",
            "n_peers", "client", "link_index"} <= fields
    # the measured columns aggregate.py reduces
    assert {"setup_ms", "rtt_ms", "candidate_type", "glare"} <= fields
