from pyspark.sql.types import ArrayType, StructType

from pipeline.spark import schemas


def test_voice_summary_schema_shape():
    # Top-level envelope carries the run metadata and a perClient array.
    top = {f.name: f.dataType for f in schemas.VOICE_SUMMARY.fields}
    assert set(top) == {"exportedAt", "N", "holdMs", "perClient"}
    assert isinstance(top["perClient"], ArrayType)

    # Each client nests a KPI struct with the candidate mix aggregate.py reads.
    client = top["perClient"].elementType
    assert isinstance(client, StructType)
    client_fields = {f.name for f in client.fields}
    assert {"client", "pos", "peers", "kpis"} <= client_fields

    kpi_fields = {f.name for f in schemas.KPI.fields}
    assert {"setupMedianMs", "iceSuccessPct", "relayPct",
            "latRawMedianMs", "glareTotal", "candidateMix"} <= kpi_fields
    assert {f.name for f in schemas.CANDIDATE_MIX.fields} == {"host", "srflx", "relay"}
