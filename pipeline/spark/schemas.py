"""Explicit Spark schemas for PROX-VOICE telemetry.

Reading raw JSON with an explicit schema (rather than inference) keeps the
bronze/silver types stable across runs and rejects records whose shape drifts.
The field set mirrors what VoiceMetrics exports and what aggregate.py reads:
per-client KPI blocks with a candidate-type mix, wrapped in a voice-summary
envelope. Nested link arrays and the silver fact schema are added alongside the
transforms that consume them.
"""
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ICE candidate-type histogram: host / server-reflexive / relay counts.
CANDIDATE_MIX = StructType([
    StructField("host", IntegerType()),
    StructField("srflx", IntegerType()),
    StructField("relay", IntegerType()),
])

# Per-link KPI rollup exported by VoiceMetrics. Calibrated-latency fields carry
# -1 when no device constant is set, so they stay signed integers.
KPI = StructType([
    StructField("links", IntegerType()),
    StructField("iceAttempts", IntegerType()),
    StructField("iceSuccessPct", IntegerType()),
    StructField("setupMedianMs", IntegerType()),
    StructField("setupP95Ms", IntegerType()),
    StructField("relayPct", IntegerType()),
    StructField("candidateMix", CANDIDATE_MIX),
    StructField("latRawMedianMs", IntegerType()),
    StructField("latRawP95Ms", IntegerType()),
    StructField("latCalMedianMs", IntegerType()),
    StructField("latCalP95Ms", IntegerType()),
    StructField("glareTotal", IntegerType()),
])

POS = StructType([
    StructField("x", IntegerType()),
    StructField("y", IntegerType()),
])

# Optional sensing rollup carried on a client when the run also profiled the
# sensor layer (aggregate.py reads c["sense"]["suppressedPct"]).
SENSE = StructType([
    StructField("suppressedPct", IntegerType()),
])

# One connected client within a voice-summary export.
PER_CLIENT = StructType([
    StructField("client", StringType()),
    StructField("pos", POS),
    StructField("peers", IntegerType()),
    StructField("kpis", KPI),
    StructField("meanUpKbps", IntegerType()),
    StructField("sense", SENSE),
])

# Top-level voice-summary envelope: wsn-voice-N*-summary-*.json.
VOICE_SUMMARY = StructType([
    StructField("exportedAt", StringType()),
    StructField("N", IntegerType()),
    StructField("holdMs", IntegerType()),
    StructField("perClient", ArrayType(PER_CLIENT)),
])
