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
    BooleanType,
    IntegerType,
    LongType,
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

# One connected link as reported by the real-network client reporter. This is
# the nested array that silver explodes to one fact row per link.
LINK = StructType([
    StructField("index", IntegerType()),
    StructField("detectedMs", IntegerType()),
    StructField("connectedMs", IntegerType()),
    StructField("setupMs", IntegerType()),
    StructField("failed", BooleanType()),
    StructField("candidateType", StringType()),
    StructField("rttMs", IntegerType()),
    StructField("latRawMs", IntegerType()),
    StructField("latCalMs", IntegerType()),
    StructField("glare", IntegerType()),
])

# One line of wsn-remote-reports.jsonl: a per-session real-network report keyed
# by condition tag (cond) and page-load session id (sid).
REMOTE_REPORT = StructType([
    StructField("cond", StringType()),
    StructField("sid", StringType()),
    StructField("ts", LongType()),
    StructField("peers", IntegerType()),
    StructField("kpis", KPI),
    StructField("links", ArrayType(LINK)),
    StructField("sense", SENSE),
])

# Silver fact grain: one row per connected link, flattened from either a
# voice-summary client or a realnet report. Nullable columns carry the
# dimension that a given source provides (client for local runs, condition/
# session for realnet).
SILVER_FACT = StructType([
    StructField("record_type", StringType()),
    StructField("source_file", StringType()),
    StructField("condition", StringType()),
    StructField("session_id", StringType()),
    StructField("n_peers", IntegerType()),
    StructField("client", StringType()),
    StructField("link_index", IntegerType()),
    StructField("setup_ms", IntegerType()),
    StructField("rtt_ms", IntegerType()),
    StructField("candidate_type", StringType()),
    StructField("lat_raw_ms", IntegerType()),
    StructField("glare", IntegerType()),
    StructField("ingested_at", StringType()),
])
