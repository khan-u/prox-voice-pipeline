"""Silver transforms: parse bronze payloads and reshape to the fact grain.

The headline move is nested -> columnar: the realnet stream carries a links[]
array per report, and silver explodes it to one row per connected link so every
downstream mart works on a flat fact table. Later transforms flatten the
voice-summary KPI blocks and normalize the per-experiment records onto the same
grain.
"""
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pipeline.spark.schemas import REMOTE_REPORT, VOICE_SUMMARY

# Column order of the silver link-fact grain (mirrors schemas.SILVER_FACT).
FACT_COLUMNS = [
    "record_type", "source_file", "condition", "session_id", "n_peers",
    "client", "link_index", "setup_ms", "rtt_ms", "candidate_type",
    "lat_raw_ms", "glare", "ingested_at",
]


def normalize_candidate(mix):
    """Collapse a candidate-mix histogram to the effective connection type.

    WebRTC reports host/srflx/relay candidate counts; the effective path is the
    highest tier actually used (a relay pair beats server-reflexive beats host).
    Priority relay > srflx > host, matching how relayPct is reported.
    """
    return (
        F.when(mix["relay"] > 0, F.lit("relay"))
        .when(mix["srflx"] > 0, F.lit("srflx"))
        .otherwise(F.lit("host"))
    )


def explode_realnet_links(stream_df):
    """Explode the realnet bronze stream into one fact row per connected link.

    Input is the bronze `raw_stream` table (payload + provenance columns). A
    session (cond, sid) emits periodic reports; only its latest report (max ts)
    contributes, matching the reducer. Each link in that report becomes one row;
    a report with no links yields a single null-link row (explode_outer) so it
    is not silently lost.
    """
    parsed = stream_df.withColumn("r", F.from_json("payload", REMOTE_REPORT))
    latest = Window.partitionBy("r.cond", "r.sid").orderBy(
        F.col("r.ts").desc_nulls_last()
    )
    newest = (
        parsed.withColumn("_rn", F.row_number().over(latest))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )
    exploded = newest.withColumn("link", F.explode_outer("r.links"))
    return exploded.select(
        F.col("record_type"),
        F.col("source_file"),
        F.col("r.cond").alias("condition"),
        F.col("r.sid").alias("session_id"),
        F.col("r.peers").alias("n_peers"),
        F.lit(None).cast("string").alias("client"),
        F.col("link.index").alias("link_index"),
        F.col("link.setupMs").alias("setup_ms"),
        F.col("link.rttMs").alias("rtt_ms"),
        F.col("link.candidateType").alias("candidate_type"),
        F.col("link.latRawMs").alias("lat_raw_ms"),
        F.col("link.glare").alias("glare"),
        F.col("ingested_at"),
    )


def flatten_voice_kpis(json_df):
    """Flatten voice-summary per-client KPI blocks onto the link-fact grain.

    Local N-client runs report a per-client KPI rollup rather than a links[]
    array, so each client contributes one fact row: its median setup/latency,
    its glare total, and a candidate type normalized from the candidate mix.
    link_index is null (client grain); condition/session are null (local run).
    """
    voice = json_df.where(F.col("record_type") == "voice")
    parsed = voice.withColumn("s", F.from_json("payload", VOICE_SUMMARY))
    client = parsed.withColumn("c", F.explode("s.perClient"))
    return client.select(
        F.col("record_type"),
        F.col("source_file"),
        F.lit(None).cast("string").alias("condition"),
        F.lit(None).cast("string").alias("session_id"),
        F.col("s.N").alias("n_peers"),
        F.col("c.client").alias("client"),
        F.lit(None).cast("int").alias("link_index"),
        F.col("c.kpis.setupMedianMs").alias("setup_ms"),
        F.lit(None).cast("int").alias("rtt_ms"),
        normalize_candidate(F.col("c.kpis.candidateMix")).alias("candidate_type"),
        F.col("c.kpis.latRawMedianMs").alias("lat_raw_ms"),
        F.col("c.kpis.glareTotal").alias("glare"),
        F.col("ingested_at"),
    )
