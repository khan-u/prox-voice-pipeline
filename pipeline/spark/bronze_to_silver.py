"""Silver jobs: read bronze Delta tables and materialize silver tables.

The link-fact job conforms the two link-bearing sources — realnet reports
(one row per link) and voice summaries (one row per client) — onto a single
flat fact table so the connection-level marts read one grain.
"""
import os

from pyspark.sql import functions as F

from pipeline.spark.schemas import VOICE_SUMMARY
from pipeline.spark.transforms import (
    FACT_COLUMNS,
    explode_realnet_links,
    flatten_voice_kpis,
    normalize_audiogap,
    normalize_dtx,
    normalize_gracegap,
    normalize_mobility,
    normalize_sensing,
)


def _read_delta(spark, root, name):
    return spark.read.format("delta").load(os.path.join(root, name))


def build_link_fact(spark, bronze_root, silver_root):
    """Union realnet link facts and voice client facts into silver link_fact.

    Returns the row count written.
    """
    raw_json = _read_delta(spark, bronze_root, "raw_json")
    raw_stream = _read_delta(spark, bronze_root, "raw_stream")

    voice = flatten_voice_kpis(raw_json).select(*FACT_COLUMNS)
    realnet = explode_realnet_links(raw_stream).select(*FACT_COLUMNS)
    fact = voice.unionByName(realnet)

    out = os.path.join(silver_root, "link_fact")
    fact.write.format("delta").mode("overwrite").save(out)
    return spark.read.format("delta").load(out).count()


def _voice_client_summary(raw_json):
    """Rich per-client voice KPIs the vs-N marts need (ICE%, relay%, uplink,
    suppression) — wider than the link-fact grain, one row per client."""
    voice = raw_json.where(F.col("record_type") == "voice")
    p = voice.withColumn("s", F.from_json("payload", VOICE_SUMMARY))
    c = p.withColumn("c", F.explode("s.perClient"))
    return c.select(
        F.col("source_file"),
        F.col("s.N").alias("n_peers"),
        F.col("c.client").alias("client"),
        F.col("c.peers").alias("peers"),
        F.col("c.kpis.links").alias("links"),
        F.col("c.kpis.setupMedianMs").alias("setup_median_ms"),
        F.col("c.kpis.setupP95Ms").alias("setup_p95_ms"),
        F.col("c.kpis.iceSuccessPct").alias("ice_success_pct"),
        F.col("c.kpis.relayPct").alias("relay_pct"),
        F.col("c.kpis.latRawMedianMs").alias("lat_raw_median_ms"),
        F.col("c.kpis.glareTotal").alias("glare_total"),
        F.col("c.meanUpKbps").alias("mean_up_kbps"),
        F.col("c.sense.suppressedPct").alias("suppressed_pct"),
        F.col("ingested_at"),
    )


def build_experiment_tables(spark, bronze_root, silver_root):
    """Materialize one silver Delta table per experiment from bronze raw_json.

    Returns the list of table names written.
    """
    raw_json = _read_delta(spark, bronze_root, "raw_json")
    tables = {
        "voice_client": _voice_client_summary(raw_json),
        "mobility": normalize_mobility(raw_json),
        "audiogap": normalize_audiogap(raw_json),
        "gracegap": normalize_gracegap(raw_json),
        "dtx": normalize_dtx(raw_json),
        "sensing": normalize_sensing(raw_json),
    }
    for name, df in tables.items():
        df.write.format("delta").mode("overwrite").save(
            os.path.join(silver_root, name)
        )
    return list(tables)
