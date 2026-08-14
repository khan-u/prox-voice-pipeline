"""Silver jobs: read bronze Delta tables and materialize silver tables.

The link-fact job conforms the two link-bearing sources — realnet reports
(one row per link) and voice summaries (one row per client) — onto a single
flat fact table so the connection-level marts read one grain.
"""
import os

from pipeline.spark.transforms import (
    FACT_COLUMNS,
    explode_realnet_links,
    flatten_voice_kpis,
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
