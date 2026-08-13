"""Bronze ingestion: land raw telemetry into Delta with provenance columns.

Bronze keeps the payload intact — the whole-file JSON text is stored verbatim
alongside where it came from (source_file, record_type, peer_count, ingested_at).
Parsing, exploding, and normalizing happen downstream in silver, so a schema
change never forces a re-land of the raw data.
"""
import os

from pyspark.sql import functions as F

from pipeline.common.records import classify, peer_count


def discover_exports(names):
    """Return the whole-file JSON exports to land, in stable order.

    Pure: filters a list of filenames to the per-run `wsn-*.json` exports,
    dropping the append-only `.jsonl` stream (landed separately) and anything
    that is not a recognized telemetry file (e.g. the energy-screenshots dir).
    """
    keep = []
    for name in names:
        if not name.endswith(".json"):
            continue
        try:
            classify(name)
        except ValueError:
            continue
        keep.append(name)
    return sorted(keep)


def land_json_exports(spark, data_root, bronze_root, ingested_at):
    """Land every JSON export under data_root into a bronze Delta table.

    One row per source file: the raw JSON text plus provenance columns. Returns
    the number of files landed.
    """
    names = discover_exports(os.listdir(data_root))
    frames = []
    for name in names:
        path = os.path.join(data_root, name)
        df = (
            spark.read.text(path, wholetext=True)
            .withColumnRenamed("value", "payload")
            .withColumn("source_file", F.lit(name))
            .withColumn("record_type", F.lit(classify(name)))
            .withColumn("peer_count", F.lit(peer_count(name)))
            .withColumn("ingested_at", F.lit(ingested_at))
        )
        frames.append(df)
    if not frames:
        return 0
    bronze = frames[0]
    for df in frames[1:]:
        bronze = bronze.unionByName(df)
    (
        bronze.write.format("delta")
        .mode("overwrite")
        .save(os.path.join(bronze_root, "raw_json"))
    )
    return len(names)
