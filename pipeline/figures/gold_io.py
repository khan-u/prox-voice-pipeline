"""Read a gold Delta mart into plain Python rows for the figure scripts.

The figures keep their plotting logic pure (a `plot(rows)` that takes dict rows
and returns a matplotlib Figure), so only this helper touches Spark. It builds a
short-lived local Delta session and returns the mart as a list of dicts sorted
for stable plotting.
"""
import os
import sys

DEFAULT_GOLD_DB = os.environ.get(
    "PIPELINE_GOLD_DB", "pipeline/dbt/spark-warehouse/gold.db"
)

_JDK_17 = "/Library/Java/JavaVirtualMachines/jdk-17.jdk/Contents/Home"


def _session():
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    if "JAVA_HOME" not in os.environ and os.path.exists(_JDK_17):
        os.environ["JAVA_HOME"] = _JDK_17
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("prox-voice-figures")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.ui.enabled", "false")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    return session


def read_gold(name, order_by=None, gold_db=DEFAULT_GOLD_DB):
    """Return the gold mart `name` as a list of dict rows."""
    spark = _session()
    df = spark.read.format("delta").load(os.path.join(gold_db, name))
    if order_by:
        df = df.orderBy(order_by)
    return [row.asDict() for row in df.collect()]
