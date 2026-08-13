"""Shared pytest fixtures for the pipeline tests.

Provides a session-scoped local SparkSession with Delta Lake configured, so the
Spark transform and job tests run against a real engine without a cluster.
Spark 3.5 runs on Java 8/11/17; if a newer default JDK is active it aborts the
JVM at startup, so we pin JAVA_HOME to a supported release when one is present.
"""
import os
import sys
from pathlib import Path

import pytest

# Candidate JDK homes in preference order; the first that exists wins. Override
# with PIPELINE_JAVA_HOME to point at any Spark-compatible JDK.
_JDK_CANDIDATES = [
    os.environ.get("PIPELINE_JAVA_HOME"),
    "/Library/Java/JavaVirtualMachines/jdk-17.jdk/Contents/Home",
    "/Library/Java/JavaVirtualMachines/amazon-corretto-11.jdk/Contents/Home",
    "/usr/lib/jvm/java-17-openjdk-amd64",
]


def _pin_supported_jdk():
    for home in _JDK_CANDIDATES:
        if home and Path(home).exists():
            os.environ["JAVA_HOME"] = home
            return home
    return None


@pytest.fixture(scope="session")
def spark():
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    _pin_supported_jdk()
    # Spark launches worker processes with PYSPARK_PYTHON (default: the system
    # `python3`). Pin both driver and worker to this interpreter so their minor
    # versions match — a mismatch aborts every task.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("prox-voice-pipeline-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
