"""Command-line driver for the PROX-VOICE pipeline stages.

Resolves the lake roots to absolute paths under the repo so the stages run from
any working directory, builds a local Delta Spark session, and dispatches to the
bronze / silver / figures / parity steps. dbt is run separately (see the
Makefile) since it manages its own session.

Usage:  python run_pipeline.py {bronze|silver|figures|parity|all}
"""
import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DEFAULT_JDK = "/Library/Java/JavaVirtualMachines/jdk-17.jdk/Contents/Home"


def _abs(root):
    return str((REPO / root).resolve())


def _config():
    sys.path.insert(0, str(REPO))
    from pipeline.common.config import load_config

    cfg = load_config()
    for key in ("data_root", "bronze_root", "silver_root", "gold_root"):
        cfg[key] = _abs(cfg[key])
    # Local runs read gold from the dbt-managed warehouse (schema `gold`), which
    # is where `make dbt` materializes the marts.
    if cfg.get("mode", "local") == "local":
        cfg["gold_root"] = _abs("pipeline/dbt/spark-warehouse/gold.db")
    return cfg


def _spark():
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    if "JAVA_HOME" not in os.environ and os.path.exists(DEFAULT_JDK):
        os.environ["JAVA_HOME"] = DEFAULT_JDK
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("prox-voice-pipeline")
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


def stage_bronze(spark, cfg):
    from pipeline.ingest.land_raw import land_json_exports, land_jsonl_stream

    stamp = datetime.now(timezone.utc).isoformat()
    n = land_json_exports(spark, cfg["data_root"], cfg["bronze_root"], stamp)
    m = land_jsonl_stream(spark, cfg["data_root"], cfg["bronze_root"], stamp)
    print(f"bronze: landed {n} json exports + {m} stream rows")


def stage_silver(spark, cfg):
    from pipeline.spark.bronze_to_silver import (
        build_experiment_tables,
        build_link_fact,
    )

    facts = build_link_fact(spark, cfg["bronze_root"], cfg["silver_root"])
    tables = build_experiment_tables(spark, cfg["bronze_root"], cfg["silver_root"])
    print(f"silver: link_fact={facts} rows; tables={tables}")


def stage_figures(cfg):
    os.environ["PIPELINE_GOLD_DB"] = cfg["gold_root"]
    from pipeline.figures import (
        fig02_setup_vs_n, fig03_glare, fig04_m2e, fig05_uplink, fig06_suppression,
        fig07_mobility, fig08_audiogap, fig09_realnet_rtt, fig10_realnet_setup,
        fig11_energy, fig12_grace_window,
    )

    for module in [
        fig02_setup_vs_n, fig03_glare, fig04_m2e, fig05_uplink, fig06_suppression,
        fig07_mobility, fig08_audiogap, fig09_realnet_rtt, fig10_realnet_setup,
        fig11_energy, fig12_grace_window,
    ]:
        module.main()


def stage_parity(spark, cfg):
    from pipeline.validate.parity_check import run_parity

    ok, report = run_parity(spark, cfg["gold_root"], cfg["data_root"])
    for row in report:
        mark = "ok  " if row["ok"] else "FAIL"
        print(f"  {mark} {row['check']}: expected {row['expected']} got {row['actual']}")
    if not ok:
        raise SystemExit("parity gate failed")
    print(f"parity: {len(report)} checks passed")


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("stage", choices=["bronze", "silver", "figures", "parity", "all"])
    args = parser.parse_args(argv)

    cfg = _config()
    if args.stage == "figures":
        stage_figures(cfg)
        return
    spark = _spark()
    if args.stage in ("bronze", "all"):
        stage_bronze(spark, cfg)
    if args.stage in ("silver", "all"):
        stage_silver(spark, cfg)
    if args.stage == "parity":
        stage_parity(spark, cfg)


if __name__ == "__main__":
    main()
