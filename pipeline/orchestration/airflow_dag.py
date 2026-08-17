"""Airflow DAG for the PROX-VOICE medallion pipeline.

Wires the stages in dependency order: land raw telemetry to bronze, build the
silver tables, run dbt to materialize the gold marts, then render the figures.
Each task shells the same modules the local runs use, so the DAG is a thin
orchestration layer over tested code. Task callables import heavy libraries
lazily to keep DAG parsing fast.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    "owner": "prox-voice",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

# Gold marts validated by the GX checkpoint before publish.
GOLD_MARTS = ["kpi_vs_n", "sensing_suppression", "realnet_conditions"]


def _spark():
    import os
    import sys

    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    builder = (
        SparkSession.builder.appName("prox-voice-airflow")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def land_bronze(**_):
    from pipeline.common.config import load_config
    from pipeline.ingest.land_raw import land_json_exports, land_jsonl_stream

    cfg = load_config()
    spark = _spark()
    stamp = datetime.utcnow().isoformat()
    land_json_exports(spark, cfg["data_root"], cfg["bronze_root"], stamp)
    land_jsonl_stream(spark, cfg["data_root"], cfg["bronze_root"], stamp)


def build_silver(**_):
    from pipeline.common.config import load_config
    from pipeline.spark.bronze_to_silver import (
        build_experiment_tables,
        build_link_fact,
    )

    cfg = load_config()
    spark = _spark()
    build_link_fact(spark, cfg["bronze_root"], cfg["silver_root"])
    build_experiment_tables(spark, cfg["bronze_root"], cfg["silver_root"])


def run_dbt(**_):
    import subprocess

    subprocess.run(["dbt", "build"], cwd="pipeline/dbt", check=True)


def gx_checkpoint(**_):
    from pipeline.common.config import load_config
    from pipeline.quality.expectations.gold_marts import run_checkpoint

    cfg = load_config()
    spark = _spark()
    marts = {
        name: spark.read.format("delta").load(f"{cfg['gold_root']}/{name}")
        for name in GOLD_MARTS
    }
    _per_mart, overall = run_checkpoint(marts)
    if not overall:
        raise ValueError(f"GX checkpoint failed: {_per_mart}")


def parity(**_):
    # Imported lazily: the parity gate lands in its own module and is not needed
    # to parse or run the rest of the DAG.
    from pipeline.validate.parity_check import run_parity

    ok, report = run_parity()
    if not ok:
        raise ValueError(f"parity gate failed: {report}")


def render_figures(**_):
    from pipeline.figures import (
        fig02_setup_vs_n,
        fig03_glare,
        fig04_m2e,
        fig05_uplink,
        fig06_suppression,
        fig07_mobility,
        fig08_audiogap,
        fig09_realnet_rtt,
        fig10_realnet_setup,
        fig11_energy,
        fig12_grace_window,
    )

    for module in [
        fig02_setup_vs_n, fig03_glare, fig04_m2e, fig05_uplink, fig06_suppression,
        fig07_mobility, fig08_audiogap, fig09_realnet_rtt, fig10_realnet_setup,
        fig11_energy, fig12_grace_window,
    ]:
        module.main()


with DAG(
    dag_id="prox_voice_pipeline",
    description="PROX-VOICE medallion pipeline: bronze -> silver -> gold -> figures",
    default_args=DEFAULT_ARGS,
    schedule=None,
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["prox-voice", "medallion"],
) as dag:
    t_bronze = PythonOperator(task_id="land_bronze", python_callable=land_bronze)
    t_silver = PythonOperator(task_id="build_silver", python_callable=build_silver)
    t_dbt = PythonOperator(task_id="run_dbt", python_callable=run_dbt)
    t_gx = PythonOperator(task_id="gx_checkpoint", python_callable=gx_checkpoint)
    t_parity = PythonOperator(task_id="parity", python_callable=parity)
    t_figures = PythonOperator(task_id="render_figures", python_callable=render_figures)

    # Gold must pass the GX checkpoint and the parity gate before figures publish.
    t_bronze >> t_silver >> t_dbt >> t_gx >> t_parity >> t_figures
