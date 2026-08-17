"""Structural checks on the Airflow DAG without importing Airflow.

The DAG runs inside an Airflow deployment; here we parse its source with `ast`
so the orchestration graph — retry policy and task set — is guarded in CI
without pulling in the Airflow runtime.
"""
import ast
import os

DAG_PATH = os.path.join(
    os.path.dirname(__file__), "..", "orchestration", "airflow_dag.py"
)


def _module():
    with open(DAG_PATH, encoding="utf-8") as f:
        return ast.parse(f.read())


def _retries(tree):
    """Read DEFAULT_ARGS['retries'] from the AST (the dict also holds a
    timedelta call, so it is not a plain literal)."""
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "DEFAULT_ARGS" for t in node.targets
        ):
            for key, value in zip(node.value.keys, node.value.values):
                if isinstance(key, ast.Constant) and key.value == "retries":
                    return ast.literal_eval(value)
    return 0


def _task_ids(tree):
    ids = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "PythonOperator":
            for kw in node.keywords:
                if kw.arg == "task_id":
                    ids.append(ast.literal_eval(kw.value))
    return ids


def test_dag_retries_configured():
    assert _retries(_module()) >= 1


def test_dag_has_gx_and_parity_tasks():
    ids = set(_task_ids(_module()))
    assert {"land_bronze", "build_silver", "run_dbt",
            "gx_checkpoint", "parity", "render_figures"} <= ids
