"""Flow 2 - Load DAG.

Self-contained: final validation step.
"""
import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def load_task_fn(**kwargs):
    """Validate: log current state of flow2_target_table."""
    hook = PostgresHook(postgres_conn_id="local_postgres")
    row = hook.get_first("SELECT MAX(updated_at), COUNT(*) FROM flow2_target_table")
    print(f"[flow2_load] flow2_target_table: max_date={row[0]}, rows={row[1]}")


with DAG(
    dag_id="flow2_load",
    description="Flow 2 - Load (validation)",
    start_date=datetime.datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["flow2", "load", "etl"],
) as dag:

    load = PythonOperator(
        task_id="load",
        python_callable=load_task_fn,
    )
