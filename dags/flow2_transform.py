"""Flow 2 - Transform DAG.

Self-contained: transforms data in flow2_target_table.
No cross-DAG XCom dependencies.
"""
import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def transform_task_fn(**kwargs):
    """Transform: reverse data string in flow2_target_table."""
    hook = PostgresHook(postgres_conn_id="local_postgres")
    hook.run("""
        UPDATE flow2_target_table
        SET data = REVERSE(data)
        WHERE data IS DISTINCT FROM REVERSE(data)
    """)
    print("[flow2_transform] Transformed data (REVERSE) in flow2_target_table")


with DAG(
    dag_id="flow2_transform",
    description="Flow 2 - Transform",
    start_date=datetime.datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["flow2", "transform", "etl"],
) as dag:

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_task_fn,
    )
