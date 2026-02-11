"""Flow 1 - Transform DAG.

Self-contained: transforms data in flow1_target_table.
No cross-DAG XCom dependencies.
"""
import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def transform_task_fn(**kwargs):
    """Transform: uppercase data in flow1_target_table where not yet transformed."""
    hook = PostgresHook(postgres_conn_id="local_postgres")
    hook.run("""
        UPDATE flow1_target_table
        SET data = UPPER(data)
        WHERE data IS DISTINCT FROM UPPER(data)
    """)
    print("[flow1_transform] Transformed data (UPPER) in flow1_target_table")


with DAG(
    dag_id="flow1_transform",
    description="Flow 1 - Transform",
    start_date=datetime.datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["flow1", "transform", "etl"],
) as dag:

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_task_fn,
    )
