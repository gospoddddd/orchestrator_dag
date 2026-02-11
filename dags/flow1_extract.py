"""Flow 1 - Extract DAG.

Self-contained: finds next missing date in source_data
that is not yet in flow1_target_table, and copies one day.
"""
import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def extract_task_fn(**kwargs):
    """Extract one day of data from source_data into flow1_target_table."""
    hook = PostgresHook(postgres_conn_id="local_postgres")

    row = hook.get_first("""
        SELECT s.data_date, s.data
        FROM source_data s
        WHERE s.data_date NOT IN (SELECT updated_at FROM flow1_target_table)
          AND s.data_date <= CURRENT_DATE - INTERVAL '1 day'
        ORDER BY s.data_date ASC
        LIMIT 1
    """)

    if row:
        data_date, data = row
        hook.run(
            "INSERT INTO flow1_target_table (updated_at, data) VALUES (%s, %s)",
            parameters=(str(data_date), data),
        )
        print(f"[flow1_extract] Loaded {data_date} into flow1_target_table")
    else:
        print("[flow1_extract] No new data to extract — all caught up")


with DAG(
    dag_id="flow1_extract",
    description="Flow 1 - Extract one day from source_data",
    start_date=datetime.datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["flow1", "extract", "etl"],
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_task_fn,
    )
