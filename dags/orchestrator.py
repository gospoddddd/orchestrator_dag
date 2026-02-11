"""Universal ETL Orchestrator — single shared DAG for all pipelines.

Each run triggers exactly ONE ETL DAG, then self-triggers to continue.
The conf tracks: {"pipeline": "flow1", "iteration": 0, "etl_step": 0}

Flow per run:
  check_freshness → branch
    → trigger_etl → retry           (not fresh, triggers one ETL DAG then self-retries)
    → trigger_next                   (fresh, triggers next pipeline or external DAG)
    → done                           (fresh, no next pipeline)
    → max_iterations_reached         (safety stop)

The etl_step cycles through on_not_fresh.trigger_dags:
  step 0 → trigger ETL[0], retry with step=1
  step 1 → trigger ETL[1], retry with step=2
  step N (last) → trigger ETL[N], retry with step=0, iteration+1
  step 0 (next iter) → check freshness again → repeat or finish

XCom keys pushed by check_freshness (available for Jinja in conf):
  next_date, max_dest_date, max_source_date, source_table,
  source_date_column, dest_table, dest_date_column, is_fresh, iteration

Usage:
  # Manual trigger
  airflow dags trigger orchestrator --conf '{"pipeline": "flow1"}'

  # Scheduled: starts with settings.default_pipeline automatically
"""
import datetime

import yaml
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONFIG_PATH = "/opt/airflow/config/pipelines.yaml"
DAG_ID = "orchestrator"


def _load_config():
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


# Read config at parse time for DAG-level settings (schedule, timeout)
_config = _load_config()
_settings = _config.get("settings", {})


def check_freshness_fn(**kwargs):
    """Check data freshness and push all metadata to XCom.

    When etl_step > 0, skip the DB check (we're mid-ETL-chain, not fresh by definition).
    When etl_step == 0, query source vs dest tables.
    """
    config = _load_config()
    pipelines = config.get("pipelines", {})
    settings = config.get("settings", {})

    conf = kwargs["dag_run"].conf or {}
    pipeline_name = conf.get("pipeline", settings.get("default_pipeline"))
    if not pipeline_name:
        pipeline_name = list(pipelines.keys())[0]

    iteration = int(conf.get("iteration", 0))
    etl_step = int(conf.get("etl_step", 0))

    pipeline_cfg = pipelines[pipeline_name]
    freshness = pipeline_cfg["freshness_check"]
    etl_dags = pipeline_cfg.get("on_not_fresh", {}).get("trigger_dags", [])
    on_fresh = pipeline_cfg.get("on_fresh", {})
    max_iterations = pipeline_cfg.get(
        "max_iterations", settings.get("default_max_iterations", 30)
    )

    src_tbl = freshness["source_table"]
    src_col = freshness["source_date_column"]
    dst_tbl = freshness["dest_table"]
    dst_col = freshness["dest_date_column"]

    # --- Freshness check (only at step 0) ---
    is_fresh = False
    max_source_date = None
    max_dest_date = None
    next_date = None

    if etl_step == 0:
        hook = PostgresHook(postgres_conn_id=freshness["conn_id"])

        src_row = hook.get_first(
            f"SELECT MAX({src_col}) FROM {src_tbl} "
            f"WHERE {src_col} <= CURRENT_DATE - INTERVAL '1 day'"
        )
        max_source_date = src_row[0] if src_row and src_row[0] else None

        dst_row = hook.get_first(f"SELECT MAX({dst_col}) FROM {dst_tbl}")
        max_dest_date = dst_row[0] if dst_row and dst_row[0] else None

        if isinstance(max_source_date, datetime.datetime):
            max_source_date = max_source_date.date()
        if isinstance(max_dest_date, datetime.datetime):
            max_dest_date = max_dest_date.date()

        if max_source_date is None:
            is_fresh = True
        elif max_dest_date is not None:
            is_fresh = max_dest_date >= max_source_date

        if not is_fresh:
            if max_dest_date:
                next_date = str(max_dest_date + datetime.timedelta(days=1))
            else:
                first = hook.get_first(f"SELECT MIN({src_col}) FROM {src_tbl}")
                next_date = str(first[0]) if first and first[0] else None

    # --- Determine current ETL DAG to trigger ---
    current_etl_dag = etl_dags[etl_step] if etl_step < len(etl_dags) else None

    # --- Determine what retry looks like ---
    if etl_step + 1 < len(etl_dags):
        next_etl_step = etl_step + 1
        next_iteration = iteration
    else:
        next_etl_step = 0
        next_iteration = iteration + 1

    # --- Determine on_fresh target ---
    next_pipeline = on_fresh.get("next_pipeline")
    external_trigger_dag = on_fresh.get("trigger_dag")

    if next_pipeline:
        next_trigger_dag = DAG_ID
        next_trigger_conf = next_pipeline
    elif external_trigger_dag:
        next_trigger_dag = external_trigger_dag
        next_trigger_conf = ""
    else:
        next_trigger_dag = None
        next_trigger_conf = ""

    # --- Push everything to XCom ---
    ti = kwargs["ti"]
    ti.xcom_push(key="pipeline_name", value=pipeline_name)
    ti.xcom_push(key="is_fresh", value=is_fresh)
    ti.xcom_push(key="iteration", value=iteration)
    ti.xcom_push(key="etl_step", value=etl_step)
    ti.xcom_push(key="max_iterations", value=max_iterations)
    ti.xcom_push(key="current_etl_dag", value=current_etl_dag)
    ti.xcom_push(key="next_etl_step", value=next_etl_step)
    ti.xcom_push(key="next_iteration", value=next_iteration)
    ti.xcom_push(key="next_trigger_dag", value=next_trigger_dag)
    ti.xcom_push(key="next_trigger_conf", value=next_trigger_conf)
    ti.xcom_push(key="max_dest_date", value=str(max_dest_date) if max_dest_date else None)
    ti.xcom_push(key="max_source_date", value=str(max_source_date) if max_source_date else None)
    ti.xcom_push(key="next_date", value=next_date)
    ti.xcom_push(key="source_table", value=src_tbl)
    ti.xcom_push(key="source_date_column", value=src_col)
    ti.xcom_push(key="dest_table", value=dst_tbl)
    ti.xcom_push(key="dest_date_column", value=dst_col)

    print(
        f"[{DAG_ID}] pipeline={pipeline_name} iter={iteration}/{max_iterations} "
        f"step={etl_step}/{len(etl_dags)} fresh={is_fresh} "
        f"src_max={max_source_date} dst_max={max_dest_date} next_date={next_date}"
    )


def branch_fn(**kwargs):
    """Route: trigger_etl / trigger_next / done / max_iterations_reached."""
    ti = kwargs["ti"]
    is_fresh = ti.xcom_pull(task_ids="check_freshness", key="is_fresh")
    etl_step = ti.xcom_pull(task_ids="check_freshness", key="etl_step")
    iteration = ti.xcom_pull(task_ids="check_freshness", key="iteration")
    max_iterations = ti.xcom_pull(task_ids="check_freshness", key="max_iterations")
    current_etl_dag = ti.xcom_pull(task_ids="check_freshness", key="current_etl_dag")
    next_trigger_dag = ti.xcom_pull(task_ids="check_freshness", key="next_trigger_dag")

    # Mid-chain (etl_step > 0): always continue ETL
    if etl_step > 0 and current_etl_dag:
        print(f"Mid-chain step={etl_step} → trigger_etl ({current_etl_dag})")
        return "trigger_etl"

    # Step 0: freshness was checked
    if is_fresh:
        if next_trigger_dag:
            print(f"Fresh → trigger_next ({next_trigger_dag})")
            return "trigger_next"
        print("Fresh → done")
        return "done"

    # Not fresh
    if iteration + 1 >= max_iterations:
        print(f"NOT fresh, max iterations ({max_iterations}) reached.")
        return "max_iterations_reached"

    if current_etl_dag:
        print(f"NOT fresh → trigger_etl ({current_etl_dag})")
        return "trigger_etl"

    print("No ETL DAGs configured → done")
    return "done"


# --- DAG Definition ---

with DAG(
    dag_id=DAG_ID,
    description="Universal ETL Orchestrator",
    start_date=datetime.datetime(2024, 1, 1),
    schedule=_settings.get("schedule"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=datetime.timedelta(
        minutes=_settings.get("default_timeout_minutes", 300)
    ),
    tags=["orchestrator", "etl"],
) as dag:

    check_freshness = PythonOperator(
        task_id="check_freshness",
        python_callable=check_freshness_fn,
    )

    branch = BranchPythonOperator(
        task_id="branch",
        python_callable=branch_fn,
    )

    # --- Not-fresh path: trigger ONE ETL DAG, then self-retry ---
    trigger_etl = TriggerDagRunOperator(
        task_id="trigger_etl",
        trigger_dag_id="{{ ti.xcom_pull(task_ids='check_freshness', key='current_etl_dag') }}",
        conf={
            "next_date": "{{ ti.xcom_pull(task_ids='check_freshness', key='next_date') }}",
            "max_dest_date": "{{ ti.xcom_pull(task_ids='check_freshness', key='max_dest_date') }}",
            "max_source_date": "{{ ti.xcom_pull(task_ids='check_freshness', key='max_source_date') }}",
            "source_table": "{{ ti.xcom_pull(task_ids='check_freshness', key='source_table') }}",
            "source_date_column": "{{ ti.xcom_pull(task_ids='check_freshness', key='source_date_column') }}",
            "dest_table": "{{ ti.xcom_pull(task_ids='check_freshness', key='dest_table') }}",
            "dest_date_column": "{{ ti.xcom_pull(task_ids='check_freshness', key='dest_date_column') }}",
            "iteration": "{{ ti.xcom_pull(task_ids='check_freshness', key='iteration') }}",
            "pipeline": "{{ ti.xcom_pull(task_ids='check_freshness', key='pipeline_name') }}",
        },
        wait_for_completion=True,
        poke_interval=30,
        deferrable=False,
    )

    retry = TriggerDagRunOperator(
        task_id="retry",
        trigger_dag_id=DAG_ID,
        conf={
            "pipeline": "{{ ti.xcom_pull(task_ids='check_freshness', key='pipeline_name') }}",
            "iteration": "{{ ti.xcom_pull(task_ids='check_freshness', key='next_iteration') }}",
            "etl_step": "{{ ti.xcom_pull(task_ids='check_freshness', key='next_etl_step') }}",
        },
        wait_for_completion=False,
        reset_dag_run=False,
        deferrable=False,
    )

    # --- Fresh path: trigger next pipeline/DAG or done ---
    trigger_next = TriggerDagRunOperator(
        task_id="trigger_next",
        trigger_dag_id="{{ ti.xcom_pull(task_ids='check_freshness', key='next_trigger_dag') }}",
        conf={
            "pipeline": "{{ ti.xcom_pull(task_ids='check_freshness', key='next_trigger_conf') }}",
        },
        wait_for_completion=False,
        reset_dag_run=False,
        deferrable=False,
    )

    done = EmptyOperator(task_id="done")
    max_reached = EmptyOperator(task_id="max_iterations_reached")

    # --- Wiring ---
    check_freshness >> branch
    branch >> trigger_etl >> retry
    branch >> trigger_next
    branch >> done
    branch >> max_reached
