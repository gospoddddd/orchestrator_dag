# CLAUDE.md

use only english language for economy tokens
This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

---

## 1. Project Overview

A universal, configuration-driven ETL orchestration system built on **Apache Airflow 3.1.7**. A single `orchestrator` DAG manages any number of ETL pipelines defined in YAML. It automatically checks data freshness, triggers ETL DAGs when data is stale, retries until data is caught up, and chains pipelines together.

**Stack:** Airflow 3.1.7 | CeleryExecutor | PostgreSQL 16 | Redis 7.2 | Docker Compose

---

## 2. Quick Reference Commands

```bash
# --- Lifecycle ---
docker compose up -d                    # start all services
docker compose down                     # stop all services
docker compose restart airflow-worker   # restart a single service

# --- DAG Operations ---
docker compose exec airflow-apiserver airflow dags list
docker compose exec airflow-apiserver airflow dags unpause orchestrator
docker compose exec airflow-apiserver airflow dags trigger orchestrator \
  --conf '{"pipeline": "flow1"}'

# --- Logs & Debugging ---
docker compose logs airflow-dag-processor --tail 30   # check parse errors
docker compose logs airflow-worker --tail 50          # check task execution
docker compose logs -f airflow-scheduler              # follow scheduler

# --- Database ---
docker compose exec postgres psql -U airflow -d airflow -c "<SQL>"
```

**Airflow UI:** http://localhost:8080 &mdash; credentials: `airflow` / `airflow`

---

## 3. Repository Structure

```
airflow-docker/
├── dags/
│   ├── orchestrator.py           # Universal orchestrator DAG (core)
│   ├── flow1_extract.py          # Flow 1 ETL: Extract
│   ├── flow1_transform.py        # Flow 1 ETL: Transform
│   ├── flow1_load.py             # Flow 1 ETL: Load
│   ├── flow2_extract.py          # Flow 2 ETL: Extract
│   ├── flow2_transform.py        # Flow 2 ETL: Transform
│   └── flow2_load.py             # Flow 2 ETL: Load
├── config/
│   ├── pipelines.yaml            # Pipeline definitions (source of truth)
│   └── airflow.cfg               # Airflow config (auto-generated, rarely edited)
├── plugins/                      # Custom Airflow plugins (empty)
├── logs/                         # Runtime logs (auto-generated)
├── .env                          # Connections & environment variables
└── docker-compose.yaml           # Infrastructure definition
```

---

## 4. Architecture

### 4.1 High-Level Data Flow

```
                        ┌─────────────────────┐
          schedule      │                     │   on_fresh.next_pipeline
  ┌──────(cron)────────►│    ORCHESTRATOR      ├─────────────┐
  │                     │    (single DAG)      │             │
  │   ┌─self-trigger──►│                     │◄─────┐      │
  │   │                 └──────────┬──────────┘      │      │
  │   │                            │ on_not_fresh    │      │
  │   │                            ▼                 │      │
  │   │                 ┌──────────────────────┐     │      │
  │   │                 │   ETL DAG (one per   │     │      │
  │   │                 │   run via trigger_etl)│     │      │
  │   │                 │   E → T → L steps    │     │      │
  │   │                 └──────────┬───────────┘     │      │
  │   │                            │                 │      │
  │   └────────────────────────────┘                 │      │
  │              retry (next step)                   │      │
  │                                                  │      │
  │   ┌──────────────────────────────────────────────┘      │
  │   │  when fresh: self-trigger with next pipeline        │
  └───┘                                                     │
                                                            ▼
                                                    next pipeline
                                                   or DONE / external DAG
```

### 4.2 Orchestrator DAG — Internal Flow

The orchestrator (`dags/orchestrator.py`) is a single shared DAG serving all pipelines. Each DAG run triggers exactly **one ETL DAG**, then self-triggers to continue.

**DAG graph (5 tasks):**

```
check_freshness ──► branch
                      ├──► trigger_etl ──► retry              (not fresh)
                      ├──► trigger_next                        (fresh + next pipeline)
                      ├──► done                                (fresh + last pipeline)
                      └──► max_iterations_reached              (safety stop)
```

**State is tracked via `dag_run.conf`:**

```json
{
  "pipeline": "flow1",
  "iteration": 0,
  "etl_step": 0
}
```

### 4.3 The ETL Step Cycle

The `etl_step` field cycles through `on_not_fresh.trigger_dags` across multiple self-triggered runs:

```
Run 1: etl_step=0 → check freshness → NOT fresh
       → trigger ETL[0] (flow1_extract) → retry(step=1)

Run 2: etl_step=1 → skip freshness check (mid-chain)
       → trigger ETL[1] (flow1_transform) → retry(step=2)

Run 3: etl_step=2 → skip freshness check (mid-chain)
       → trigger ETL[2] (flow1_load) → retry(step=0, iteration=1)

Run 4: etl_step=0 → check freshness again → still NOT fresh?
       → trigger ETL[0] again → continue cycle...

Run N: etl_step=0 → check freshness → FRESH!
       → trigger_next(pipeline=flow2) or done
```

**Key design decisions:**
- `etl_step == 0` &rarr; freshness check runs (query source vs dest tables)
- `etl_step > 0` &rarr; freshness check is skipped (we are mid-ETL-chain)
- After the last ETL step, `etl_step` resets to 0 and `iteration` increments
- Freshness = `MAX(dest.date_column) >= MAX(source.date_column WHERE date <= yesterday)`

### 4.4 Freshness Check Logic

```
source: MAX(source_date_column) WHERE date <= yesterday  →  max_source_date
dest:   MAX(dest_date_column)                            →  max_dest_date

if max_source_date is NULL          → fresh (no source data)
if max_dest_date >= max_source_date → fresh (caught up)
otherwise                           → NOT fresh (need to load)

next_date = max_dest_date + 1 day   (passed to ETL DAGs via conf)
```

### 4.5 Self-Trigger & Deadlock Prevention

- `trigger_etl` uses `wait_for_completion=True` (orchestrator waits for ETL DAG to finish)
- `retry` and `trigger_next` use `wait_for_completion=False` (fire-and-forget, current run ends)
- `max_active_runs=1` prevents overlapping orchestrator runs
- `deferrable=False` explicitly set to avoid Airflow 3.x bug [#60049](https://github.com/apache/airflow/issues/60049)
- `reset_dag_run=False` ensures each self-trigger creates a new DAG run

---

## 5. Configuration Reference

### 5.1 `config/pipelines.yaml` — Full Schema

```yaml
pipelines:
  <pipeline_name>:
    description: "Human-readable description"       # optional

    freshness_check:
      conn_id: "local_postgres"                     # Airflow connection ID
      source_table: "source_data"                   # table to read from
      source_date_column: "data_date"               # date column in source
      dest_table: "flow1_target_table"              # table to write to
      dest_date_column: "updated_at"                # date column in destination

    on_not_fresh:
      trigger_dags:                                 # ETL DAGs triggered sequentially
        - "flow1_extract"
        - "flow1_transform"
        - "flow1_load"

    on_fresh:
      next_pipeline: "flow2"                        # self-trigger with next pipeline
      # trigger_dag: "some_external_dag"            # OR trigger any external DAG
      # (set both to null/omit to stop)

    max_iterations: 35                              # safety limit per pipeline
    timeout_minutes: 300                            # dagrun_timeout for orchestrator

settings:
  schedule: "0 10 * * *"                            # orchestrator cron, or null
  default_pipeline: "flow1"                         # used when no conf provided
  default_max_iterations: 30                        # fallback for pipelines
  default_timeout_minutes: 300                      # fallback for pipelines
```

### 5.2 `on_fresh` Routing Options

| Configuration | Behavior |
|---|---|
| `next_pipeline: "flow2"` | Orchestrator self-triggers with `pipeline=flow2` |
| `trigger_dag: "my_dag"` | Triggers an external DAG (any dag_id) |
| both null/omitted | Orchestrator finishes (`done` task) |

### 5.3 Connections (`.env`)

Connections are defined as `AIRFLOW_CONN_<ID>` environment variables:

```
AIRFLOW_CONN_LOCAL_POSTGRES=postgresql://airflow:airflow@postgres:5432/airflow
```

This creates connection ID `local_postgres` (prefix stripped, lowercased). To add a new source, add a new `AIRFLOW_CONN_*` line in `.env` and reference the conn_id in `pipelines.yaml`.

---

## 6. Jinja Templates & Conf Passing

When the orchestrator triggers an ETL DAG via `trigger_etl`, it passes a **conf dict** with all metadata from `check_freshness` XCom. These values are resolved at runtime via Jinja templates.

### 6.1 Conf Received by ETL DAGs

Every triggered ETL DAG receives these keys in `dag_run.conf`:

| Key | Example Value | Description |
|---|---|---|
| `next_date` | `"2026-02-08"` | Next date to load (max_dest + 1 day) |
| `max_dest_date` | `"2026-02-07"` | Current MAX date in destination table |
| `max_source_date` | `"2026-02-10"` | MAX date in source (up to yesterday) |
| `source_table` | `"source_data"` | Source table name |
| `source_date_column` | `"data_date"` | Date column in source |
| `dest_table` | `"flow1_target_table"` | Destination table name |
| `dest_date_column` | `"updated_at"` | Date column in destination |
| `iteration` | `"2"` | Current iteration number |
| `pipeline` | `"flow1"` | Pipeline name |

### 6.2 Using Conf in ETL DAGs

```python
def extract_task_fn(**kwargs):
    conf = kwargs["dag_run"].conf or {}
    next_date = conf.get("next_date")           # "2026-02-08"
    source_table = conf.get("source_table")     # "source_data"
    dest_table = conf.get("dest_table")         # "flow1_target_table"
    # use these to build dynamic SQL
```

### 6.3 XCom Keys Available for Custom Jinja Templates

All XCom keys pushed by `check_freshness` task:

```
pipeline_name, is_fresh, iteration, etl_step, max_iterations,
current_etl_dag, next_etl_step, next_iteration,
next_trigger_dag, next_trigger_conf,
max_dest_date, max_source_date, next_date,
source_table, source_date_column, dest_table, dest_date_column
```

Reference in any TriggerDagRunOperator conf:
```python
"{{ ti.xcom_pull(task_ids='check_freshness', key='next_date') }}"
```

---

## 7. ETL DAGs

Each ETL DAG is **self-contained** with no cross-DAG XCom. They communicate through the database and receive context via `dag_run.conf` from the orchestrator.

| DAG | Purpose | Operation |
|---|---|---|
| `flow1_extract` | Copy one day of data from source to target | `INSERT INTO flow1_target_table` the next missing date from `source_data` |
| `flow1_transform` | Transform data in target table | `UPDATE SET data = UPPER(data)` |
| `flow1_load` | Validate target table state | Logs `MAX(updated_at)` and row count |
| `flow2_extract` | Same as flow1 but for flow2 tables | `INSERT INTO flow2_target_table` |
| `flow2_transform` | Transform flow2 data | `UPDATE SET data = REVERSE(data)` |
| `flow2_load` | Validate flow2 state | Logs stats |

All ETL DAGs use `schedule=None` (triggered only by orchestrator) and `catchup=False`.

**XCom between separate DAG runs does NOT work in Airflow.** Each triggered DAG is an independent run. Use `dag_run.conf` or the database to share data between DAGs.

---

## 8. Adding a New Pipeline

### Step 1: Add pipeline to `config/pipelines.yaml`

```yaml
pipelines:
  # ... existing pipelines ...

  flow3:
    description: "Flow 3 - New pipeline"
    freshness_check:
      conn_id: "local_postgres"
      source_table: "new_source_table"
      source_date_column: "event_date"
      dest_table: "flow3_target_table"
      dest_date_column: "loaded_at"
    on_not_fresh:
      trigger_dags:
        - "flow3_extract"
        - "flow3_transform"
        - "flow3_load"
    on_fresh:
      next_pipeline: null    # or chain to another pipeline
    max_iterations: 50
    timeout_minutes: 600
```

### Step 2: Update the previous pipeline's `on_fresh`

```yaml
  flow2:
    on_fresh:
      next_pipeline: "flow3"    # was null, now chains to flow3
```

### Step 3: Create ETL DAG files in `dags/`

Follow existing patterns (`flow1_extract.py`, etc.). Each DAG should:
- Use `schedule=None` and `catchup=False`
- Read `dag_run.conf` for context (`next_date`, `source_table`, etc.)
- Be fully self-contained (no XCom from other DAGs)

### Step 4: Create target table in PostgreSQL

```sql
CREATE TABLE flow3_target_table (
    id SERIAL PRIMARY KEY,
    loaded_at DATE,
    data TEXT
);
```

### Step 5: Unpause the new DAGs

```bash
docker compose exec airflow-apiserver airflow dags unpause flow3_extract
docker compose exec airflow-apiserver airflow dags unpause flow3_transform
docker compose exec airflow-apiserver airflow dags unpause flow3_load
```

---

## 9. Database Schema

### Application Tables

| Table | Columns | Purpose |
|---|---|---|
| `source_data` | `id SERIAL`, `data_date DATE`, `data TEXT`, `created_at TIMESTAMP` | Source of truth for ETL |
| `flow1_target_table` | `id SERIAL`, `updated_at DATE`, `data TEXT` | Flow 1 destination |
| `flow2_target_table` | `id SERIAL`, `updated_at DATE`, `data TEXT` | Flow 2 destination |

### Freshness Check Queries (auto-generated by orchestrator)

```sql
-- Source: max available date (excludes today, only up to yesterday)
SELECT MAX(data_date) FROM source_data WHERE data_date <= CURRENT_DATE - INTERVAL '1 day'

-- Destination: max loaded date
SELECT MAX(updated_at) FROM flow1_target_table

-- Fresh when: max_dest >= max_source
```

---

## 10. Docker Infrastructure

**7 services** via docker-compose (CeleryExecutor):

| Service | Port | Purpose |
|---|---|---|
| `postgres` | 5432 (internal) | Metadata DB + application data |
| `redis` | 6379 (internal) | Celery message broker |
| `airflow-apiserver` | **8080** | REST API + Web UI |
| `airflow-scheduler` | - | Schedules DAG runs |
| `airflow-dag-processor` | - | Parses DAG files |
| `airflow-worker` | - | Executes tasks |
| `airflow-triggerer` | - | Handles deferred tasks |

Optional: `flower` on port 5555 via `docker compose --profile flower up`

**Volume mounts:** `./dags`, `./logs`, `./config`, `./plugins` &rarr; `/opt/airflow/*`

---

## 11. Airflow 3.x Import Paths

```python
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
```

**Do not use** deprecated `airflow.operators.*` or `airflow.sensors.*` paths.

---

## 12. Safety Mechanisms

| Mechanism | Setting | Purpose |
|---|---|---|
| `max_iterations` | per pipeline (default 30) | Stops ETL retry loop after N full cycles |
| `max_active_runs` | 1 (on orchestrator) | Prevents overlapping orchestrator runs |
| `dagrun_timeout` | per pipeline (default 300 min) | Kills hung orchestrator runs |
| `wait_for_completion=False` | on self-trigger | Prevents deadlock with max_active_runs=1 |
| `deferrable=False` | on all TriggerDagRunOperator | Avoids Airflow 3.x deferral bug |

---

## 13. Troubleshooting

**DAG not appearing in UI:**
```bash
docker compose logs airflow-dag-processor --tail 30 | grep "Errors"
```

**Task stuck in "queued":**
```bash
docker compose logs airflow-worker --tail 20
docker compose exec airflow-apiserver airflow dags unpause <dag_id>
```

**Check current data state:**
```sql
SELECT 'source' as tbl, MAX(data_date), COUNT(*) FROM source_data
UNION ALL SELECT 'flow1', MAX(updated_at), COUNT(*) FROM flow1_target_table
UNION ALL SELECT 'flow2', MAX(updated_at), COUNT(*) FROM flow2_target_table;
```

**Force re-parse DAGs:** touch the .py file or restart dag-processor:
```bash
docker compose restart airflow-dag-processor
```

**Clear failed DAG runs:**
```bash
docker compose exec airflow-apiserver airflow dags backfill --reset-dagruns -s 2024-01-01 orchestrator
```
