# Airflow ETL Orchestrator — Описание проекта

| Поле | Значение |
|---|---|
| Система | Apache Airflow (Docker Compose) |
| Тип документа | Project Overview |
| Статус | Draft |
| Основной DAG | `orchestrator` |
| Конфигурация | `config/pipelines.yaml` |

## 1. Назначение
Проект реализует локальную ETL-платформу на Apache Airflow с универсальным оркестратором, который управляет последовательным запуском пайплайнов на основе проверки свежести данных в PostgreSQL.

Основная цель:
- автоматизировать обработку данных по дням;
- запускать ETL-шаги только при отставании целевых таблиц от источника;
- поддерживать цепочку из нескольких пайплайнов (flow1 -> flow2).

## 2. Технологический стек
- Apache Airflow `3.1.7` (CeleryExecutor)
- PostgreSQL `16` (метаданные Airflow + рабочие таблицы)
- Redis `7.2` (broker для Celery)
- Docker Compose (локальная разработка)
- Python DAG-скрипты

## 3. Архитектура решения
### 3.1 Инфраструктурные сервисы
Определены в `docker-compose.yaml`:
- `postgres`
- `redis`
- `airflow-apiserver`
- `airflow-scheduler`
- `airflow-dag-processor`
- `airflow-worker`
- `airflow-triggerer`
- `airflow-init`

### 3.2 Топология данных
Используются таблицы:
- источник: `source_data(data_date, data)`
- цели Flow 1: `flow1_target_table(updated_at, data)`
- цели Flow 2: `flow2_target_table(updated_at, data)`

Подключение к БД задаётся через переменную окружения:
- `AIRFLOW_CONN_LOCAL_POSTGRES=postgresql://airflow:airflow@postgres:5432/airflow`

### 3.3 Управление конфигурацией
Файл `config/pipelines.yaml` содержит:
- перечень пайплайнов (`flow1`, `flow2`);
- правила freshness-check;
- DAG-и, запускаемые при отставании;
- поведение при актуальных данных (`next_pipeline`);
- глобальные настройки расписания и лимитов.

## 4. Логика оркестрации
Оркестратор: `dags/orchestrator.py` (`dag_id=orchestrator`).

### 4.1 Принцип работы
Каждый запуск оркестратора выполняет цикл:
1. `check_freshness` — вычисляет, актуальны ли данные в target относительно source.
2. `branch` — выбирает ветку выполнения:
   - `trigger_etl` (данные неактуальны);
   - `trigger_next` (данные актуальны и задан следующий пайплайн);
   - `done` (данные актуальны и цепочка завершена);
   - `max_iterations_reached` (достигнут лимит повторов).

### 4.2 Поведение при неактуальных данных
- Запускается **один** DAG из списка `on_not_fresh.trigger_dags`.
- Затем оркестратор самотриггерится (`retry`) с обновлённым `etl_step`/`iteration`.
- ETL-шаги крутятся по кругу: extract -> transform -> load -> повторная проверка.

### 4.3 Поведение при актуальных данных
- Оркестратор переходит к `next_pipeline` (если задан).
- Для `flow2` `next_pipeline=null`, поэтому цепочка завершается.

### 4.4 Расписание
Глобальное расписание оркестратора:
- `0 10 * * *` (из `config/pipelines.yaml`).

## 5. ETL-пайплайны
### 5.1 Flow 1
Конфигурация:
- описание: `Flow 1 - Main data pipeline`
- шаги при отставании:
  1. `flow1_extract`
  2. `flow1_transform`
  3. `flow1_load`

DAG-и:
- `dags/flow1_extract.py`:
  - ищет ближайшую необработанную дату из `source_data`;
  - вставляет одну дневную запись в `flow1_target_table`.
- `dags/flow1_transform.py`:
  - нормализация данных: `UPPER(data)`.
- `dags/flow1_load.py`:
  - валидационный шаг: логирует `MAX(updated_at)` и `COUNT(*)`.

### 5.2 Flow 2
Конфигурация:
- описание: `Flow 2 - Secondary data pipeline`
- шаги при отставании:
  1. `flow2_extract`
  2. `flow2_transform`
  3. `flow2_load`

DAG-и:
- `dags/flow2_extract.py`:
  - аналогично Flow 1, заполняет `flow2_target_table`.
- `dags/flow2_transform.py`:
  - трансформация данных: `REVERSE(data)`.
- `dags/flow2_load.py`:
  - валидационный шаг: логирует `MAX(updated_at)` и `COUNT(*)`.

## 6. Управляющие ограничения и safety-механизмы
- `max_iterations` на пайплайн (по умолчанию до `35` в конфиге flow).
- `dagrun_timeout` (по умолчанию до `300` минут).
- `max_active_runs=1` для `orchestrator`.
- Проверка данных идёт до `CURRENT_DATE - 1 day` (исключение незавершённого текущего дня).

## 7. Наблюдаемость и операционная диагностика
Логи Airflow пишутся в `logs/`, включая:
- логи DAG processor;
- логи запусков задач DAG-ов.

Вкладки IDE показывают активный мониторинг логов:
- `logs/dag_processor/2026-02-11/dags-folder/flow1_extract.py.log`
- `logs/dag_processor/2026-02-11/dags-folder/flow1_transform.py.log`
- `logs/dag_processor/2026-02-11/dags-folder/flow2_extract.py.log`

## 8. Структура репозитория
- `docker-compose.yaml` — локальная инфраструктура Airflow/Celery/Postgres/Redis
- `config/airflow.cfg` — настройки Airflow
- `config/pipelines.yaml` — декларативная конфигурация пайплайнов
- `dags/orchestrator.py` — универсальный оркестратор
- `dags/flow1_*.py` — ETL шаги Flow 1
- `dags/flow2_*.py` — ETL шаги Flow 2
- `logs/` — runtime-логи
- `plugins/` — расширения Airflow (на текущий момент пусто)

## 9. Границы текущей реализации
- Реализация ориентирована на локальную среду разработки (не production-ready).
- ETL-операции демонстрационные (простые SQL-трансформации).
- Оркестрация завязана на PostgreSQL-таблицы и конфигурационный YAML.

## 10. Краткий сценарий выполнения
1. По расписанию стартует `orchestrator`.
2. Проверяется отставание `flow1_target_table` от `source_data`.
3. Если есть лаг, по одному запускаются `flow1_extract -> flow1_transform -> flow1_load`.
4. После выравнивания `flow1` оркестратор переключается на `flow2`.
5. Повторяется тот же цикл для `flow2`.
6. При отсутствии отставания процесс завершается.
