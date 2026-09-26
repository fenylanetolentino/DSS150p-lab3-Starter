    # DSS150P Laboratory 3 Starter Repository

This repository supports Module 2: Pipeline Construction, Storage, and Orchestration.
It is intentionally incomplete. Students must implement the marked TODOs and document their decisions.

## Main progression
- Goal 1: reproducible environment, modularization, Git, Docker, configuration
- Goal 2: raw -> staging -> curated transformations; audit/error handling; rerun-safe loading
- Goal 3: CSV/JSON/Parquet/PostgreSQL comparison; partitioning; selected-partition load
- Goal 4: Apache Airflow DAG for extract -> transform -> load -> validate

Start with `DSS150P_Laboratory_Activity_3.pdf`.

## Recommended commands
```bash
cp .env.example .env
python -m venv .venv
# activate .venv then:
pip install -r requirements.txt
python -m src.cli validate-env
```
The provided `.env.example` uses `POSTGRES_HOST=localhost` for host-side commands. Docker Compose overrides the application containers to use the service hostname `postgres`.

Docker/PostgreSQL:
```bash
docker compose up -d postgres
docker compose run --rm pipeline python -m src.cli validate-env
```

## Running the full pipeline (Goal 2)

Run each stage individually, or all at once with `run-all`:

```bash
python -m src.cli extract
python -m src.cli transform
python -m src.cli load
python -m src.cli validate
```

Or equivalently:
```bash
python -m src.cli run-all
python -m src.cli load
python -m src.cli validate
```

`load` is rerun-safe: it performs an UPSERT into `curated.sales_order_lines` keyed on `order_id`,
using `record_hash` to skip rows whose content hasn't changed. Running `load` multiple times will
not create duplicate rows.

## Storage benchmark and partitioning (Goal 3)

Compare CSV, JSON Lines, Parquet, and PostgreSQL for the same curated dataset:
```bash
python -m src.cli benchmark --repeats 5
```
Results are written to `data/benchmarks/benchmark_results.csv`.

Load a single year/month partition into PostgreSQL and record it in `audit.partition_loads`:
```bash
python -m src.cli load-partition --year 2026 --month 1
```

## Airflow in Goal 4

```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up airflow-init
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler
```
Airflow UI: http://localhost:8080 (training credentials: admin/admin; change if reused outside the lab).

With the Airflow services running, open the UI and:

1. Unpause and trigger `dss150p_sales_pipeline`.
2. For a full run, use the default parameters:
   - `run_mode`: `full`
3. For a partition-mode run, set:
   - `run_mode`: `partition`
   - `year`: e.g. `2026`
   - `month`: e.g. `1`
4. The DAG runs `extract -> transform -> load -> validate` in sequence, propagating a single
   `PIPELINE_RUN_ID` (the Airflow `run_id`) to every task so all stages share one run identity.
5. The `load` task branches automatically based on `run_mode`: `full` calls `python -m src.cli load`,
   `partition` calls `python -m src.cli load-partition --year <year> --month <month>`.

You can also trigger and inspect a run from the CLI inside the scheduler container:
```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml exec airflow-scheduler \
  airflow dags test dss150p_sales_pipeline 2026-01-01
```

### Failure handling and recovery

Each task retries twice (1-minute delay) before failing, and an `on_failure_callback` logs the
DAG id, task id, run id, try number, and error to the task log. All stages are idempotent and
safe to rerun or "Clear Task" after a failure: `extract` and `transform` write into run-id-scoped
folders and never overwrite prior data, and `load`/`load-partition` use hash-gated UPSERTs, so
recovery never requires manual database cleanup.

## Git workflow

Every major goal ends with a commit (and this repo also tags each checkpoint):
```bash
git add .
git commit -m "feat: <describe the goal/task completed>"
git push
```
See `git log --oneline --all` for the full history, including `goal-1-checkpoint` through
`goal-4-checkpoint` tags.