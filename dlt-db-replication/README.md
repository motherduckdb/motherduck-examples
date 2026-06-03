---
title: Replicate PostgreSQL Tables to MotherDuck with dlt
id: dlt-db-replication
description: >-
  A dlt pipeline that extracts a list of PostgreSQL tables in parallel and loads
  them into MotherDuck, with per-run timing metrics. Use when you need to copy or
  refresh tables from a source SQL database into MotherDuck and want tunable
  extract, normalize, and load parallelism.
type: example
features: []
tags: [dlt, postgres, connectorx, parquet, sqlalchemy, replication]
---

# Replicate PostgreSQL Tables to MotherDuck with dlt

This example uses [dlt](https://dlthub.com/) to replicate a configured set of PostgreSQL tables into MotherDuck. It reads the source connection and the table list from `.dlt/` config, extracts tables in parallel through dlt's `sql_database` source (ConnectorX backend, Parquet interim storage), and loads them into a MotherDuck dataset with `write_disposition="replace"` (full refresh: each table is dropped and recreated every run). The MotherDuck pattern it shows is bulk loading from an external relational database via dlt's MotherDuck destination, plus a helper that logs extract, normalize, and load timings per run.

## What you'll adjust

| Setting | Purpose | Options / example |
| --- | --- | --- |
| `[sources.sql_database.credentials]` in `.dlt/secrets.toml` | Source PostgreSQL connection | `drivername`, `database`, `host`, `port` (5432), `username`, `password` |
| `[destination.motherduck.credentials] token` in `.dlt/secrets.toml` | MotherDuck auth token for the destination | your MotherDuck access token |
| `[sources.sql_database] schema` in `.dlt/config.toml` | Source schema to read tables from | e.g. `my_pg`, `public` |
| `[sources.sql_database] tables` in `.dlt/config.toml` | Which tables to replicate (read by `dlt.config.get("sources.sql_database.tables")` in `sql_database_pipeline.py`) | list of table names, e.g. `["customer", "store_sales"]` |
| `pipeline_name` / `dataset_name` in `sql_database_pipeline.py` | Pipeline id and target MotherDuck dataset (schema) | `pg2md` / `pg2md_data` |
| `write_disposition` in `sql_database_pipeline.py` `pipeline.run(...)` | Load strategy | `replace` (full refresh, current), `append`, or `merge` (incremental) |
| `[sources.sql_database] workers` and `[postgres] pool_size` in `.dlt/config.toml` | Source extraction parallelism and matching connection pool | both `6` by default; keep them equal |
| `[extract] / [normalize] / [load] workers` in `.dlt/config.toml` | Per-stage parallelism | `8` / `4` / `4` |
| `[destination.motherduck] batch_size` in `.dlt/config.toml` | Rows per load batch (memory vs throughput) | `1000000` |
| `[data_writer] format` in `.dlt/config.toml` | Interim file format | `parquet` |
| `[runtime] log_level` in `.dlt/config.toml` | Log verbosity | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` |

## Questions to ask the user

- Source database: which PostgreSQL host, database, and schema?
- Which tables to replicate, and is the list stable or changing often?
- Load strategy: full refresh (`replace`, current default) or incremental (`merge`/`append` with a cursor/primary key)?
- Target MotherDuck database and dataset (schema) name?
- Expected data volume, so extract/normalize/load workers and `batch_size` can be tuned?
- Credentials: PostgreSQL username/password and a MotherDuck access token, and where they should live (`secrets.toml` vs environment).
- How often should this run, and from where (local, CI, an orchestrator)?

## Run it

Prerequisites: Python 3.11+, a reachable PostgreSQL source, and a MotherDuck account plus access token. Fill in `.dlt/secrets.toml` (PostgreSQL credentials and the MotherDuck `token`) before running. `secrets.toml` is gitignored.

```bash
# One step: uv creates the env, installs deps from pyproject.toml, and runs
uv run sql_database_pipeline.py
```

Or sync first, then run:

```bash
uv sync
uv run python sql_database_pipeline.py
```

The run connects to PostgreSQL, extracts the configured tables in parallel, normalizes them to Parquet, loads them into the MotherDuck dataset, and then logs per-stage timing and row counts via `timing_logs.py`.

## How it works / Learn more

- `sql_database_pipeline.py`: defines the pipeline, reads the table list from config, builds the `sql_database` source with `backend="connectorx"`, `.parallelize()`, and `.with_resources(*tables)`, then runs with `write_disposition="replace"`.
- `timing_logs.py`: `print_pipeline_metrics()` and `configure_logger()` extract timings and row counts from the dlt trace for overall, extract, normalize, and load stages.
- `.dlt/config.toml`: all the non-secret knobs (schema, table list, workers, batch size, format, log level).
- dlt write dispositions and incremental loading: https://dlthub.com/docs/general-usage/incremental-loading
- For deeper MotherDuck or DuckDB questions (destination behavior, dataset/schema layout, tuning loads), run the `ask_docs_question` MCP tool or see the MotherDuck docs.
