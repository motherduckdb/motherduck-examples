---
title: Run TPC-DS Models on DuckLake with dbt
id: dbt-ducklake
description: >-
  Loads the TPC-DS benchmark dataset into a DuckLake-backed database with dbt,
  then materializes the 99 TPC-DS analytical queries into a separate MotherDuck
  database. Use when you want a dbt project that lands raw tables in DuckLake
  storage and writes analytics models to native MotherDuck, optionally run from
  a Flight.
type: example
features: [flights, ducklake]
tags: [dbt, tpc-ds, s3, parquet, lakehouse]
---

# Run TPC-DS Models on DuckLake with dbt

This dbt project loads TPC-DS Scale Factor 100 parquet from public S3 into a
DuckLake-backed database (`dbt_ducklake`), then runs the standard 99 TPC-DS
analytical queries and materializes their results into a second MotherDuck
database (`my_db`). It shows the MotherDuck pattern of splitting a project across
two storage layers: raw tables in DuckLake (so the parquet lives in your own
object store) and downstream analytics in native MotherDuck storage. The same
`flight.py` as the `dbt-runner` template can run it in MotherDuck-managed compute.

## What you'll adjust

| Setting | Purpose | Options / example |
|---|---|---|
| `profiles.yml` `path` (target `motherduck`) | DuckLake database dbt builds raw tables into | `md:dbt_ducklake`; create it first as a DuckLake DB with your own `DATA_PATH` |
| `profiles.yml` `is_ducklake` | Marks the target as DuckLake-backed | `true` |
| `profiles.yml` `schema` / `threads` | Default schema and parallelism | `test` / `4` |
| `dbt_project.yml` `models.dbt_ducklake.tpcds.queries.+database` | Where the 99 query models land | `my_db` (a separate native MotherDuck database) |
| `dbt_project.yml` raw/queries `+materialized`, `+schema`, `+tags` | Materialization and tagging per layer | raw: `table` in schema `raw`; queries: `table` |
| `models/tpcds/raw/_sources.yml` `external_location` | S3 location of the TPC-DS parquet | `s3://devrel-test-data/tpcds/sf100/{name}.parquet`; swap for your own bucket or scale factor |
| dbt selector (`--select` / `--exclude`) | Limit which models run | e.g. `tag:raw`, `tag:queries`, a single `query_1` |

For the Flight, the same knobs are set as environment variables (the Flight
writes its own `profiles.yml` at runtime, ignoring the checked-in one):

| Flight env var | Purpose | Options / example |
|---|---|---|
| `PROJECT_PATH` | dbt project path inside the cloned repo | `dbt-ducklake` |
| `DBT_PROFILE_NAME` | Profile name written to the runtime `profiles.yml` | `dbt_ducklake` |
| `MOTHERDUCK_DATABASE` | DuckLake database dbt connects to | `dbt_ducklake` |
| `DBT_IS_DUCKLAKE` | Adds `is_ducklake: true` to the generated profile | `true` (required for this example) |
| `DBT_SCHEMA` | Schema used by the generated profile | `test` |
| `DBT_COMMAND` | dbt command to run | `build` (also `run`, `test`) |
| `DBT_SELECT` / `DBT_EXCLUDE` | Optional model selectors | `tag:queries` |
| `DBT_THREADS` | dbt thread count | `4` |
| `REPO_URL` / `REPO_REF` | Repo and ref to clone | defaults to this repo on `main`; pin a tag/commit for scheduled runs |
| `AUDIT_SCHEMA` | Schema for the run audit table | `flight_audit` |

## Questions to ask the user

- Source: keep the public `devrel-test-data` TPC-DS bucket, or point at your own parquet and scale factor?
- DuckLake database: which database name and `DATA_PATH` (S3/object-store prefix) should hold the raw tables?
- Analytics target: which native MotherDuck database should the query models land in (default `my_db`)?
- Scope: run all 99 queries plus 25 raw tables, or a subset via `--select` / `--exclude`?
- Full vs partial: rebuild raw tables every run, or only refresh the query layer?
- Schedule: on-demand (default, TPC-DS is heavy) or a cron schedule?
- Credentials: MotherDuck token, and access to the S3 source bucket.

## Run it

Prerequisites: a MotherDuck account and token, plus a `dbt_ducklake` database
created as a DuckLake database with your own `DATA_PATH`, and a `my_db` database
for the analytics models. Create the DuckLake database first, for example:

```sql
CREATE DATABASE dbt_ducklake (TYPE ducklake, DATA_PATH 's3://your-bucket/dbt_ducklake/');
CREATE DATABASE IF NOT EXISTS my_db;
```

Then build locally:

```bash
uv sync
uv run dbt build
```

This creates the 25 TPC-DS raw tables in DuckLake from S3 parquet and runs the
99 analytical query models into `my_db`. Your browser prompts for MotherDuck
authentication unless a token is configured.

### Deploy as a Flight

Use the MotherDuck MCP `create_flight` tool with this folder's `flight.py` and
`requirements.txt` as the source. Set the knobs from "What you'll adjust" as
Flight config/env: at minimum `PROJECT_PATH=dbt-ducklake`,
`DBT_PROFILE_NAME=dbt_ducklake`, `MOTHERDUCK_DATABASE=dbt_ducklake`,
`DBT_IS_DUCKLAKE=true`, and `DBT_SCHEMA=test`. The `MOTHERDUCK_TOKEN` is injected
by the runtime. Leave the Flight on-demand by default (TPC-DS is heavier than the
smaller scheduled examples); add a cron schedule only if you want periodic
rebuilds. Then trigger it with the `run_flight` tool. At runtime the Flight
installs `git`, clones the repo, writes a runtime `profiles.yml` with
`is_ducklake: true`, runs `dbt build`, and writes one audit row to
`flight_audit.dbt_flight_runs`.

## How it works / Learn more

- `flight.py` is the shared dbt-runner: it clones the repo, generates a
  `profiles.yml` from the env vars above, runs the dbt command, and records an
  audit row. See `flight-plans/dbt-runner/README.md` for the full env var matrix.
- `dbt_project.yml` splits the two storage layers; `profiles.yml` holds the local
  `motherduck` and `local` targets; `models/tpcds/raw/_sources.yml` defines the S3
  source location.
- Flights runtime, scheduling, and secrets: run the `get_flight_guide` MCP tool.
- DuckLake setup, `DATA_PATH`, and deeper MotherDuck/DuckDB questions: use the
  `ask_docs_question` MCP tool.
