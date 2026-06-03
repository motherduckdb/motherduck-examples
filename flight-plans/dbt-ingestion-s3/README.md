---
title: Build Hacker News Models From S3 With dbt
id: dbt-ingestion-s3
description: >-
  Queries a public Hacker News Parquet file in S3 and builds three dbt models on
  top of it, run locally against DuckDB, against MotherDuck, or on a schedule from
  a Flight. Use when you want a dbt-on-MotherDuck recipe that reads Parquet/CSV
  directly from object storage without copying it first.
type: example
features: [flights]
tags: [dbt, s3, parquet, hacker-news]
---

# Build Hacker News Models From S3 With dbt

A small dbt project that reads the public Hacker News Parquet file straight from
S3 as a dbt source, then builds three table models from it. It shows the
MotherDuck pattern of querying object storage in place (no copy step) and running
the same dbt project three ways: locally against a DuckDB file, in the cloud
against MotherDuck, or unattended from a Flight that clones this repo and runs dbt
on a schedule. `flight.py` is a generic dbt runner: it installs git, clones a
repo/ref, writes a runtime `profiles.yml` pointed at MotherDuck, runs dbt, and
writes one audit row to `flight_audit.dbt_flight_runs`.

## What you'll adjust

| Setting | Purpose | Options / example |
|---|---|---|
| `models/sources.yml` `external_location` | The object-storage path dbt reads as its source. | `s3://us-prd-motherduck-open-datasets/hacker_news/parquet/{name}.parquet`; swap for your own S3/HTTPS Parquet or CSV |
| Source table `name` | Which file under that location to query (`{name}` in the path). | `hacker_news_2024_2025` |
| `models/*.sql` | The three analytical models built from the source. | `top_story_by_comments`, `duckdb_keyword_mentions`, `top_domains`; add or replace your own |
| `dbt_project.yml` `models.+materialized` | How models are persisted. | `table` (default) or `view` |
| `profiles.yml` targets | Local vs cloud destination when running dbt by hand. | `local` (`local.db` DuckDB file) or `prod` (`md:hacker_news_stats`) |
| `MOTHERDUCK_DATABASE` (Flight) | MotherDuck database dbt builds into. | `hacker_news_stats` |
| `DBT_SCHEMA` (Flight) | Schema for the built models. | `main` |
| `DBT_COMMAND` (Flight) | dbt verb the Flight runs. | `build` (default), `run`, `test` |
| `DBT_SELECT` / `DBT_EXCLUDE` (Flight) | Limit which models run. | `top_domains`, `tag:daily` |
| `REPO_URL` / `REPO_REF` / `PROJECT_PATH` (Flight) | Where the Flight gets the dbt code. | `main` for ad hoc; pin a tag or commit for scheduled runs; `PROJECT_PATH=dbt-ingestion-s3` |
| `AUDIT_SCHEMA` (Flight) | Schema for the run audit table. | `flight_audit` -> `flight_audit.dbt_flight_runs` |
| `MOTHERDUCK_HOST` (Flight) | MotherDuck API host for non-prod environments. | unset for prod; set for staging |

## Questions to ask the user

- What is the source data: which S3/HTTPS path and file, and is it Parquet or CSV?
- Which models do they actually need (keep the three samples, replace them, or select a subset)?
- Target MotherDuck database and schema for the built tables.
- Local DuckDB run, MotherDuck run, or scheduled Flight?
- For a Flight: which `REPO_REF` to pin, what schedule (cron), and `build` vs `run` vs `test`.
- Do they have a MotherDuck account and access token?

## Run it

Prerequisites: a MotherDuck account and token for cloud runs, and `uv` for the
local Python runtime. The S3 dataset is public, so no AWS credentials are needed.

Local DuckDB run (writes to `local.db`):

```bash
uv run --with dbt-duckdb dbt run --target local
```

MotherDuck run (create the database once, then build):

```bash
export MOTHERDUCK_TOKEN=your_token_here
uv run --with dbt-duckdb dbt run --target prod
```

Create the `prod` database first if it does not exist:

```sql
CREATE DATABASE IF NOT EXISTS hacker_news_stats;
```

### Deploy as a Flight

Use the MotherDuck MCP `create_flight` tool to create a Flight from this folder's
`flight.py` and `requirements.txt` (`duckdb`, `dbt-duckdb`). Set the knobs from
"What you'll adjust" as Flight config/env, for example `REPO_REF` (pin a tag or
commit for scheduled runs), `PROJECT_PATH=dbt-ingestion-s3`,
`MOTHERDUCK_DATABASE=hacker_news_stats`, `DBT_SCHEMA=main`, `DBT_COMMAND=build`,
and `AUDIT_SCHEMA=flight_audit`. Add an optional cron schedule (the original
default was daily at `07:45 UTC`, cron `45 7 * * *`). Then run it on demand with
the `run_flight` tool. Each run writes one row to `flight_audit.dbt_flight_runs`
recording the repo, ref, project path, profile, target, schema, and command.

## How it works / Learn more

- `flight.py`: the dbt runner. It validates `DBT_COMMAND`, runs `dbt deps` only
  when `packages.yml`/`dependencies.yml` exist, optionally runs `dbt seed`
  (`RUN_DBT_SEED`), and writes the audit row via a direct `duckdb.connect("md:...")`.
- `models/sources.yml`: defines the S3 Parquet file as a dbt source using DuckDB's
  external-location support, so dbt reads it in place rather than copying it.
- The same runner is published as a reusable template in
  [`../dbt-runner`](../dbt-runner); this folder is the worked example of it.
- Flights runtime, scheduling, and secrets: run the `get_flight_guide` MCP tool.
- Deeper MotherDuck or DuckDB questions (querying S3, dbt-duckdb behavior): use the
  `ask_docs_question` MCP tool.
