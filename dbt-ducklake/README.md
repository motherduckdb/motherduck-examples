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
tags: [dbt]
---

# Run TPC-DS Models on DuckLake with dbt

This dbt project loads TPC-DS Scale Factor 100 parquet from public S3 into a
DuckLake-backed database (`dbt_ducklake`), then runs the standard 99 TPC-DS
analytical queries and materializes their results into a second MotherDuck
database (`my_db`). It shows the MotherDuck pattern of splitting a project across
two storage layers: raw tables in DuckLake (so the parquet lives in your own
object store) and downstream analytics in native MotherDuck storage. The same
`flight.py` as the `dbt-runner` template can run it in MotherDuck-managed compute.

## Architecture

Two model layers, two storage backends:

| Layer | Path | Storage | Materialization | Schema/tag |
|---|---|---|---|---|
| Raw (`models/tpcds/raw/`) | reads S3 parquet via dbt sources | DuckLake database `dbt_ducklake` | `table` | schema `raw`, tag `raw` |
| Queries (`models/tpcds/queries/`) | references the raw tables | native MotherDuck database `my_db` | `table` | tag `queries` |

```
models/tpcds/
  raw/          # 25 TPC-DS base tables -> DuckLake (schema `raw`)
    _sources.yml
    customer.sql, store_sales.sql, catalog_sales.sql, web_sales.sql, ...
  queries/      # 99 TPC-DS analytical queries -> MotherDuck `my_db`
    query_1.sql ... query_99.sql
```

The 25 raw tables are: `call_center`, `catalog_page`, `catalog_returns`,
`catalog_sales`, `customer_address`, `customer_demographics`, `customer`,
`date_dim`, `household_demographics`, `income_band`, `inventory`, `item`,
`promotion`, `reason`, `ship_mode`, `store_returns`, `store_sales`, `store`,
`time_dim`, `warehouse`, `web_page`, `web_returns`, `web_sales`, `web_site`.

## How it works

The raw models are intentionally thin: each one just selects from a dbt source.

```sql
-- models/tpcds/raw/customer.sql
from {{ source("tpc-ds", "customer") }}
```

The source is defined once in `models/tpcds/raw/_sources.yml`, where
`meta.external_location` tells the dbt-duckdb adapter to read parquet straight
from S3, substituting each table name into `{name}`:

```yaml
sources:
  - name: tpc-ds
    meta:
      external_location: |-
        s3://devrel-test-data/tpcds/sf100/{name}.parquet
    tables:
      - name: customer
      - name: store_sales
      # ... 25 tables total
```

Because the `motherduck` target sets `is_ducklake: true`, those raw tables are
materialized into the DuckLake-backed `dbt_ducklake` database, so the underlying
parquet lands in your own `DATA_PATH` object store. The 99 query models then
reference the raw tables with `{{ ref(...) }}` and, via the
`tpcds.queries.+database: my_db` config in `dbt_project.yml`, write their results
into the separate native MotherDuck database `my_db`.

`profiles.yml` also ships a `local` target (`path: ducklake:local_dev.db`) for
developing against a local DuckLake file instead of MotherDuck; switch with
`dbt build --target local`.

The Flight reuses the shared dbt-runner `flight.py`: at runtime it installs
`git`, shallow-clones the repo and ref, generates a `profiles.yml` from the env
vars in "What you'll adjust" (adding `is_ducklake: true` when `DBT_IS_DUCKLAKE` is truthy), runs
the dbt command, then writes one row to `<AUDIT_SCHEMA>.dbt_flight_runs` recording
the repo, ref, project path, profile/target/schema, and command. Identifier-like
env vars (`DBT_PROFILE_NAME`, `DBT_SCHEMA`, `DBT_TARGET`, `AUDIT_SCHEMA`) are
validated against `^[A-Za-z_][A-Za-z0-9_]*$` before being interpolated into SQL.

## What you'll adjust

| Setting | Purpose | Options / example |
|---|---|---|
| `profiles.yml` `path` (target `motherduck`) | DuckLake database dbt builds raw tables into | `md:dbt_ducklake`; create it first as a DuckLake DB with your own `DATA_PATH` |
| `profiles.yml` `is_ducklake` | Marks the target as DuckLake-backed | `true` (required; see Caveats) |
| `profiles.yml` `schema` / `threads` | Default schema and parallelism | `test` / `4` |
| `dbt_project.yml` `models.dbt_ducklake.tpcds.queries.+database` | Where the 99 query models land | `my_db` (a separate native MotherDuck database) |
| `dbt_project.yml` raw/queries `+materialized`, `+schema`, `+tags` | Materialization and tagging per layer | raw: `table` in schema `raw`; queries: `table` |
| `models/tpcds/raw/_sources.yml` `meta.external_location` | S3 location of the TPC-DS parquet | `s3://devrel-test-data/tpcds/sf100/{name}.parquet`; swap for your own bucket or scale factor |
| dbt selector (`--select` / `--exclude`) | Limit which models run | e.g. `tag:raw`, `tag:queries`, a single `query_1` |

For the Flight, the same knobs are set as environment variables (the Flight
writes its own `profiles.yml` at runtime, ignoring the checked-in one):

| Flight env var | Purpose | Options / example |
|---|---|---|
| `PROJECT_PATH` | dbt project path inside the cloned repo | `dbt-ducklake` |
| `DBT_PROFILE_NAME` | Profile name written to the runtime `profiles.yml`; must match `dbt_project.yml`'s `profile:` | `dbt_ducklake` |
| `MOTHERDUCK_DATABASE` | DuckLake database dbt connects to | `dbt_ducklake` |
| `DBT_IS_DUCKLAKE` | Adds `is_ducklake: true` to the generated profile | `true` (required for this example) |
| `DBT_SCHEMA` | Schema used by the generated profile | `test` |
| `DBT_TARGET` | dbt target name in the generated profile | `flight` (default) |
| `DBT_COMMAND` | dbt command to run | `build` (also `run`, `test`) |
| `DBT_SELECT` / `DBT_EXCLUDE` | Optional model selectors | `tag:queries` |
| `DBT_THREADS` | dbt thread count | `4` |
| `REPO_URL` / `REPO_REF` | Repo and ref to clone | defaults to this repo on `main`; pin a tag/commit for scheduled runs |
| `AUDIT_SCHEMA` | Schema for the run audit table | `flight_audit` |
| `MOTHERDUCK_HOST` | Optional MotherDuck API host for non-production environments | unset (default), e.g. a staging host |
| `MOTHERDUCK_TOKEN` | MotherDuck access token | Flight-managed: injected when you select a token, do not set manually |

## Questions to answer

- Source: keep the public `devrel-test-data` TPC-DS bucket, or point at your own parquet and scale factor?
- DuckLake database: which database name and `DATA_PATH` (S3/object-store prefix) should hold the raw tables?
- Analytics target: which native MotherDuck database should the query models land in (default `my_db`)?
- Scope: run all 99 queries plus 25 raw tables, or a subset via `--select` / `--exclude`?
- Full vs partial: rebuild raw tables every run, or only refresh the query layer?
- Schedule: on-demand (default, TPC-DS is heavy) or a cron schedule?
- Credentials: MotherDuck token, and read access to the S3 source bucket.

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
authentication unless a token is configured (set `MOTHERDUCK_TOKEN` in the
environment to run non-interactively).

### Deploy as a Flight

Use the MotherDuck MCP `create_flight` tool with this folder's `flight.py` and
`requirements.txt` as the source. Set the knobs from "What you'll adjust" as
Flight config/env: at minimum `PROJECT_PATH=dbt-ducklake`,
`DBT_PROFILE_NAME=dbt_ducklake`, `MOTHERDUCK_DATABASE=dbt_ducklake`,
`DBT_IS_DUCKLAKE=true`, and `DBT_SCHEMA=test`. Select an access token so the
runtime injects `MOTHERDUCK_TOKEN` (you can list candidates with
`SELECT * FROM md_access_tokens()`). Leave the Flight on-demand by default
(TPC-DS is heavier than the smaller scheduled examples); add a cron schedule only
if you want periodic rebuilds. Then trigger it with the `run_flight` tool. At
runtime the Flight installs `git`, clones the repo, writes a runtime
`profiles.yml` with `is_ducklake: true`, runs `dbt build`, and writes one audit
row to `flight_audit.dbt_flight_runs`.

## Files

- `[flight.py](flight.py)` - the shared dbt-runner Flight: installs git, shallow-clones the repo/ref, writes a runtime `profiles.yml` from env vars (adding `is_ducklake: true`), runs the dbt command, and records one audit row in `<AUDIT_SCHEMA>.dbt_flight_runs`.
- `[dbt_project.yml](dbt_project.yml)` - dbt project config: profile name `dbt_ducklake`, plus the per-layer settings that send raw models to schema `raw` (tag `raw`) and the query models to database `my_db` (tag `queries`).
- `[profiles.yml](profiles.yml)` - the local dbt profile: a `motherduck` target (`md:dbt_ducklake`, `is_ducklake: true`) and a `local` target (`ducklake:local_dev.db`) for offline development. The Flight ignores this and generates its own.
- `[models/tpcds/raw/](models/tpcds/raw/)` - the 25 thin raw models (one `select` per TPC-DS base table) materialized into DuckLake, plus `_sources.yml` which points the dbt-duckdb adapter at the S3 parquet via `meta.external_location`.
- `[models/tpcds/queries/](models/tpcds/queries/)` - the 99 standard TPC-DS analytical query models (`query_1.sql` through `query_99.sql`) that `ref` the raw tables and land in `my_db`.
- `[requirements.txt](requirements.txt)` - Python deps the Flight installs at runtime: `duckdb` and `dbt-duckdb`.
- `[pyproject.toml](pyproject.toml)` - local project deps for `uv sync` / `uv run dbt` (dbt-core, dbt-duckdb, duckdb), Python 3.12+. `[uv.lock](uv.lock)` pins the resolved versions.
- `[.python-version](.python-version)` - pins the local Python version (3.12) for uv.
- `[.user.yml](.user.yml)` / `[.gitignore](.gitignore)` - dbt anonymous-usage user id, and ignore rules for `target/`, `dbt_packages/`, `logs/`, and `*.db`.
- `[analyses/](analyses/)`, `[macros/](macros/)`, `[seeds/](seeds/)`, `[snapshots/](snapshots/)`, `[tests/](tests/)` - empty standard dbt scaffold directories (each holds a `.gitkeep`).

## Caveats

- Create the DuckLake database before the first run. `CREATE DATABASE dbt_ducklake
  (TYPE ducklake, DATA_PATH '...')` and `my_db` must already exist; dbt does not
  create them. A missing `dbt_ducklake` fails the run, and a missing `my_db`
  fails the query layer.
- `is_ducklake: true` is required, not optional. Without it (locally or by
  omitting `DBT_IS_DUCKLAKE=true` on the Flight), the raw tables silently write
  to native MotherDuck storage instead of your `DATA_PATH` object store, defeating
  the point of this example. There is no error, just the wrong storage backend.
- `DBT_PROFILE_NAME` (and `profiles.yml`'s profile name) must match the `profile:`
  key in `dbt_project.yml` (`dbt_ducklake`). A mismatch makes dbt fail to find a
  profile.
- TPC-DS Scale Factor 100 is large. A full `dbt build` materializes 25 raw tables
  plus 99 query tables and can take a while and consume meaningful memory. For
  iteration, scope with `--select tag:raw` / `--select tag:queries` or a single
  model (`--select query_1`), and prefer on-demand over a schedule.
- The S3 source bucket needs read access. The public `devrel-test-data` bucket is
  readable without credentials; if you swap in your own bucket, configure object
  storage credentials (a MotherDuck/DuckDB secret) or the raw layer fails.
- Identifier env vars are validated. `DBT_PROFILE_NAME`, `DBT_SCHEMA`,
  `DBT_TARGET`, and `AUDIT_SCHEMA` must be simple SQL identifiers
  (`[A-Za-z_][A-Za-z0-9_]*`); values with dashes, spaces, or dots raise an error
  before anything runs.
- `DBT_COMMAND` only accepts `build`, `run`, or `test`; anything else raises.
- Don't hardcode the token. `MOTHERDUCK_TOKEN` is injected by the Flight runtime
  from the selected access token, not stored in Flight config or the repo.
- For scheduled runs, pin `REPO_REF` to a tag or commit rather than `main`, so a
  later push to the branch can't change what the Flight executes.

## Learn more

- `flight.py` is the shared dbt-runner: it clones the repo, generates a
  `profiles.yml` from the env vars above, runs the dbt command, and records an
  audit row. See `flight-plans/dbt-runner/README.md` for the full env var matrix.
- `dbt_project.yml` splits the two storage layers; `profiles.yml` holds the local
  `motherduck` and `local` targets; `models/tpcds/raw/_sources.yml` defines the S3
  source location.
- Flights runtime, scheduling, and secrets: run the `get_flight_guide` MCP tool.
- DuckLake setup, `DATA_PATH`, and deeper MotherDuck/DuckDB questions: use the
  `ask_docs_question` MCP tool.
