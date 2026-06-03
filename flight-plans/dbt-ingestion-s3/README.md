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
tags: [dbt]
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

## How it works

`models/sources.yml` defines the S3 file as a dbt source using DuckDB's external
location support, so dbt reads the Parquet in place instead of copying it. The
`{name}` placeholder is filled from the table name:

```yaml
sources:
  - name: hn_external
    meta:
      external_location: "s3://us-prd-motherduck-open-datasets/hacker_news/parquet/{name}.parquet"
    tables:
      - name: hacker_news_2024_2025
```

Models reference that source with `{{ source('hn_external', 'hacker_news_2024_2025') }}`,
so swapping the data source is a one-line change in `sources.yml`. The three
models are:

- `top_story_by_comments.sql`: top story per month by comment count, using a
  windowed `ROW_NUMBER()` partition over year/month.
- `duckdb_keyword_mentions.sql`: monthly count of stories mentioning `duckdb` in
  the title or text.
- `top_domains.sql`: top 20 story domains, extracted from the URL with
  `regexp_extract`.

`flight.py` is the runner. It validates `DBT_COMMAND` (must be `build`, `run`, or
`test`), shallow-clones `REPO_URL` at `REPO_REF`, writes a runtime `profiles.yml`
from the env knobs, runs `dbt deps` only when `packages.yml`/`dependencies.yml`
exist, optionally runs `dbt seed` (`RUN_DBT_SEED`), runs the dbt command, then
writes the audit row.

The runner guards SQL identifiers that flow into generated profiles and DDL.
`DBT_PROFILE_NAME`, `DBT_SCHEMA`, `DBT_TARGET`, and `AUDIT_SCHEMA` must match a
simple SQL identifier or the Flight fails fast:

```python
def validate_identifier(name: str, value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"{name} must be a simple SQL identifier, got {value!r}")
    return value
```

The audit row is inserted with bound parameters rather than string formatting, so
the repo URL, ref, and command can be arbitrary strings safely:

```python
con.execute(
    f"INSERT INTO {audit_schema}.dbt_flight_runs VALUES (current_timestamp, ?, ?, ?, ?, ?, ?, ?)",
    [repo_url, repo_ref, project_path, profile["profile_name"],
     profile["target"], profile["schema"], command],
)
```

The same runner is published as a reusable template in
[`../dbt-runner`](../dbt-runner); this folder is the worked example of it.

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
| `DBT_TARGET` / `DBT_PROFILE_NAME` (Flight) | Target and profile name written into the runtime `profiles.yml`. | `flight`, `dbt_ingestion_s3` |
| `DBT_THREADS` (Flight) | dbt threads for the run. | `1` |
| `RUN_DBT_SEED` / `DBT_SEED_FULL_REFRESH` (Flight) | Run `dbt seed` before the main command, optionally full-refresh. | `false` (default) |
| `DBT_IS_DUCKLAKE` (Flight) | Write `is_ducklake: true` into the profile when the target database is a DuckLake. | `false` (default) |
| `REPO_URL` / `REPO_REF` / `PROJECT_PATH` (Flight) | Where the Flight gets the dbt code. | `main` for ad hoc; pin a tag or commit for scheduled runs; `PROJECT_PATH=dbt-ingestion-s3` |
| `AUDIT_SCHEMA` (Flight) | Schema for the run audit table. | `flight_audit` -> `flight_audit.dbt_flight_runs` |
| `MOTHERDUCK_HOST` (Flight) | MotherDuck API host for non-prod environments. | unset for prod; set for staging |

## Questions to answer

- What is the source data: which S3/HTTPS path and file, and is it Parquet or CSV?
- Which models are actually needed (keep the three samples, replace them, or select a subset)?
- Target MotherDuck database and schema for the built tables.
- Local DuckDB run, MotherDuck run, or scheduled Flight?
- For a Flight: which `REPO_REF` to pin, what schedule (cron), and `build` vs `run` vs `test`.
- Is there a MotherDuck account and access token available?

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

Create the `prod` database first if it does not exist. dbt does not create the
database for you, and a missing database makes both the `prod` run and the
Flight audit write fail:

```sql
CREATE DATABASE IF NOT EXISTS hacker_news_stats;
```

`profiles.yml` ships two hand-run targets: `local` (a `local.db` DuckDB file) and
`prod` (`md:hacker_news_stats`). The default target is `local`, so a bare
`dbt run` stays on disk. There is no `dev` target; use `local`. The Flight does
not use this `profiles.yml` at all: it generates its own with a `flight` target
(see below).

### Deploy as a Flight

Use the MotherDuck MCP `create_flight` tool to create a Flight from this folder's
`flight.py` and `requirements.txt` (`duckdb`, `dbt-duckdb`). Set the knobs from
"What you'll adjust" as Flight config/env, for example `REPO_REF` (pin a tag or
commit for scheduled runs), `PROJECT_PATH=dbt-ingestion-s3`,
`MOTHERDUCK_DATABASE=hacker_news_stats`, `DBT_SCHEMA=main`, `DBT_COMMAND=build`,
and `AUDIT_SCHEMA=flight_audit`. Add an optional cron schedule (the original
default was daily at `07:45 UTC`, cron `45 7 * * *`). Then run it on demand with
the `run_flight` tool.

The Flight uses its own MotherDuck token, so do not paste a token into the source
or config. To find the token names available to your account:

```sql
SELECT * FROM md_access_tokens();
```

Each run writes one row to `flight_audit.dbt_flight_runs` recording the repo, ref,
project path, profile, target, schema, and command:

| Column | Meaning |
|---|---|
| `run_at` | `TIMESTAMPTZ` of the run |
| `repo_url`, `repo_ref`, `project_path` | The cloned source the dbt project came from |
| `profile_name`, `target_name`, `target_schema` | The runtime profile dbt used |
| `dbt_command` | `build`, `run`, or `test` |

## Files

- [`flight.py`](flight.py): the Flight runner. Installs git, shallow-clones the repo at `REPO_REF`, writes a runtime `profiles.yml` pointed at MotherDuck, optionally runs `dbt deps`/`dbt seed`, runs the dbt command, and writes one audit row to `flight_audit.dbt_flight_runs`.
- [`models/`](models/): the dbt project content. Three table models ([`top_story_by_comments.sql`](models/top_story_by_comments.sql), [`duckdb_keyword_mentions.sql`](models/duckdb_keyword_mentions.sql), [`top_domains.sql`](models/top_domains.sql)) plus [`sources.yml`](models/sources.yml), which declares the public Hacker News S3 Parquet as a dbt source via DuckDB's `external_location`.
- [`dbt_project.yml`](dbt_project.yml): dbt project config (profile name `dbt_ingestion_s3`, models materialized as `table`).
- [`profiles.yml`](profiles.yml): the two hand-run targets, `local` (a `local.db` DuckDB file) and `prod` (`md:hacker_news_stats`). The Flight ignores this and generates its own.
- [`pyproject.toml`](pyproject.toml): Python project metadata for local `uv run` (pins `dbt-duckdb==1.9.3`).
- [`requirements.txt`](requirements.txt): the Flight runtime dependencies (`duckdb==1.5.2`, `dbt-duckdb==1.10.1`), separate from `pyproject.toml`.
- [`uv.lock`](uv.lock): resolved lockfile for the local `uv` environment. [`.python-version`](.python-version) pins Python 3.12.
- [`analyses/`](analyses/), [`macros/`](macros/), [`seeds/`](seeds/), [`snapshots/`](snapshots/), [`tests/`](tests/): standard dbt scaffold directories, empty for now (each holds a `.gitkeep`).

## Caveats

- **The target database must already exist.** dbt does not create it. The `prod`
  target and the Flight audit write both `connect("md:<database>")`, which fails if
  the database is missing. Run `CREATE DATABASE IF NOT EXISTS ...` first.
- **`DBT_COMMAND` is restricted to `build`, `run`, `test`.** Anything else raises
  `ValueError` before dbt is invoked.
- **Identifier env vars are validated.** `DBT_SCHEMA`, `DBT_TARGET`,
  `DBT_PROFILE_NAME`, and `AUDIT_SCHEMA` must be plain SQL identifiers
  (`[A-Za-z_][A-Za-z0-9_]*`). Dotted names, quotes, or spaces fail the run.
- **Pin `REPO_REF` for scheduled Flights.** The default `main` means a scheduled
  run picks up whatever is on `main` at run time. Pin a tag or commit so a push
  cannot silently change what production builds.
- **No `dev` target exists.** `profiles.yml` defines `local` and `prod` only. dbt
  errors on `--target dev`.
- **Version skew between local and Flight.** Local `uv run` resolves dbt-duckdb
  from `pyproject.toml` (`1.9.3`), while the Flight runtime installs
  `requirements.txt` (`dbt-duckdb==1.10.1`, `duckdb==1.5.2`). Align these if a run
  must reproduce exactly.
- **The Flight needs a Debian-based runtime.** `setup()` installs git with
  `apt-get`, so a non-apt base image will not work without editing `flight.py`.
- **Swapping to a private bucket needs a secret.** The default S3 dataset is
  public. Pointing `external_location` at a private bucket requires a DuckDB/
  MotherDuck `SECRET`; the project ships none.
- **Do not put a token in source or config.** The Flight uses its own MotherDuck
  token; the local/`prod` runs read `MOTHERDUCK_TOKEN` from the environment.

## Learn more

- Flights runtime, scheduling, and secrets: run the `get_flight_guide` MCP tool.
- Deeper MotherDuck or DuckDB questions (querying S3, dbt-duckdb behavior): use the
  `ask_docs_question` MCP tool.
