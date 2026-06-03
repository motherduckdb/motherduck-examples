---
title: Run Any dbt Project as a MotherDuck Flight
id: dbt-runner
description: >-
  A generic, reusable Flight that clones a dbt project from git, writes a runtime
  MotherDuck profile, runs dbt (build/run/test), and records an audit row. Use when
  you want to schedule or trigger an existing dbt project against MotherDuck without
  packaging your own runner, especially as the starting point for a custom dbt Flight.
type: template
features: [flights]
tags: [dbt, git, scheduling]
---

# Run Any dbt Project as a MotherDuck Flight

This is the base plan that the other dbt Flight examples are adapted from. It shows the MotherDuck pattern for running dbt in a Flight: at runtime it installs `git`, shallow-clones a repo and ref, generates a `profiles.yml` pointing at MotherDuck (using the Flight's injected `MOTHERDUCK_TOKEN`), optionally runs `dbt deps` and `dbt seed`, then runs `dbt build`, `run`, or `test` and writes one row to an audit table. Everything is driven by environment variables, so an agent adapts it by setting Flight config rather than editing `flight.py`.

## What you'll adjust

Every knob is an environment variable read in `flight.py`. Adjust them as Flight config/env, not by editing code.

| Setting | Purpose | Options / example |
|---|---|---|
| `REPO_URL` | Git repository to clone the dbt project from. | `https://github.com/motherduckdb/motherduck-examples.git` |
| `REPO_REF` | Branch, tag, or commit to clone. Pin a tag or commit for scheduled runs. | `main`, `v1.2.0`, `a1b2c3d` |
| `PROJECT_PATH` | Path to the dbt project inside the cloned repo. | `dbt-ingestion-s3`, `analytics/dbt` |
| `DBT_COMMAND` | Which dbt command to run. | `build` (default), `run`, `test` |
| `DBT_PROFILE_NAME` | Profile name written to `profiles.yml`; must match the project's `dbt_project.yml`. Validated as a SQL identifier. | `dbt_ingestion_s3` |
| `MOTHERDUCK_DATABASE` | MotherDuck database dbt builds into (`md:<database>`). | `hacker_news_stats`, `analytics_prod` |
| `DBT_SCHEMA` | Schema for the generated profile. Validated as a SQL identifier. | `main`, `marts` |
| `DBT_TARGET` | dbt target name in the generated profile. Validated as a SQL identifier. | `flight` (default), `prod` |
| `DBT_THREADS` | dbt thread count. | `1` (default), `4` |
| `DBT_SELECT` | Optional `--select` selector to limit models. | `tag:daily`, `staging+` |
| `DBT_EXCLUDE` | Optional `--exclude` selector. | `tag:slow` |
| `RUN_DBT_SEED` | Run `dbt seed` before the main command. | `false` (default), `true` |
| `DBT_SEED_FULL_REFRESH` | Pass `--full-refresh` to `dbt seed`. | `false` (default), `true` |
| `DBT_IS_DUCKLAKE` | Add `is_ducklake: true` to the generated profile for DuckLake projects. | `false` (default), `true` |
| `AUDIT_SCHEMA` | Schema for the `dbt_flight_runs` audit table. Validated as a SQL identifier. | `flight_audit` (default) |
| `MOTHERDUCK_TOKEN` | MotherDuck access token. Injected by the Flight when you select a token; you do not set this manually. | (Flight-managed) |
| `MOTHERDUCK_HOST` | Optional MotherDuck API host for non-production environments. | unset (default), staging host |

## Questions to ask the user

- Which dbt project and where does it live (`REPO_URL`, `REPO_REF`, `PROJECT_PATH`)?
- What is the project's profile name in `dbt_project.yml` (`DBT_PROFILE_NAME`)?
- Which MotherDuck database and schema should models build into (`MOTHERDUCK_DATABASE`, `DBT_SCHEMA`)? Does the database already exist?
- Which command and scope: `build`, `run`, or `test`, and any `--select`/`--exclude` (`DBT_COMMAND`, `DBT_SELECT`, `DBT_EXCLUDE`)?
- Does the project need seeds run first, and full vs incremental (`RUN_DBT_SEED`, `DBT_SEED_FULL_REFRESH`)?
- Is the target storage native MotherDuck or DuckLake (`DBT_IS_DUCKLAKE`)?
- On what schedule should it run, and which MotherDuck token/account?

## Run it

Prerequisites: a MotherDuck account and access token, and the target database already created in MotherDuck.

This plan is built to run as a Flight (it installs `git` via `apt-get` and writes `profiles.yml` at runtime). To test the logic locally, set the same environment variables and run it with the dependencies in `requirements.txt`:

```bash
export MOTHERDUCK_TOKEN=your_token
export REPO_URL=https://github.com/motherduckdb/motherduck-examples.git
export REPO_REF=main
export PROJECT_PATH=dbt-ingestion-s3
export DBT_PROFILE_NAME=dbt_ingestion_s3
export MOTHERDUCK_DATABASE=hacker_news_stats
export DBT_SCHEMA=main

uv run --with-requirements requirements.txt python flight.py
```

Note: `flight.py` runs `apt-get` to install `git`, which assumes the Flight Debian runtime. Locally, ensure `git` is already installed (skip the `apt-get` step if you adapt it for local use).

### Deploy as a Flight

1. Create the Flight from this folder using the MotherDuck MCP `create_flight` tool, passing the contents of `flight.py` as the source and `requirements.txt` as the dependencies.
2. Set the knobs from "What you'll adjust" as the Flight's config/env (at minimum `REPO_URL`, `REPO_REF`, `PROJECT_PATH`, `DBT_PROFILE_NAME`, `MOTHERDUCK_DATABASE`, `DBT_SCHEMA`; plus any of `DBT_COMMAND`, `DBT_SELECT`, `RUN_DBT_SEED`, `DBT_IS_DUCKLAKE`, etc.).
3. Select a MotherDuck access token on the Flight so `MOTHERDUCK_TOKEN` is injected at runtime.
4. Optionally add a schedule (for example, daily) so dbt runs on a cadence. Pin `REPO_REF` to a tag or commit for scheduled runs.
5. Trigger it with the MotherDuck MCP `run_flight` tool, then check `<AUDIT_SCHEMA>.dbt_flight_runs` in MotherDuck for the recorded run.

## How it works / Learn more

- `flight.py`: the full runner. Read `write_profile()` for the generated `profiles.yml` shape (including the DuckLake branch), `clone_repo()` for the shallow-clone steps, and `record_audit()` for the `dbt_flight_runs` table schema.
- `requirements.txt`: pins `duckdb` and `dbt-duckdb` versions used in the runtime.
- For Flight runtime details, scheduling, and secrets/token handling, run the `get_flight_guide` MCP tool.
- For deeper MotherDuck or DuckDB questions (DuckLake profiles, dbt-duckdb specifics), use the `ask_docs_question` MCP tool.
