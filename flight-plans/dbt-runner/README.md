---
title: Run Any dbt Project as a MotherDuck Flight
id: dbt-runner
description: >-
  A generic, reusable Flight that clones a dbt project from git, writes a runtime
  MotherDuck profile, runs dbt (build/run/test), and records an audit row. Use when
  you want to schedule or trigger an existing dbt project against MotherDuck without
  packaging your own runner, especially as the starting point for a custom dbt Flight.
type: template
features: [flights, ducklake]
tags: [dbt]
---

# Run Any dbt Project as a MotherDuck Flight

This is the base plan that the other dbt Flight examples are adapted from. It shows the MotherDuck pattern for running dbt in a Flight: at runtime it installs `git`, shallow-clones a repo and ref, generates a `profiles.yml` pointing at MotherDuck (using the Flight's injected `MOTHERDUCK_TOKEN`), optionally runs `dbt deps` and `dbt seed`, then runs `dbt build`, `run`, or `test` and writes one row to an audit table. Everything is driven by environment variables, so you adapt it by setting Flight config rather than editing `flight.py`.

## How it works

`flight.py` runs a fixed sequence; the env vars in "What you'll adjust" only change its inputs:

1. `apt-get update` and `apt-get install git`, then wipe and recreate the work root `/tmp/motherduck-flight-dbt-runner`.
2. Shallow-clone the repo with an explicit `git init` + `remote add` + `fetch --depth 1` + detached checkout, so it can target a branch, tag, or commit without a full clone.
3. Generate `profiles.yml` inside the project directory pointing at MotherDuck.
4. Run `dbt deps` only when the project has a `packages.yml` or `dependencies.yml`.
5. Optionally run `dbt seed` (with `--full-refresh` if requested).
6. Run `dbt build`, `run`, or `test` with the resolved `--target`, `--profiles-dir .`, and any `--select`/`--exclude`.
7. Connect to `md:<MOTHERDUCK_DATABASE>` and write one audit row.

### The generated profile

`write_profile()` writes a minimal `dbt-duckdb` profile that connects to MotherDuck through the native DuckDB `md:` path (not the Postgres endpoint). The token comes from the environment, so it never appears in the file:

```yaml
<DBT_PROFILE_NAME>:
  outputs:
    <DBT_TARGET>:
      type: duckdb
      path: "md:<MOTHERDUCK_DATABASE>"
      schema: <DBT_SCHEMA>
      threads: <DBT_THREADS>
      # is_ducklake: true   # only when DBT_IS_DUCKLAKE is set
  target: <DBT_TARGET>
```

For DuckLake projects, set `DBT_IS_DUCKLAKE=true` to add the `is_ducklake: true` line so dbt-duckdb attaches the database as a DuckLake catalog.

### The audit table

`record_audit()` creates `<AUDIT_SCHEMA>.dbt_flight_runs` if needed and appends one row per run, so you have a history of what ran against the database:

```sql
CREATE TABLE IF NOT EXISTS <AUDIT_SCHEMA>.dbt_flight_runs (
    run_at        TIMESTAMPTZ,
    repo_url      VARCHAR,
    repo_ref      VARCHAR,
    project_path  VARCHAR,
    profile_name  VARCHAR,
    target_name   VARCHAR,
    target_schema VARCHAR,
    dbt_command   VARCHAR
);
```

## Security

Two patterns keep the dynamic SQL safe, and adaptations should preserve both:

- **Identifier validation.** `DBT_PROFILE_NAME`, `DBT_SCHEMA`, `DBT_TARGET`, and `AUDIT_SCHEMA` flow into the profile and into `CREATE SCHEMA` / `CREATE TABLE` statements, which cannot be parameterized. They are checked against `^[A-Za-z_][A-Za-z0-9_]*$` first, so anything that is not a plain SQL identifier raises before any SQL runs:

  ```python
  def validate_identifier(name: str, value: str) -> str:
      if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
          raise ValueError(f"{name} must be a simple SQL identifier, got {value!r}")
      return value
  ```

- **Parameterized data.** Values stored in the audit table (repo URL, ref, paths, command) are written with bound parameters, never string-formatted into SQL:

  ```python
  con.execute(
      "INSERT INTO ... VALUES (current_timestamp, ?, ?, ?, ?, ?, ?, ?)",
      [repo_url, repo_ref, project_path, profile["profile_name"],
       profile["target"], profile["schema"], command],
  )
  ```

Do not put the MotherDuck token in `profiles.yml` or in Flight config. Select a token on the Flight so `MOTHERDUCK_TOKEN` is injected at runtime and read from the environment.

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

## Questions to answer

Gather these before adapting the plan:

- Which dbt project and where does it live (`REPO_URL`, `REPO_REF`, `PROJECT_PATH`)?
- What is the project's profile name in `dbt_project.yml` (`DBT_PROFILE_NAME`)?
- Which MotherDuck database and schema should models build into (`MOTHERDUCK_DATABASE`, `DBT_SCHEMA`)? Does the database already exist?
- Which command and scope: `build`, `run`, or `test`, and any `--select`/`--exclude` (`DBT_COMMAND`, `DBT_SELECT`, `DBT_EXCLUDE`)?
- Does the project need seeds run first, and full vs incremental (`RUN_DBT_SEED`, `DBT_SEED_FULL_REFRESH`)?
- Is the target storage native MotherDuck or DuckLake (`DBT_IS_DUCKLAKE`)?
- On what schedule should it run, and which MotherDuck token/account?

## Run it

Prerequisites: a MotherDuck account and access token, and the target database already created in MotherDuck (the runner does not create it; only the audit schema is created automatically).

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

Note: `flight.py` runs `apt-get` to install `git`, which assumes the Flight Debian runtime. Locally, ensure `git` is already installed and skip the `apt-get` step if you adapt it for local use. The runner also deletes and recreates `/tmp/motherduck-flight-dbt-runner` on every run, so do not point it at a directory you care about.

### Deploy as a Flight

1. Create the Flight from this folder using the MotherDuck MCP `create_flight` tool, passing the contents of `flight.py` as the source and `requirements.txt` as the dependencies.
2. Set the knobs from "What you'll adjust" as the Flight's config/env (at minimum `REPO_URL`, `REPO_REF`, `PROJECT_PATH`, `DBT_PROFILE_NAME`, `MOTHERDUCK_DATABASE`, `DBT_SCHEMA`; plus any of `DBT_COMMAND`, `DBT_SELECT`, `RUN_DBT_SEED`, `DBT_IS_DUCKLAKE`, etc.).
3. Select a MotherDuck access token on the Flight so `MOTHERDUCK_TOKEN` is injected at runtime.
4. Optionally add a schedule (for example, daily) so dbt runs on a cadence. Pin `REPO_REF` to a tag or commit for scheduled runs.
5. Trigger it with the MotherDuck MCP `run_flight` tool, then check `<AUDIT_SCHEMA>.dbt_flight_runs` in MotherDuck for the recorded run.

## Files

- [`flight.py`](flight.py) - the Flight runner: installs `git`, shallow-clones the repo/ref, generates a MotherDuck `profiles.yml`, optionally runs `dbt deps`/`dbt seed`, runs `dbt build`/`run`/`test`, and writes one audit row. Key functions are `write_profile()`, `clone_repo()`, and `record_audit()`.
- [`requirements.txt`](requirements.txt) - runtime dependencies, pinning `duckdb==1.5.2` and `dbt-duckdb==1.10.1`.

## Caveats

- **The target database must already exist.** The runner only creates the audit schema. If `MOTHERDUCK_DATABASE` does not exist, dbt fails when it connects.
- **Identifier-validated knobs reject dashes and dots.** `DBT_PROFILE_NAME`, `DBT_SCHEMA`, `DBT_TARGET`, and `AUDIT_SCHEMA` must be plain SQL identifiers (`[A-Za-z_][A-Za-z0-9_]*`). Values like `my-schema` or `analytics.prod` raise a `ValueError` before dbt runs. Database names in `MOTHERDUCK_DATABASE` are not validated, so quote them yourself if they are unusual.
- **`DBT_PROFILE_NAME` must match `dbt_project.yml`.** The generated `profiles.yml` uses this name as its top-level key. If it does not match the project's `profile:`, dbt cannot find the profile and exits, often with a confusing "Could not find profile" error.
- **Blank env vars fall back to defaults silently.** `env()` strips whitespace and returns the default for empty strings, so an empty `PROJECT_PATH` quietly becomes `dbt-ingestion-s3`. Set values explicitly rather than blanking them.
- **`REPO_REF` must be fetchable by SHA for commit pins.** The clone uses `git fetch --depth 1 origin <ref>`. Branches and tags always work; a specific commit only works if the server allows fetching that object directly. If a commit pin fails, fall back to a tag or branch.
- **Debian runtime assumption.** `setup()` calls `apt-get`, so the runner expects the Flight's Debian-based runtime. It will fail on non-Debian local environments unless you remove that step.
- **`DBT_COMMAND` is restricted.** Only `build`, `run`, and `test` are accepted; anything else raises before cloning. There is no hook for `dbt snapshot`, `dbt compile`, or arbitrary commands without editing `flight.py`.
- **No state between runs.** This is a stateless runner with no `--defer`/`--state` wiring. Incremental models still work through MotherDuck, but cross-run dbt artifacts are not persisted.

## Learn more

- `flight.py`: the full runner. Read `write_profile()` for the generated `profiles.yml` shape (including the DuckLake branch), `clone_repo()` for the shallow-clone steps, and `record_audit()` for the audit table schema.
- `requirements.txt`: pins the `duckdb` and `dbt-duckdb` versions used in the runtime.
- For Flight runtime details, scheduling, and secrets/token handling, run the `get_flight_guide` MCP tool.
- For deeper MotherDuck or DuckDB questions (DuckLake profiles, dbt-duckdb specifics), use the `ask_docs_question` MCP tool.
