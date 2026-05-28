# dbt Runner Flight template

This template runs a dbt project from a Flight.

At runtime it:

1. Installs `git` with `apt-get`.
2. Clones the configured repository and ref into `/tmp`.
3. Writes a `profiles.yml` file that connects dbt to MotherDuck with the
   Flight's injected `MOTHERDUCK_TOKEN`.
4. Runs `dbt deps` when the project has `packages.yml` or `dependencies.yml`.
5. Optionally runs `dbt seed`.
6. Runs `dbt build`, `dbt run`, or `dbt test`.
7. Writes one audit row to `<AUDIT_SCHEMA>.dbt_flight_runs`.

## Required Flight config

| Key | Default | Description |
|---|---|---|
| `REPO_URL` | `https://github.com/motherduckdb/motherduck-examples.git` | Git repository to clone. |
| `REPO_REF` | `main` | Branch, tag, or commit to clone. Prefer a pinned tag or commit for scheduled Flights. |
| `PROJECT_PATH` | `dbt-ingestion-s3` | Path to the dbt project inside the cloned repository. |
| `DBT_PROFILE_NAME` | `dbt_ingestion_s3` | Profile name from the dbt project. |
| `MOTHERDUCK_DATABASE` | `hacker_news_stats` | Database where dbt builds models. |
| `DBT_SCHEMA` | `main` | Schema used by the generated dbt profile. |

## Optional Flight config

| Key | Default | Description |
|---|---|---|
| `DBT_COMMAND` | `build` | One of `build`, `run`, or `test`. |
| `DBT_TARGET` | `flight` | dbt target name written to `profiles.yml`. |
| `DBT_THREADS` | `1` | dbt thread count. |
| `DBT_SELECT` | unset | Optional dbt selector. |
| `DBT_EXCLUDE` | unset | Optional dbt exclude selector. |
| `RUN_DBT_SEED` | `false` | Whether to run `dbt seed` before the main command. |
| `DBT_SEED_FULL_REFRESH` | `false` | Whether to pass `--full-refresh` to `dbt seed`. |
| `DBT_IS_DUCKLAKE` | `false` | Whether to add `is_ducklake: true` to the generated dbt profile. |
| `AUDIT_SCHEMA` | `flight_audit` | Schema for the run audit table. |
| `MOTHERDUCK_HOST` | unset | Optional MotherDuck API host. Set this for staging or other non-production environments. |
