# Run the DuckLake dbt project from a Flight

This folder contains a standalone Flight wrapper for the `dbt-ducklake`
example.

The Flight installs `git`, clones this repository, writes a runtime
`profiles.yml` with `is_ducklake: true`, runs `dbt build`, and writes one row to
`flight_audit.dbt_flight_runs`.

Before running it, create the `dbt_ducklake` database as a DuckLake database
with your own `DATA_PATH`. The SQL file also creates the `my_db` analytics
database used by the query models.

This Flight is on-demand by default because the TPC-DS project is heavier than
the smaller scheduled examples.
