# Run this dbt project from a Flight

This folder contains a standalone Flight wrapper for the `dbt-ingestion-s3`
example.

Use it when you want MotherDuck to run the dbt project on demand or on a
schedule without maintaining a separate scheduler.

## Files

- `flight.py`: Python source for the Flight.
- `requirements.txt`: Python packages installed in the Flight runtime.
- `create_flight.sql`: SQL that creates the Flight from the MotherDuck SQL
  editor or any DuckDB client connected to MotherDuck.

## Flow

The Flight installs `git`, clones this repository, writes a runtime
`profiles.yml`, runs dbt, and writes one row to
`flight_audit.dbt_flight_runs`.

For scheduled Flights, set `REPO_REF` in `create_flight.sql` to a tag or commit
instead of `main`.

If you run this against staging or another non-production MotherDuck
environment, set `MOTHERDUCK_HOST` in the Flight config to that environment's
API host.
