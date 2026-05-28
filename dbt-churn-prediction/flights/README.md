# Run the churn dbt feature refresh from a Flight

This folder contains a standalone Flight wrapper for the
`dbt-churn-prediction` dbt feature refresh.

The Flight installs `git`, clones this repository, writes a runtime
`profiles.yml`, runs `dbt seed --full-refresh`, runs the `tag:churn_daily+`
dbt build while excluding seed resources, and writes one row to
`flight_audit.dbt_flight_runs`.

This Flight covers the warehouse feature-refresh part of the churn example. The
Python model training script remains a separate workflow.

For scheduled Flights, set `REPO_REF` in `create_flight.sql` to a tag or commit
instead of `main`.
