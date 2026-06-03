---
title: Build Churn Prediction Features with dbt
id: dbt-churn-prediction
description: >-
  dbt builds customer churn feature and label tables from raw customer,
  membership, usage, and payment history, plus a Python script that trains and
  scores a churn model on top. Use when you need a SQL-first churn feature
  pipeline on MotherDuck that can run on a schedule, with model training kept as
  a separate step.
type: example
features: [flights]
tags: [dbt, churn, feature-engineering, machine-learning, scikit-learn, duckdb]
---

# Build Churn Prediction Features with dbt

This example splits churn prediction into two jobs: a dbt project that turns raw
customer history into a point-in-time feature matrix, churn labels, and a
warehouse-side daily score table, and a Python script that trains and calibrates
a scikit-learn model on those tables. The MotherDuck pattern it shows is a
SQL-first feature pipeline you build once and refresh on a schedule (locally,
in MotherDuck, or from a Flight), with model training as a downstream workflow
rather than something baked into the warehouse.

## What you'll adjust

| Setting | Purpose | Options / example |
| --- | --- | --- |
| `MOTHERDUCK_DATABASE` (env / profiles.yml) | Target MotherDuck database for the `prod`/`flight` build | `subscription_churn` (default) |
| `MOTHERDUCK_TOKEN` (env / `.env`) | Auth for MotherDuck runs | your account token |
| profiles.yml target | Where dbt builds: local DuckDB file vs MotherDuck | `local` (`local.db`) or `prod` (`md:<db>`) |
| `vars.churn_as_of_date` (dbt_project.yml) | The "today" the daily score table is computed against | `'2026-04-15'` |
| `vars.member_churn_grace_period_days` (dbt_project.yml) | Days of inactivity before a member counts as churned | `30` |
| dbt model selector | Which models the build/refresh touches | `tag:churn_daily+` (staging + marts) or `--exclude resource_type:seed` for all |
| seeds (`seeds/raw_*.csv`) | Sample raw inputs to swap for your own customer, membership, usage, payment data | `raw_customers`, `raw_memberships`, `raw_usage_events`, `raw_payments` |
| `--source` (training script) | Training data source | `ibm_telco` (runs immediately) or `dbt` (your built tables) |
| `--write-schema` (training script) | Schema for Python prediction/metric tables written back | `science` (default) |
| Flight env: `PROJECT_PATH`, `DBT_SELECT`, `DBT_EXCLUDE`, `RUN_DBT_SEED`, `DBT_SEED_FULL_REFRESH`, `DBT_TARGET`, `DBT_SCHEMA`, `DBT_PROFILE_NAME`, `REPO_REF`, `AUDIT_SCHEMA` | Knobs the Flight wrapper reads to clone, profile, and run dbt | see "Deploy as a Flight" |

## Questions to ask the user

- How is churn defined for this business (cancellation window, inactivity window)? This sets the label and `member_churn_grace_period_days`.
- What are the source tables for customers, subscriptions/memberships, usage/activity, and payments, and where do they live?
- Target MotherDuck database and schema for the feature tables (default `subscription_churn`).
- Full refresh each run, or incremental? (Current models rebuild as tables; seeds use `--full-refresh`.)
- What "as of" date should the daily score table use (`churn_as_of_date`)?
- Should this run on a schedule, and at what cadence?
- MotherDuck token / credentials for cloud and Flight runs.
- Train on the bundled IBM Telco benchmark first, or straight on your own dbt-built history?

## Run it

Prerequisites: a MotherDuck account and token for cloud or Flight runs. Local
DuckDB runs need no account. The project uses `uv`.

```sh
# install dbt, DuckDB, pandas, scikit-learn, lifelines, etc.
uv sync

# build the feature/label/score tables locally
uv run dbt seed --profiles-dir . --full-refresh
uv run dbt build --profiles-dir . --exclude resource_type:seed

# inspect the current warehouse-side score table
uv run dbt show --profiles-dir . --select fct_customer_churn_scores_daily

# train and evaluate a model on the IBM Telco benchmark
uv run python scripts/train_python_churn_models.py --source ibm_telco
```

To build in MotherDuck instead, copy `.env.example` to `.env`, set
`MOTHERDUCK_TOKEN` and `MOTHERDUCK_DATABASE`, then:

```sh
uv run dbt seed --profiles-dir . --target prod --full-refresh
uv run dbt build --profiles-dir . --target prod --select tag:churn_daily+ --exclude resource_type:seed
```

Once you have enough real history, train on the dbt-built feature matrix and
optionally write predictions back:

```sh
uv run python scripts/train_python_churn_models.py --source dbt --database "md:${MOTHERDUCK_DATABASE}" --write-schema science
```

The script refuses `--source dbt` on the tiny bundled sample on purpose: it is
too small for useful machine learning.

### Deploy as a Flight

`flight.py` is the generic dbt-runner: it installs `git`, clones the repo at
`REPO_REF`, writes a runtime `profiles.yml` pointed at your MotherDuck database,
optionally seeds, runs the selected dbt command, and writes one audit row to
`flight_audit.dbt_flight_runs`. This Flight covers only the warehouse
feature-refresh part of the workflow; Python model training and scoring stay a
separate step.

1. Create a Flight from this folder's `flight.py` and `requirements.txt` using
   the MotherDuck MCP `create_flight` tool.
2. Set the knobs from "What you'll adjust" as Flight config/env so it runs the
   churn refresh:
   - `PROJECT_PATH=dbt-churn-prediction`
   - `MOTHERDUCK_DATABASE=subscription_churn` (your target)
   - `DBT_PROFILE_NAME=dbt_churn_prediction`
   - `RUN_DBT_SEED=true`, `DBT_SEED_FULL_REFRESH=true`
   - `DBT_SELECT=tag:churn_daily+`, `DBT_EXCLUDE=resource_type:seed`
   - `REPO_REF=main` for testing; pin to a tag or commit for scheduled runs.
3. This reproduces, inside the runtime:

   ```sh
   dbt seed --target flight --profiles-dir . --full-refresh
   dbt build --target flight --profiles-dir . --select tag:churn_daily+ --exclude resource_type:seed
   ```

4. Optionally add a schedule (the prior recipe used daily at `07:15 UTC`).
5. Run it with the MCP `run_flight` tool, then check `flight_audit.dbt_flight_runs`
   for the audit row.

## How it works / Learn more

- Models live in `models/staging/` (views) and `models/marts/` (tables). The
  training input is `fct_customer_features_historical`, the target is
  `fct_customer_churn_labels`, and `fct_customer_churn_scores_daily` is the
  warehouse-side baseline score for the current eligible population.
- Feature logic is in `macros/churn_features.sql`; data-quality assertions are in
  `tests/`.
- The Python training and scoring workflow is `scripts/train_python_churn_models.py`:
  it loads the dataset, splits train/validation/test, preprocesses, trains
  logistic regression plus comparison models, calibrates the winner, evaluates
  (`roc_auc`, `average_precision`, `brier_score`), and writes artifacts under
  `artifacts/python_models/`. Read that script before adapting the model side.
- Flight runtime, scheduling, and secrets: run the `get_flight_guide` MCP tool.
- Deeper MotherDuck or DuckDB questions: use the `ask_docs_question` MCP tool.
