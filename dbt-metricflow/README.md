---
title: Define and Query Metrics with dbt MetricFlow on MotherDuck
id: dbt-metricflow
description: >-
  A dbt project that defines a semantic layer with MetricFlow over an orders
  fact table, then queries the metrics with the mf CLI against local DuckDB or
  MotherDuck. Use when you want one metric definition (revenue, order count,
  derived ratios) that runs identically on a local file and in the cloud.
type: example
features: []
tags: [dbt, metricflow, semantic-layer, duckdb, metrics]
---

# Define and Query Metrics with dbt MetricFlow on MotherDuck

This example defines metrics once in a dbt semantic model and queries them with the MetricFlow `mf` CLI. The same project runs against a local DuckDB file or MotherDuck just by switching the dbt target, because dbt-duckdb uses the same code path for both. It shows the MotherDuck pattern of writing portable dbt models and a portable semantic layer, then promoting from local to cloud with one environment change.

## What you'll adjust

| Setting | Purpose | Options / example |
| --- | --- | --- |
| `metricflow-example/profiles.yml` `local.path` | Local DuckDB file the project builds into | `ecommerce_local.duckdb` (default) |
| `metricflow-example/profiles.yml` `motherduck.path` | MotherDuck database name to build into | `md:ecommerce_test_db`, change to your own `md:<db>` |
| `MOTHERDUCK_TOKEN` (or `motherduck_token`) env var | Auth for the MotherDuck target | your MotherDuck access token |
| `DBT_TARGET` env var | Selects which `profiles.yml` output to use | unset (uses `local`) or `motherduck` |
| `DBT_PROFILES_DIR` env var | Where dbt finds `profiles.yml` | `..` when run from `ecommerce_metrics/` |
| `mf query --metrics` | Which metric(s) to compute | `revenue`, `orders`, `customers`, `avg_order_value`, `revenue_per_customer` |
| `mf query --group-by` | Dimension(s) to slice by | `metric_time__month`, `metric_time__day`, `order_id__status` |
| `models/semantic_models.yml` | Semantic model: entities, dimensions, measures, metrics | add/edit measures and metrics here to define new KPIs |
| `models/fct_orders.sql` + `seeds/raw_orders.csv` | The fact table and its source rows | swap the seed and fact model for your own grain/columns |
| `models/metricflow_time_spine.sql` | Date spine backing time dimensions | `generate_series` range, currently `2024-01-01` to `2025-12-31` |

## Questions to ask the user

- What is the source fact table and its grain (one row per order, event, session)?
- Which metrics matter (sums, counts, distinct counts, derived ratios) and what are their labels?
- What time grain and date range should the time spine cover?
- Run against local DuckDB only, MotherDuck only, or both (promote local to cloud)?
- For MotherDuck: which database name, and is a MotherDuck token available?

## Run it

Prerequisites: Python with `uv`, and (for the cloud target) a MotherDuck account plus token from the [MotherDuck UI](https://app.motherduck.com) under Settings, Access Tokens.

Install dependencies from `requirements.txt` (`dbt-core`, `dbt-duckdb`, `dbt-metricflow`):

```bash
cd dbt-metricflow
uv venv
uv pip install -r requirements.txt
```

Local DuckDB, run from `metricflow-example/ecommerce_metrics/`:

```bash
cd metricflow-example/ecommerce_metrics
DBT_PROFILES_DIR=.. uv run dbt seed
DBT_PROFILES_DIR=.. uv run dbt run
DBT_PROFILES_DIR=.. uv run mf query --metrics revenue,orders,customers --group-by metric_time__month
```

MotherDuck, same project with the cloud target:

```bash
export MOTHERDUCK_TOKEN='your_token_here'
# First time only: create the target database
uv run python -c "import duckdb; duckdb.connect('md:').execute('CREATE DATABASE ecommerce_test_db')"

DBT_PROFILES_DIR=.. uv run dbt seed --target motherduck
DBT_PROFILES_DIR=.. uv run dbt run --target motherduck
DBT_PROFILES_DIR=.. DBT_TARGET=motherduck uv run mf query --metrics revenue,orders,customers --group-by metric_time__month
```

## How it works / Learn more

`mf query` reads the metrics and dimensions in `models/semantic_models.yml`, renders DuckDB SQL, and runs it through the dbt-duckdb adapter against whichever target you pick. MotherDuck uses the same connection path as local DuckDB with a different connection string, so the generated SQL is identical.

- [EXAMPLES.md](EXAMPLES.md): many more `mf query` patterns (single/multiple metrics, time ranges, status breakdowns, `--explain` to see the SQL, adding custom metrics).
- [models/semantic_models.yml](metricflow-example/ecommerce_metrics/models/semantic_models.yml): the full semantic model (entities, dimensions, measures, metrics) to copy from.
- For deeper MotherDuck or DuckDB questions (connection strings, hybrid execution, SQL behavior), use the `ask_docs_question` MCP tool or see the [MotherDuck docs](https://motherduck.com/docs).
- MetricFlow CLI reference: [dbt MetricFlow commands](https://docs.getdbt.com/docs/build/metricflow-commands).
