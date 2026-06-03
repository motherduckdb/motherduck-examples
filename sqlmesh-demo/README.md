---
title: Transform Stock Data with SQLMesh on MotherDuck
id: sqlmesh-demo
description: >-
  Loads Yahoo Finance stock data into MotherDuck with dlt, then transforms it
  through interim, conformed, and mart layers with SQLMesh (incremental,
  SCD type 2, audits, virtual data environments). Use when you want a SQLMesh
  project on MotherDuck, or a dlt-to-SQLMesh ELT pipeline to adapt.
type: example
features: []
tags: [sqlmesh, dlt, yfinance, stocks, elt, incremental, scd, uv]
---

# Transform Stock Data with SQLMesh on MotherDuck

This example loads daily stock prices, company info, and option chains from Yahoo Finance into a MotherDuck database with [dlt](https://dlthub.com/), then transforms the raw `dlt` tables into analytics models with [SQLMesh](https://sqlmesh.com/). It shows the MotherDuck pattern of pointing a SQLMesh `motherduck` gateway at a cloud database and using SQLMesh model kinds (incremental by time range, SCD type 2, full, view) plus column-level audits to build a layered warehouse, all running against MotherDuck compute.

## What you'll adjust

| Setting | Purpose | Options / example |
| --- | --- | --- |
| `load/symbols.txt` | The tickers the dlt pipeline fetches, one per line | `MSFT`, `AAPL`, `NVDA`, ... swap for your own list |
| `dataset_name` in `load/stock_data_pipeline.py` | Schema the raw dlt tables land in | `"stock_data"` (referenced by SQLMesh models as `stock_data.<table>`) |
| `destination` in `load/stock_data_pipeline.py` | Where dlt writes the raw data | `"motherduck"` (configured via `.dlt/secrets.toml`) |
| `start_date` window in `stock_history_resource` | How far back history is pulled | `datetime.now() - timedelta(days=360)` |
| `gateways.local.connection.database` in `transform/config.yaml` | MotherDuck database SQLMesh reads and writes | `dlt_test_db` (must match the dlt destination database) |
| `model_defaults.start` in `transform/config.yaml` | Default backfill start for time-based models | `2024-12-08` |
| `start` in `transform/models/interim/stock_history.sql` | Backfill start for the incremental price model | `'2023-01-01'` |
| Model `kind` per model | Materialization strategy | `INCREMENTAL_BY_TIME_RANGE` (history), `SCD_TYPE_2_BY_TIME` (info/options), `FULL`, `VIEW` |
| `cron` per model | How often SQLMesh refreshes the model | `'@daily'` on the interim and conformed models |
| `audits (...)` per model | Data quality checks enforced at run time | `UNIQUE_COMBINATION_OF_COLUMNS`, `NOT_NULL`, `UNIQUE_VALUES` |
| `transform/external_models.yaml` | Declares the raw dlt tables (and their columns) as external sources | regenerate with `sqlmesh create_external_models` after a load |

## Questions to ask the user

- Which tickers or source dataset should the pipeline load (replace `symbols.txt`, or swap dlt for a different source)?
- What MotherDuck database and schema are the target (set both the dlt `destination` database and `transform/config.yaml` `database`, keep them in sync)?
- Full refresh or incremental, and what is the backfill start date for time-based models?
- How often should models run (the `@daily` cron, or a different cadence)?
- Where will the MotherDuck token come from (the `MOTHERDUCK_TOKEN` env var and `.dlt/secrets.toml`)?

## Run it

Prerequisites: a [MotherDuck account](https://app.motherduck.com/) and a service token, plus [uv](https://docs.astral.sh/uv/getting-started/installation/).

```bash
# from sqlmesh-demo/
uv sync                       # create the venv and install dlt + sqlmesh + yfinance

# point dlt at MotherDuck (token goes in .dlt/secrets.toml; see dlt docs link below)
# then load the raw stock data into MotherDuck:
uv run python load/stock_data_pipeline.py

# transform with SQLMesh (run from the transform/ dir)
export MOTHERDUCK_TOKEN=<your-token>
uv run sqlmesh -p transform info     # verify the connection and project state
uv run sqlmesh -p transform plan     # preview and apply; type 'y' to push the changes

# optional: open the SQLMesh web UI (the [web] extra is installed)
uv run sqlmesh -p transform ui
```

dlt destination setup for MotherDuck (the `.dlt/secrets.toml` database and token) is documented in the [dlt MotherDuck destination guide](https://dlthub.com/docs/dlt-ecosystem/destinations/motherduck#setup-guide).

## How it works / Learn more

- `load/stock_data_pipeline.py`: a dlt pipeline with three resources (`stock_info`, `stock_options`, `stock_history`) that fetch from `yfinance` and write to the MotherDuck `stock_data` schema.
- `transform/models/interim/`: typed, cleaned models over the raw dlt tables, including an `INCREMENTAL_BY_TIME_RANGE` price history model and `SCD_TYPE_2_BY_TIME` models that track changes to company info and option chains over time.
- `transform/models/conformed/` and `transform/models/mart/`: business-ready views and the `stock_price_by_day` mart that joins shares outstanding to daily close for a market-cap series.
- `transform/external_models.yaml`: the contract for the upstream dlt tables; regenerate it whenever the raw schema changes.
- SQLMesh concepts used here (virtual data environments, model kinds, audits, cron): see the [SQLMesh docs](https://sqlmesh.readthedocs.io/).
- Deeper MotherDuck or DuckDB SQL questions: run the `ask_docs_question` MCP tool or check the [MotherDuck docs](https://motherduck.com/docs/).
