---
title: Ingest API Data into MotherDuck with Python
id: python-ingestion
description: >-
  Fetches JSON from an HTTP API and loads it into a MotherDuck table using the
  native DuckDB Python client, with a simple pandas path and a typed, chunked
  PyArrow path. Use when you need a standalone Python ingestion script that pulls
  from an API or in-memory dataframe and writes to MotherDuck.
type: example
features: []
tags: [python, duckdb, pyarrow, pandas, requests, github-api]
---

# Ingest API Data into MotherDuck with Python

This example pulls contributor stats from the GitHub API and loads them into a
MotherDuck database from a plain Python process. It connects with the native
DuckDB Python client (`duckdb.connect()` then `ATTACH 'md:'`) and shows two
ingestion patterns: a quick pandas `CREATE TABLE AS SELECT` for small payloads,
and a typed PyArrow buffer that inserts in chunks for larger loads while avoiding
type-inference surprises.

## What you'll adjust

| Setting | Purpose | Options / example |
| --- | --- | --- |
| `motherduck_token` (in `.env`) | Authenticates the DuckDB client to MotherDuck. Copy `.env.template` to `.env` and set it. | A MotherDuck access token |
| `url` in `fetch_github_data()` | The API endpoint to ingest. Swap for your own source. | `https://api.github.com/repos/duckdb/duckdb/stats/contributors` |
| `process_data()` body | Maps the API JSON into rows. Rewrite for your payload's shape. | builds `login`, `total_commits` records |
| `CREATE DATABASE IF NOT EXISTS github` | Target MotherDuck database. | any database name |
| Target table name | Where rows land: `github.github_commits` (small) / `github.github_commits_large` (large). | `<database>.<table>` |
| Explicit schema (large script) | The PyArrow + DuckDB column types for typed ingestion. | `[("login", pa.string()), ("total_commits", pa.int64())]` |
| `chunk_size` (large script) | Rows per INSERT batch. Set to 10 in the demo; use a larger value in production. | `100000` is a good default |

## Questions to ask the user

- What is the source: an HTTP API, a local file, or an in-memory dataframe?
- Which MotherDuck database and table should the data land in?
- Small payload (pandas, one shot) or larger payload (typed PyArrow, chunked)?
- Full refresh (create/replace) or append to an existing table?
- How often should it run, and where will the `motherduck_token` come from?

## Run it

Prerequisites: a MotherDuck account and access token, Python 3.12, and `uv`.
Copy `.env.template` to `.env` and set `motherduck_token`.

```bash
# from this folder
make load-md-small   # pandas path: load_to_motherduck_small.py
make load-md-large   # typed PyArrow chunked path: load_to_motherduck_large.py
make test            # run the PyArrow buffer tests
```

The same scripts run directly via `uv`:

```bash
uv run python -m python_ingestion.load_to_motherduck_small
uv run python -m python_ingestion.load_to_motherduck_large
uv run pytest tests
```

## How it works / Learn more

- `python_ingestion/load_to_motherduck_small.py`: fetch, build a pandas
  DataFrame, then `CREATE TABLE ... AS SELECT * FROM df`. Smallest viable path.
- `python_ingestion/load_to_motherduck_large.py`: defines `ArrowTableLoadingBuffer`,
  a typed PyArrow buffer that slices the table into `chunk_size` batches and
  runs `INSERT INTO ... SELECT * FROM buffer_table` per chunk. Read this when
  the source is too large for a single load or you want explicit column types.
- `tests/test_load_to_motherduck_large.py`: shows the buffer chunking behavior
  against an in-memory DuckDB database.
- For loading-data options beyond this script (object storage, secrets, CTAS vs
  INSERT tradeoffs) and deeper MotherDuck or DuckDB questions, use the
  `ask_docs_question` MCP tool.
