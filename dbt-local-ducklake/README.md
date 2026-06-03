---
title: Run dbt on a Local DuckLake Catalog
id: dbt-local-ducklake
description: >-
  A dbt-duckdb project that materializes TPC-H parquet into a DuckLake catalog,
  using local Postgres or SQLite for metadata and a folder of parquet files for
  storage. Use when you want a lakehouse-style catalog (table versioning,
  snapshots, file compaction) for local dbt development that mirrors a managed
  DuckLake on MotherDuck.
type: example
features: [ducklake]
tags: [dbt, ducklake, duckdb, tpc-h, parquet, postgres, sqlite]
---

# Run dbt on a Local DuckLake Catalog

This project reads TPC-H parquet files as dbt sources and materializes them as tables in a DuckLake catalog. DuckLake is a DuckDB extension that adds Iceberg-style catalog management (snapshots, table versioning, file compaction) on top of plain parquet, with the metadata kept in a separate Postgres or SQLite database. The same `attach` pattern points at a fully managed DuckLake on MotherDuck by swapping one target, so you can develop against a local catalog and ship to `md:`.

## What you'll adjust

| Setting | Purpose | Options / example |
| --- | --- | --- |
| `target` (profiles.yml) | Which DuckLake backend dbt runs against | `local` (Postgres metadata, ships as default) or `motherduck` (attaches `md:jdw_ducklake`); add a `local_sqlite` target for the zero-dependency SQLite path |
| `attach.path` (profiles.yml) | The DuckLake catalog connection string | `ducklake:postgres:`, `ducklake:sqlite:ducklake.db`, or `md:<your_ducklake_db>` with `is_ducklake: true` |
| `attach.options.data_path` | Where DuckLake writes its parquet data files (local targets) | `data_files` (default); any local or object-storage path |
| `secrets.ducklake_secret` (profiles.yml) | Postgres metadata store for the `local` target | `host: localhost`, `port: 5432`, `database: ducklake_catalog` |
| `extensions.ducklake.repo` | DuckLake extension channel | `core_nightly` (current); pin to a release once available |
| `external_location` (models/tpch/raw/_sources.yml) | Where raw parquet sources are read from | `data/{name}.parquet`; repoint to your own files or an `s3://`/`https://` path |
| `tables` (models/tpch/raw/_sources.yml) | Which source tables dbt knows about | the 8 TPC-H tables; replace with your own source list |
| `models.dbt_local_ducklake.tpch` (dbt_project.yml) | Where models land in the catalog | `raw` -> `catalog.raw` (tables), `queries` -> `catalog.prep` (tables) |
| TPC-H scale factor | Size of the generated benchmark data | `--scale-factor 10` (~10GB); lower it (e.g. `1`) for a fast local run |
| `maintain_ducklake()` (macros/ducklake_maintenance.sql) | Compaction and snapshot cleanup cadence | merges adjacent files, then `ducklake_expire_snapshots(... older_than => now() - INTERVAL '1 minute')` and `ducklake_cleanup_old_files`; tune the interval |

## Questions to ask the user

- Which DuckLake backend: local Postgres, local SQLite, or a managed catalog on MotherDuck (`md:`)?
- What is the source data: keep TPC-H, or repoint `external_location` to their own parquet / object-storage path?
- What scale factor should the benchmark data use (full 10GB vs a small dev sample)?
- Which target database and schema should models materialize into (defaults: `catalog.raw` and `catalog.prep`)?
- For the local Postgres target, what are the metadata store credentials (host, port, database)?
- Should DuckLake maintenance (compaction, snapshot expiry) run, and on what cadence?

## Run it

Prerequisites: `uv`, and the metadata backend for your chosen target. The default `local` target needs a reachable Postgres with a `ducklake_catalog` database; the SQLite path needs nothing extra. The `motherduck` target needs a MotherDuck account and `motherduck_token` set in the environment.

```bash
# Install dbt-core, dbt-duckdb, duckdb, and tpchgen-cli into a managed venv
uv sync

# Generate TPC-H source parquet into data/ (drop --scale-factor for a smaller set)
uv run tpchgen-cli --scale-factor 10 --output-dir data --format=parquet

# Build the raw tables and queries into the DuckLake catalog
uv run dbt build

# Run against MotherDuck-managed DuckLake instead of the local backend
uv run dbt build --target motherduck

# Build a single model
uv run dbt run --select customer
```

Run the maintenance macro on demand to compact files and expire old snapshots:

```bash
uv run dbt run-operation maintain_ducklake
```

## How it works / Learn more

- `profiles.yml`: the three DuckLake attach patterns (local Postgres, local SQLite shown in the prior README copy, and MotherDuck managed). The catalog is always aliased `catalog`, so models are backend-agnostic.
- `models/tpch/raw/_sources.yml`: `external_location: data/{name}.parquet` is the dbt-duckdb pattern for reading parquet directly as sources, with no upstream load step.
- `macros/ducklake_maintenance.sql`: discovers the DuckLake alias from `target.attach` and calls `merge_adjacent_files`, `ducklake_expire_snapshots`, and `ducklake_cleanup_old_files`.
- `macros/schema.sql`: `generate_schema_name` is overridden to use schema names verbatim, so `+schema: raw` lands in `catalog.raw` rather than `<target>_raw`.
- `models/tpch/queries/`: the 22 standard TPC-H analytical queries, materialized as tables in `catalog.prep`.
- For when a managed DuckLake on MotherDuck is the right call (BYOB storage, own-compute access, file-aware maintenance), see the `motherduck-ducklake` skill. For deeper DuckLake or DuckDB SQL questions, use the `ask_docs_question` MCP tool.
