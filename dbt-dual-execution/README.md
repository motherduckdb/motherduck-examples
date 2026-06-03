---
title: dbt Dual Execution Across Local DuckDB and MotherDuck
id: dbt-dual-execution
description: >-
  A dbt-duckdb project that runs the same models against MotherDuck and a local
  DuckDB file in one execution context, using DuckDB's ATTACH so individual
  models can land in the cloud or on disk. Use when you want to develop or
  sample dbt models locally against a local file while still reading from (and
  writing to) MotherDuck, without maintaining two separate projects.
type: example
features: []
tags: [dbt, duckdb, tpc-ds, attach, dual-execution]
---

# dbt Dual Execution Across Local DuckDB and MotherDuck

This is a minimal dbt-duckdb project that shows MotherDuck dual execution: a single dbt run that has both a MotherDuck connection and a local DuckDB file attached at the same time. Because both databases live in one DuckDB execution context, you choose per model where a table materializes by setting its `database` config. The `example/` models hop cloud -> local -> cloud, and the `tpcds/` models read from a MotherDuck source and sample down when the target is local, so you can iterate on transformations cheaply on disk and promote the same code to the cloud unchanged.

## What you'll adjust

| Setting | Purpose | Options / example |
| --- | --- | --- |
| `profiles.yml` target | Which execution context dbt connects to. `local` connects to a local file and attaches all MotherDuck databases; `prod` connects directly to a MotherDuck database. | `--target local` (default) or `--target prod` |
| `local.path` (profiles `local` output) | Path of the on-disk DuckDB file that local materializations write to. | `path: local.db` |
| `local.attach` (profiles `local` output) | What gets attached alongside the local file. `"md:"` attaches every MotherDuck database; narrow it to one with `md:my_db`. | `attach: - path: "md:"` |
| `prod.path` (profiles `prod` output) | The MotherDuck database used when running directly in the cloud. | `path: "md:jdw_dev"` |
| `database=` in `{{ config(...) }}` | Per-model choice of where a table lands: a MotherDuck database name or the local `local` database. | `database="my_db"` (cloud) or `database="local"` |
| `models/tpcds/raw/_sources.yml` | The MotherDuck source the `tpcds` raw models read from. | `database: jdw_dev`, `schema: jdw_tpcds` |
| `{% if target.name == 'local' %}` sampling | Reduces source rows on local runs so iteration is fast; full data runs in the cloud. | `using sample 1 %` in `models/tpcds/raw/store_sales.sql` |
| `dbt_project.yml` model materializations | Default materialization per folder (`example` as views, `tpcds/raw` as tables, `tpcds/queries` as views). | `+materialized: table` / `view`, `+tags: ['raw']` |

## Questions to ask the user

- Which MotherDuck database(s) should models target, and which models should stay local on disk?
- What is the source database and schema the raw models should read from (here `jdw_dev.jdw_tpcds`)?
- Should local runs sample the source data, and at what rate, or read it in full?
- Local-only iteration, cloud-only, or the dual (attach both) setup as configured here?
- Is a MotherDuck token already configured in the shell, or should auth happen via the browser prompt?

## Run it

Prerequisites: a MotherDuck account and (for non-interactive runs) a `MOTHERDUCK_TOKEN` in your shell. The source database and schema referenced in `_sources.yml` must exist in your account, or repoint them to your own tables.

```bash
# install dbt-duckdb into a managed venv
uv sync

# build everything with the default target (local file + attached MotherDuck)
uv run dbt build

# or pick a target explicitly
uv run dbt run --target local   # writes local materializations to local.db, reads MotherDuck
uv run dbt run --target prod    # runs directly against the MotherDuck database in profiles.yml
```

The first cloud run opens a browser prompt for MotherDuck authentication unless `MOTHERDUCK_TOKEN` is already set.

## How it works / Learn more

- `profiles.yml`: the `local` output sets `path: local.db` and `attach: "md:"`, so one DuckDB process holds both the local file and your MotherDuck databases. That single context is what makes per-model `database=` routing possible.
- `models/example/`: `my_first_dbt_model` and `my_third_dbt_model` set `database="my_db"` (cloud); `my_second_dbt_model` sets `database="local"`. Following the `ref()` chain shows data moving cloud -> local -> cloud within one run.
- `models/tpcds/raw/`: thin models that `select from` a MotherDuck source. `store_sales.sql` shows the `target.name == 'local'` guard that samples 1% locally and reads everything in the cloud.
- `models/tpcds/queries/`: the TPC-DS analytical queries (`query_1.sql` ... `query_99.sql`) materialized as views on top of the raw models.
- For deeper MotherDuck or DuckDB questions (ATTACH semantics, dual/hybrid execution, dbt-duckdb config), use the `ask_docs_question` MCP tool or the MotherDuck docs.
