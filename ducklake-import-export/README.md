## DuckLake Import/Export: Move Metadata, Keep Data Put

You have data in S3 (Parquet/Iceberg). You have a DuckLake that points at it. Sometimes you want that DuckLake’s metadata to live in MotherDuck, and sometimes you want it local (backed by Postgres) for development, testing, or offline work. These scripts help you move the catalog back and forth without touching a single byte of your S3 data.

- `ducklake-export.py`: MotherDuck → local (Postgres-backed) DuckLake
- `ducklake-import.py`: local (Postgres-backed) DuckLake → MotherDuck

Why this matters: DuckLake cleanly separates metadata (schemas, tables, configuration) from data (your files in S3). Migrating “state” is fast and safe because we copy catalog metadata, not the data itself. Practical, efficient, and — dare we say — it quacks just right.

### Who this is for

- Data engineers: you’ll feel at home — Python, DuckDB SQL, and Postgres.
- Data scientists: you don’t need to be a DB admin. Treat this as “sync the catalog to where I am,” so you can explore or test without moving data.

## What you’ll need

- Python 3.12+
- A MotherDuck account and a DuckDB session that can connect to it (e.g., `MOTHERDUCK_TOKEN` set)
- An S3 bucket and prefix that will be your  `DATA_PATH`
- Local Postgres (via Homebrew on MacOS) or any reachable Postgres for the local catalog.
  - Docker can also be used, i.e. via `docker run postgres-16 ...`
- uv (Python package manager). From this folder, set up the environment with:

```bash
uv sync
```

Environment variables (export these in your shell):

- `AWS_REGION` (default: `us-east-1`)
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_SESSION_TOKEN` (optional, recommended for temporary creds)
- `DATA_PATH` — S3 URI for your DuckLake (e.g., `s3://my-bucket/my-prefix/`)
- `PGUSER` — your local Postgres user (on macOS this is usually `$(whoami)`)

Quick MotherDuck check:

```bash
duckdb -c "select md_version();" | cat
```

Optional local Postgres (for testing):

```bash
brew install postgresql@16
brew services run postgresql@16
```

Both scripts default to `is_local_test = True`, which safely drops and recreates a local DB named `ducklake_catalog` for a clean run.

## Quick start: Export (MotherDuck → local)

```bash
uv run ducklake-export.py
```

What happens:

1. Creates a scoped S3 secret in MotherDuck for your `DATA_PATH` and (re)creates a DuckLake `my_md_ducklake`.
2. If `is_local_test=True`, creates a tiny demo table (`nl_train_stations`) so you have something to validate with.
3. Exports the MotherDuck metadata DB (`__ducklake_metadata_my_md_ducklake`) to a local DuckDB file `local_duckdb__ducklake_metadata_my_md_ducklake.duckdb`.
4. Creates a local DuckLake attached to Postgres (same `DATA_PATH`).
5. Copies the exported metadata tables into the local Postgres-backed catalog.
6. Validates with a count/insert when `is_local_test=True`.

End result: your local DuckLake mirrors MotherDuck’s catalog and points at the same S3 data.

## Quick start: Import (local → MotherDuck)

```bash
uv run ducklake-import.py
```

What happens:

1. Attaches the local DuckLake (Postgres-backed) that points to your S3 `DATA_PATH`.
2. If `is_local_test=True`, creates the same tiny demo table (`nl_train_stations`).
3. Copies the local metadata DB (`__ducklake_metadata_my_ducklake`) into a local DuckDB file `local_duckdb__ducklake_metadata_my_ducklake.duckdb`.
4. Connects to MotherDuck, (re)creates `my_md_ducklake` with the same `DATA_PATH`.
5. Creates a transient database in MotherDuck from the local file and bulk-copies all metadata tables into `__ducklake_metadata_my_md_ducklake`.
6. Creates a scoped S3 secret in MotherDuck for the `DATA_PATH`.
7. Validates with a count/insert when `is_local_test=True`.

End result: your MotherDuck DuckLake mirrors the local catalog and points at the same S3 data.

## How this works (in plain terms)

- DuckLake is a catalog that points at files in S3. The catalog state lives in a database (Postgres locally, a system DB in MotherDuck).
- We move catalog metadata (schemas, table bindings), not data files. That’s why this is fast and low risk.
- S3 credentials are stored as secrets and can be scoped to the exact `DATA_PATH` so the catalog only sees what it needs.

## Configure to your taste

- Change names: edit `local_ducklake_name` and `md_ducklake_name` in the scripts.
- Postgres bits: `pg_ducklake_dbname` (default `ducklake_catalog`) and `PGUSER` control the local metadata store.
- Safer runs: set `is_local_test = False` to avoid destructive local resets and skip demo data.

## Troubleshooting (common bumps)

- MotherDuck auth: set `MOTHERDUCK_TOKEN` and confirm `select md_version();` works in DuckDB.
- S3 AccessDenied/403: double-check `AWS_*` creds and that secret `scope` matches your `DATA_PATH`.
- `DATA_PATH` format: use a fully-qualified S3 URI with a trailing slash, e.g., `s3://bucket/prefix/`.
- Postgres connection: start it (`brew services run postgresql@16`), verify `PGUSER`, and ensure local trust auth.
- DuckDB extensions: `force install ducklake from core` and `install postgres` need network access.

## Clean up

- Local files created: `local_duckdb__ducklake_metadata_<name>.duckdb` (and `.wal`). Delete any time; scripts recreate them.
