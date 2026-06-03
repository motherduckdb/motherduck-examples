---
title: Bridge Local Postgres and MotherDuck with pg_duckdb
id: postgres-demo
description: >-
  Run a local Postgres (the pg_duckdb image) wired to MotherDuck, then move data
  both ways: scan Postgres tables from the DuckDB CLI and query MotherDuck
  tables from psql. Use when you want a hybrid Postgres plus MotherDuck setup
  where one side reads the other, not a serverless app talking to MotherDuck.
type: example
features: []
tags: [postgres, pg_duckdb, docker, duckdb-cli, psql, ctas]
---

# Bridge Local Postgres and MotherDuck with pg_duckdb

This example runs a local Postgres using the `pgduckdb/pgduckdb` Docker image, which embeds DuckDB inside Postgres, and connects it to MotherDuck with a token. It shows two directions of the same hybrid pattern: from the DuckDB CLI you attach the local Postgres with the `postgres` extension (the pg scanner) and read its tables, and from inside the container you use `psql` against pg_duckdb to query MotherDuck tables. The MotherDuck pattern here is interoperability: MotherDuck and Postgres reading each other's tables in one query, with `CREATE TABLE AS SELECT` (CTAS) used to replicate data across the boundary. Note this is the pg_duckdb / pg scanner path, not the MotherDuck Postgres wire endpoint.

## What you'll adjust

| Setting | Purpose | Options / example |
| --- | --- | --- |
| `MOTHERDUCK_TOKEN` (container env) | Lets pg_duckdb authenticate to MotherDuck | Your MotherDuck access token, passed with `-e MOTHERDUCK_TOKEN="..."` |
| `POSTGRES_PASSWORD` (container env) | Password for the local Postgres `postgres` user | `very_secur3_pw`; change and reuse it in the ATTACH string |
| `-p 5432:5432` (docker run) | Host port the local Postgres listens on | Remap the left side if 5432 is taken, e.g. `-p 5433:5432` |
| `duckdb.motherduck_enabled=true` (container flag) | Turns on the MotherDuck integration in pg_duckdb | Keep `true` to reach MotherDuck from psql |
| ATTACH connection string (DuckDB CLI) | Where the DuckDB CLI finds the local Postgres | `dbname=postgres user=postgres password=... host=127.0.0.1` |
| Source table (`pg.public.winelist`) | The Postgres table you scan from DuckDB | Replace `winelist` with your own table and schema |
| Target database / schema | Where CTAS writes the replicated data | A MotherDuck database for PG to MD, or `public` in Postgres for MD to PG |

## Questions to ask the user

- Which direction do they need: read Postgres from MotherDuck/DuckDB, read MotherDuck from Postgres, or both?
- Which source table(s) in Postgres or MotherDuck should be moved, and to which target database and schema?
- Is this a one-time copy (CTAS) or do they expect to query the two systems live in a hybrid query?
- Which MotherDuck region and token should the container use, and where is that token stored?
- Is the local Postgres password and host the default from this example, or have they changed them?

## Run it

Prerequisites: Docker, the [DuckDB CLI](https://duckdb.org/docs/installation/?version=stable&environment=cli&download_method=package_manager), and a MotherDuck account with an access token.

```bash
# 1. Start the pg_duckdb container, wired to MotherDuck (replace your_token)
docker run -d -p 5432:5432 \
  -e POSTGRES_PASSWORD="very_secur3_pw" \
  -e MOTHERDUCK_TOKEN="your_token" \
  pgduckdb/pgduckdb:17-main -c duckdb.motherduck_enabled=true
```

Read the local Postgres from the DuckDB CLI (the pg scanner):

```sql
-- start with: duckdb
INSTALL postgres;
LOAD postgres;

ATTACH 'dbname=postgres user=postgres password=very_secur3_pw host=127.0.0.1' AS pg (TYPE POSTGRES);
SHOW ALL TABLES;
SELECT * FROM pg.public.winelist;

-- Exercise: CTAS to replicate Postgres data into DuckDB / MotherDuck
-- CREATE TABLE my_db.winelist AS SELECT * FROM pg.public.winelist;
```

Read MotherDuck from inside the container with pg_duckdb:

```bash
docker ps                              # find the container name
docker exec -it <container_name> /bin/bash
psql
```

```sql
SELECT * FROM public.winelist;

-- Exercise: CTAS to replicate MotherDuck data into Postgres, then write a
-- hybrid query that joins Postgres and MotherDuck tables together.
```

## How it works / Learn more

- Loading data from Postgres into MotherDuck: [MotherDuck docs](https://motherduck.com/docs/key-tasks/loading-data-into-motherduck/loading-data-md-postgres/).
- pg_duckdb (DuckDB embedded in Postgres) and the MotherDuck integration flag: the [pg_duckdb project](https://github.com/duckdb/pg_duckdb).
- The DuckDB `postgres` extension used by the pg scanner: [DuckDB Postgres extension docs](https://duckdb.org/docs/extensions/postgres).
- For deeper MotherDuck or DuckDB questions, run the `ask_docs_question` MCP tool.
