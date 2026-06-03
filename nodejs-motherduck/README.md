---
title: Connect to MotherDuck from Node.js with the DuckDB Neo Driver
id: nodejs-motherduck
description: >-
  Two Node.js scripts that connect to MotherDuck with the native DuckDB Neo
  driver (@duckdb/node-api): a basic query walkthrough and a generic-pool
  connection pool. Use when you want to run DuckDB SQL against MotherDuck from a
  Node.js app or service that can ship the DuckDB binary, including concurrent
  workloads that need pooled connections.
type: example
features: []
tags: [nodejs, javascript, duckdb-neo, connection-pool, generic-pool, nyc-taxi]
---

# Connect to MotherDuck from Node.js with the DuckDB Neo Driver

Two example scripts that connect to MotherDuck using the [DuckDB Neo driver](https://duckdb.org/docs/clients/node_neo/overview) (`@duckdb/node-api`). `src/basic.js` walks through simple queries, table creation, parameterized statements, aggregations, and querying the public `sample_data.nyc.taxi` table. `src/connection-pool.js` builds a `generic-pool` over MotherDuck connections for concurrent queries. The key MotherDuck pattern here: unlike the Python, R, JDBC, and ODBC clients, the Node.js client does not cache database instances automatically, so both scripts use `DuckDBInstance.fromCache('md:<db>', { motherduck_token })` to avoid reinitializing the MotherDuck extension and re-fetching catalog metadata on every connection.

## What you'll adjust

| Setting | Purpose | Options / example |
| --- | --- | --- |
| `MOTHERDUCK_TOKEN` (env, in `.env`) | MotherDuck access token, the credential passed as `motherduck_token` | Copy `.env.template` to `.env` and set the token |
| `MOTHERDUCK_DATABASE` (env, in `.env`) | Database the connection string `md:<db>` targets | Defaults to `my_db` in both scripts; set to your own database name |
| SQL in `src/basic.js` | The example queries that run | Replace the `example_users` DDL/DML and the `sample_data.nyc.taxi` reads with your own schema and tables |
| `bindInteger(1, 2)` in `src/basic.js` | Value bound to the parameterized query | Use the matching `bind*` method for your parameter type |
| Pool sizing in `src/connection-pool.js` | Min/max pooled connections | `min: 2`, `max: 5` |
| Eviction / recycle timers in `src/connection-pool.js` | Idle cleanup and connection recycling | `softIdleTimeoutMillis: 60000`, `idleTimeoutMillis: 120000`, `recycleTimeoutMillis: 300000` |
| `SET THREADS='1'` in the pool factory | Threads per pooled connection so they don't compete for CPU | Raise or drop depending on concurrency vs per-query speed |
| `queries` array in `src/connection-pool.js` | The concurrent queries run through the pool | Replace the `sample_data.nyc.taxi` aggregates with your own SQL |

## Questions to ask the user

- Which MotherDuck database and schema should the scripts target (default is `my_db`, sample queries read `sample_data.nyc`)?
- Is a MotherDuck access token available, and where should it live (`.env` for these scripts)?
- Is this a one-shot script or a long-running service: does it need the connection pool, and at what concurrency (min/max, recycle timers)?
- What queries or tables should replace the `example_users` and `nyc.taxi` samples?
- Does the runtime ship the DuckDB binary (native driver), or does it need the Postgres endpoint instead (see `cloudflare-workers` for that pattern)?

## Run it

Prerequisites: Node.js 22+ (uses native ESM modules), npm, and a MotherDuck account with an access token.

```bash
# Install dependencies
npm install

# Copy the env template and add your token
cp .env.template .env
# Edit .env: set MOTHERDUCK_TOKEN, optionally MOTHERDUCK_DATABASE

# Run the basic query walkthrough
npm run basic

# Run the connection-pool example with concurrent queries
npm run pool
```

## How it works / Learn more

- `src/basic.js`: connects with `DuckDBInstance.fromCache('md:<db>', { motherduck_token })`, then demonstrates a simple query, `CREATE`/`INSERT`/`DROP`, a parameterized `prepare()` + `bindInteger()`, an aggregate, a read from `sample_data.nyc.taxi`, and reuse of the cached instance for a second connection.
- `src/connection-pool.js`: defines an `MDConnectionFactory` whose `create()` uses `fromCache()` (so all pooled connections share one cached instance) and sets `THREADS='1'`, with `validate()` recycling stale connections; `createPool` from `generic-pool` then runs four queries concurrently.
- Use `fromCache()` rather than `DuckDBInstance.create()` to avoid reinitializing the MotherDuck extension and re-fetching catalog metadata on each connection; this is specific to the Node.js client.
- For the Node.js connection guide and connection pooling docs, see https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/connecting-to-motherduck/ and the [multithreading guide](https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/multithreading-and-parallelism/multithreading-and-parallelism-nodejs/). For deeper MotherDuck or DuckDB SQL questions, use the `ask_docs_question` MCP tool.
