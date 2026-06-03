---
title: Query MotherDuck from Cloudflare Workers
id: cloudflare-workers
description: >-
  A Cloudflare Worker that queries MotherDuck over the Postgres wire protocol
  with the node-postgres (pg) driver, no DuckDB binary required. Use when you
  want a serverless HTTP API or edge endpoint that reads from MotherDuck and
  returns JSON.
type: example
features: [pg_endpoint]
tags: [cloudflare, cloudflare-workers, typescript, pg, nyc-taxi, rest-api]
---

# Query MotherDuck from Cloudflare Workers

A single-file Cloudflare Worker (`src/index.ts`) that connects to MotherDuck through the Postgres wire protocol endpoint using the `pg` driver and a connection string, then serves query results as JSON. This shows the pattern for reaching MotherDuck from an edge or serverless runtime where you cannot ship the DuckDB binary: connect with a Postgres client, authenticate with your MotherDuck token, and run parameterized SQL. The sample queries the `nyc.taxi` table in the public `sample_data` database that every MotherDuck account has.

## What you'll adjust

| Setting | Purpose | Options / example |
| --- | --- | --- |
| `MOTHERDUCK_TOKEN` (secret) | MotherDuck access token, the password in the connection string | Set via `npx wrangler secret put MOTHERDUCK_TOKEN`; locally via `.dev.vars` |
| `MOTHERDUCK_HOST` (`[vars]` in `wrangler.toml`) | Postgres endpoint host | `pg.us-east-1-aws.motherduck.com` (US), `pg.eu-central-1-aws.motherduck.com` (EU) |
| `MOTHERDUCK_DB` (`[vars]` in `wrangler.toml`) | Database the connection targets | `sample_data`; change to your own database name |
| `name` (`wrangler.toml`) | Worker / deployment name | `motherduck-taxi-stats` |
| SQL in `src/index.ts` | The queries served by each route | Replace `nyc.taxi` queries with your own schema, table, and columns |
| Routes in `src/index.ts` | URL paths the Worker handles | `/` (recent trips), `/stats?start=&end=` (aggregates); add or rename to fit your API |
| Query parameters | Inputs accepted on a route | `start`, `end` (validated as `YYYY-MM-DD`, passed as numbered `$1`/`$2` params) |

## Questions to ask the user

- Which MotherDuck database and schema should the Worker read from (default is `sample_data` / `nyc`)?
- Which region is the MotherDuck organization in, US or EU, so the right `MOTHERDUCK_HOST` is set?
- What routes and queries does the API need to expose, and what inputs do they accept?
- How should request inputs be validated and bound (this example uses a regex check plus numbered parameters)?
- Where will the MotherDuck token live (Wrangler secret for deploy, `.dev.vars` for local)?

## Run it

Prerequisites: Node.js 18+, a Cloudflare account, and a MotherDuck account with an access token.

```sh
npm install

# Set the token as a Worker secret (creates the Worker if it does not exist)
npx wrangler secret put MOTHERDUCK_TOKEN

# Local development: put the token in a .dev.vars file first
#   MOTHERDUCK_TOKEN="ey...MY_TOKEN"
npx wrangler dev

# Deploy to Cloudflare
npx wrangler deploy
```

Routes once running:

- `GET /` returns the 20 most recent trips from `nyc.taxi`.
- `GET /stats?start=YYYY-MM-DD&end=YYYY-MM-DD` returns total passengers and total fare for the date range. Both `start` and `end` are required.

## How it works / Learn more

- `src/index.ts`: builds the connection string `postgresql://anyusername:<token>@<host>:5432/<db>?sslmode=require`, connects with `pg`, routes on `url.pathname`, and runs parameterized SQL. The username is ignored by MotherDuck; the token is the credential and `sslmode=require` is mandatory.
- `wrangler.toml`: sets `compatibility_flags = ["nodejs_compat"]` (required for the `pg` driver) and the non-secret `[vars]` `MOTHERDUCK_HOST` and `MOTHERDUCK_DB`.
- Input safety: untrusted query parameters are validated with a `YYYY-MM-DD` regex and then bound as numbered parameters (`$1`, `$2`) rather than string-interpolated into the SQL.
- For the connection-string format, supported regions, and endpoint behavior, see the MotherDuck Cloudflare Workers interface docs at https://motherduck.com/docs/getting-started/interfaces/serverless/cloudflare-workers/ or ask the `ask_docs_question` MCP tool for deeper MotherDuck or DuckDB questions.
