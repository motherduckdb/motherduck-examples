---
title: Query MotherDuck from Vercel and Next.js
id: vercel-nextjs
description: >-
  Next.js API routes that query MotherDuck over the Postgres wire protocol with
  the node-postgres driver, no DuckDB binary needed. Use when building a Vercel
  (or any Node serverless) backend that reads MotherDuck data through pooled,
  parameterized SQL.
type: example
features: [pg_endpoint]
tags: [vercel, nextjs, postgres, node-postgres, serverless, nyc-taxi]
---

# Query MotherDuck from Vercel and Next.js

This is a Next.js app whose API routes connect to MotherDuck through the Postgres
wire protocol endpoint using the `pg` (node-postgres) driver. It shows the
serverless-friendly MotherDuck pattern: a module-level connection pool reused
across warm function invocations, SSL certificate verification, input validation,
and parameterized queries against the public `sample_data.nyc.taxi` dataset.

## What you'll adjust

| Setting | Purpose | Options / example |
| --- | --- | --- |
| `MOTHERDUCK_TOKEN` env var | MotherDuck access token used as the connection password | Service-account token from your MotherDuck settings |
| `MOTHERDUCK_HOST` env var | Postgres endpoint host, selects the region | `pg.us-east-1-aws.motherduck.com` (default), `pg.eu-central-1-aws.motherduck.com` for EU |
| `MOTHERDUCK_DB` env var | Database in the connection string | `sample_data` (default), or your own database |
| `connectionString` in `src/lib/motherduck.ts` | How the pool authenticates, fixed at port `5432` | `postgresql://user:${token}@${host}:5432/${db}` |
| Pool options in `src/lib/motherduck.ts` | Pooling behavior for serverless concurrency | `max: 10`, `idleTimeoutMillis: 5000` |
| `ssl` option in `src/lib/motherduck.ts` | TLS verification level | `{ rejectUnauthorized: true }` (equivalent to `sslmode=verify-full`) |
| SQL in `src/app/api/trips/route.ts` | The "recent trips" query and row limit | Change `FROM nyc.taxi`, columns, `LIMIT 20` |
| SQL in `src/app/api/stats/route.ts` | The aggregate query and its `$1`/`$2` date params | Swap the table, columns, and the `datePattern` validation regex |

## Questions to ask the user

- Which MotherDuck database and schema should the routes read from (default is `sample_data.nyc.taxi`)?
- Which region is the account in, so the right `MOTHERDUCK_HOST` is set (US vs EU)?
- What tables and columns do the API routes need to expose, and what query parameters drive them?
- How will the token be provisioned in production: manual `vercel env add` or the MotherDuck Native Integration on Vercel?
- What concurrency is expected, so the pool `max` and idle timeout can be tuned?

## Run it

Prerequisites: Node.js v18+, a MotherDuck account and access token, and (for deploy) a Vercel account.

```sh
npm install
cp .env.local.example .env.local   # then set MOTHERDUCK_TOKEN
npm run dev                         # http://localhost:3000
```

Try the routes:

```text
GET /api/trips
GET /api/stats?start=2022-11-01&end=2022-12-01
```

Build and deploy to Vercel:

```sh
npm run build
npx vercel deploy
npx vercel env add MOTHERDUCK_TOKEN   # if not using the MotherDuck Native Integration
```

If you install the [MotherDuck Native Integration](https://vercel.com/marketplace/motherduck) on Vercel, the access token is injected as an environment variable automatically.

## How it works / Learn more

- `src/lib/motherduck.ts`: the shared `pg.Pool` plus `attachDatabasePool` from `@vercel/functions`, which cleans up idle connections before a function instance is suspended.
- `src/app/api/trips/route.ts` and `src/app/api/stats/route.ts`: the two API handlers, including the `YYYY-MM-DD` regex validation and the parameterized `$1`/`$2` aggregate so untrusted input never lands in raw SQL.
- Postgres endpoint reference: [authenticating and connecting via the Postgres endpoint](https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/postgres-endpoint). The endpoint listens on port `5432`; pick the host for your region.
- For deeper MotherDuck or DuckDB SQL questions, use the `ask_docs_question` MCP tool or the MotherDuck docs.
