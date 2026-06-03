---
title: Capture Vercel Log Drain Traffic and Classify AI Agents in MotherDuck
id: vercel-agent-analytics
description: >-
  A Vercel Function that receives Vercel log drain batches, verifies the HMAC
  signature, classifies AI crawlers and agents from the user agent and referer,
  and writes one bulk INSERT per batch into a native MotherDuck table.
  Use when you want to measure AI bot, agent, and AI-referred human traffic to a
  Vercel-hosted site and query it live from a Dive or any SQL tool, with no AWS or S3.
type: example
features: []
tags: [vercel, nodejs, typescript, log-drain, ai-bots, duckdb-node-api]
---

# Capture Vercel Log Drain Traffic and Classify AI Agents in MotherDuck

A single Vercel Function (`api/drain.ts`) is registered as a Vercel log drain endpoint. Vercel POSTs NDJSON batches of request logs to it; the function verifies the HMAC-SHA1 signature, parses each line, classifies it against the rules in `bots.yaml` (crawler, agent, or human-via-AI), drops static asset requests, and writes the whole batch in one `INSERT` to a native MotherDuck table. It connects directly with `@duckdb/node-api` using a MotherDuck token, and bootstraps the database, schema, table, and an `ai_requests` view on the first cold start. The MotherDuck pattern this shows: an edge collector that lands raw web traffic in a native table you can read live from a Dive or BI tool, with classification kept in code so you can reclassify history in SQL.

## What you'll adjust

| Setting | Purpose | Options / example |
|---|---|---|
| `MOTHERDUCK_TOKEN` (env, required) | MotherDuck access token used by the DuckDB Node API connection | a token from the MotherDuck UI |
| `VERCEL_DRAIN_SECRET` (env, required) | HMAC secret to verify drain deliveries; paste the same value into the Vercel log drain config | any high-entropy string, e.g. `dev-secret` for local testing |
| `MD_DESTINATION` (env, optional) | Target database and schema in `<database>.<schema>` form | default `agent_analytics.raw`; e.g. `web_analytics.ingest` |
| `MD_TABLE` (env, optional) | Target table name | default `vercel_request_logs` |
| `MD_DATABASE` / `MD_SCHEMA` (env, optional) | Legacy split form of the destination, used only when `MD_DESTINATION` is unset | defaults `agent_analytics` / `raw` |
| `BOTS_ONLY` (env, optional) | When `true`, drop rows with no classifier match so only AI traffic is stored | default `false`; set `true` once you only care about AI traffic |
| `bots.yaml` (`user_agent_patterns`, `referer_patterns`) | Source of truth for AI classification; substring match on user agent then referer | add or edit `pattern` / `name` / `category` rows; categories are `crawler`, `agent`, `human_via_ai` |
| `IGNORED_PATH_EXTENSIONS` + `/_next/image` check (`src/handler.ts`) | Static asset paths dropped before insert | extend the extension set or path checks to filter more routes |
| `anonymizeIp` (`src/handler.ts`) | Zeroes the last IPv4 octet before insert | remove or change if you need full IPs or IPv6 handling |
| `maxDuration` / `includeFiles` (`vercel.json`) | Function timeout and which non-code files get bundled | `maxDuration: 30`; `includeFiles: "bots.yaml"` |
| `PORT` (env, local only) | Port for the local dev harness `src/local-server.ts` | default `8787` |

## Questions to ask the user

- Which Vercel project's traffic should be captured, and do you have rights to create a log drain on it?
- Target MotherDuck database and schema (`MD_DESTINATION`) and table name (`MD_TABLE`)?
- Do you want all traffic for a baseline, or AI-only (`BOTS_ONLY`)?
- Which AI crawlers, agents, and AI referers matter to you, so `bots.yaml` can be tuned?
- Should client IPs be anonymized (the default) or kept in full?
- MotherDuck token and a shared drain secret: where will they be stored as Vercel environment variables?

## Run it

Prerequisites: a MotherDuck account and access token, a Vercel project with log drain creation rights, and Node 20+.

Local dev (no deploy):

```bash
npm install
export VERCEL_DRAIN_SECRET=dev-secret
export MOTHERDUCK_TOKEN=...
npm run dev            # starts src/local-server.ts on :8787
./scripts/test-local.sh   # signs and POSTs scripts/sample-payload.ndjson
```

Type check:

```bash
npm run typecheck
```

Deploy the function to Vercel:

```bash
npm install
vercel                 # deploy; prints https://<project>.vercel.app/api/drain
```

Then in Vercel project settings: set `MOTHERDUCK_TOKEN` and `VERCEL_DRAIN_SECRET` (plus any of the optional knobs above), and under Settings : Log Drains : Add, create an `NDJSON` drain pointing at the `/api/drain` URL with the same custom secret as `VERCEL_DRAIN_SECRET`. The function creates the database, schema, table, and `ai_requests` view on the first request, so no manual SQL step is needed. Verify against the deployed endpoint:

```bash
./scripts/test-local.sh https://<project>.vercel.app/api/drain
```

Then query MotherDuck (adjust to your configured destination and table):

```sql
SELECT * FROM agent_analytics.raw.vercel_request_logs
ORDER BY event_ts DESC LIMIT 20;
```

## How it works / Learn more

- `sql/01_setup.sql`: reference DDL for the table and `ai_requests` view (the function runs the same statements automatically on cold start).
- `sql/02_dive_queries.sql`: starter tiles you can save as a MotherDuck Dive: live AI request counter, requests per minute by category, top AI agents, humans arriving via AI referers, crawler 404s, daily AI share, and top pages by category.
- `src/handler.ts`: NDJSON and JSON-array parsing, field extraction from Vercel's `proxy.*` log shape, static-asset filtering, and IP anonymization.
- `src/db.ts`: the `@duckdb/node-api` connection (`md:` + token), idempotent schema bootstrap, and the multi-row bulk `INSERT`.
- `src/classify.ts` + `bots.yaml`: substring matching that loads the YAML rules; the raw payload is stored on every row so you can reclassify history in SQL.
- For building a dashboard on top of this table, save `sql/02_dive_queries.sql` as a Dive; run the `get_dive_guide` MCP tool for Dive authoring, sharing, and embedding.
- For deeper MotherDuck or DuckDB questions (connection options, native tables, JSON columns), use the `ask_docs_question` MCP tool or the MotherDuck docs.
