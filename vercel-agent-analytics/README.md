# Vercel agent analytics

Capture Vercel log drain traffic, classify AI crawlers and agents, and write into a MotherDuck table. Use it as a live source for a Dive or any dashboard that runs SQL. No AWS, no S3, one function.

## Architecture

```
 Vercel app
     │
     │ NDJSON log drain (HMAC signed)
     ▼
 Vercel Function (api/drain)
     │  verify signature
     │  parse + classify UA / referer
     │  one bulk INSERT per batch
     ▼
 MotherDuck  <MD_DESTINATION>.<MD_TABLE>
     │
     └──── Dive / BI tool reads the same table
```

Why this shape:

- **Log drain, not middleware**: captures every request at the edge without changing app code. Vercel handles batching and retries.
- **One batch, one round-trip**: each Vercel drain POST already carries 100 to 1000 log lines. The function parses, classifies, and writes the whole batch in a single `INSERT`. No cross-invocation buffering.
- **Static assets excluded**: image requests plus `.js`, `.css`, font files (`.woff`, `.woff2`, `.ttf`, `.otf`, `.eot`), and source maps are dropped before insert so the table stays focused on page and API traffic that matters for AI measurement.
- **Native MotherDuck table**: simplest write path, fastest reads. If you ever want open Parquet under the hood and snapshot isolation, see the sibling `ducklake-log-drain/` example instead.
- **Classifier in code, rules in YAML**: `bots.yaml` is the source of truth for AI identification. Update it and redeploy; the raw payload is stored on every row so you can reclassify history in SQL.

## Folder layout

```
vercel-agent-analytics/
  api/
    drain.ts             Vercel Function entry point
  src/
    handler.ts           parsing + classification + insert orchestration
    signature.ts         Vercel HMAC verify
    classify.ts          UA and referer matcher (loads bots.yaml)
    db.ts                MotherDuck connection + bulk INSERT
    local-server.ts      dev harness
  bots.yaml              classifier patterns
  sql/
    01_setup.sql         reference DDL (the function runs this automatically)
    02_dive_queries.sql  starter queries for a Dive
  scripts/
    sample-payload.ndjson example batch
    test-local.sh        sign and POST it
  vercel.json            function config
```

## Prerequisites

- A MotherDuck account and access token (`MOTHERDUCK_TOKEN`)
- A Vercel project with log drain creation rights
- Node 20+

## Setup

### 1. Deploy the function

```bash
cd vercel-agent-analytics
npm install
vercel
```

Set the required environment variables in the Vercel project settings:

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `MOTHERDUCK_TOKEN`      | yes | —                       | MotherDuck access token |
| `VERCEL_DRAIN_SECRET`   | yes | —                       | HMAC secret; paste the same value into the Vercel log drain config |
| `BOTS_ONLY`             | no  | `false`                 | When `true`, drop rows with no classifier match. Useful once you are past the "does it work" phase and only care about AI traffic |
| `MD_DESTINATION`        | no  | `agent_analytics.raw`   | Target MotherDuck destination in `<database>.<schema>` form |
| `MD_TABLE`              | no  | `vercel_request_logs`   | Target table |

`MD_DATABASE` and `MD_SCHEMA` are still supported for backward compatibility, but `MD_DESTINATION` is the preferred setting.

`vercel deploy` prints the function URL: `https://<project>.vercel.app/api/drain`.

On the first request, the function runs `CREATE DATABASE IF NOT EXISTS`, `CREATE SCHEMA IF NOT EXISTS`, `CREATE TABLE IF NOT EXISTS`, and `CREATE OR REPLACE VIEW` against whatever destination you configured, so no manual SQL step is needed. The same DDL is in `sql/01_setup.sql` for reference or local use.

### 2. Point Vercel at the function

Vercel dashboard : Project : Settings : Log Drains : Add.

- Format: `NDJSON`
- Endpoint: the `/api/drain` URL from step 2
- Custom secret: the same string you set as `VERCEL_DRAIN_SECRET`

### 3. Verify

```bash
# local test
export VERCEL_DRAIN_SECRET=dev-secret
export MOTHERDUCK_TOKEN=...
npm run dev &
./scripts/test-local.sh

# against the deployed function
./scripts/test-local.sh https://<project>.vercel.app/api/drain
```

Then check MotherDuck:

```sql
-- replace with your configured MD_DESTINATION + MD_TABLE if you changed them
SELECT * FROM agent_analytics.raw.vercel_request_logs
ORDER BY event_ts DESC LIMIT 20;
```

The collector also anonymizes IPv4 addresses by zeroing the last octet before insert, e.g. `203.0.113.42` becomes `203.0.113.0`.

## Running a Dive

Save `sql/02_dive_queries.sql` as a MotherDuck Dive. Starter tiles include:

- Live 5-minute AI request counter
- Requests per minute by category, last 60 min
- Top 20 AI agents by request count, last 24h
- Humans arriving via AI referers (ChatGPT, Claude, Perplexity, Gemini, ...)
- 404s hit by crawlers (content they wanted but could not find)
- Daily AI share of traffic, last 30 days

## Classifier

`bots.yaml` is the source of truth. Three categories:

- `crawler`: background training and indexing bots (`GPTBot`, `ClaudeBot`, `Claude-SearchBot`, `Google-Extended`, `CCBot`, `Applebot-Extended`, `Bytespider`, `Meta-ExternalAgent`, `PerplexityBot`, ...)
- `agent`: on-demand fetchers driven by a user prompt (`ChatGPT-User`, `ChatGPT Agent`, `Operator`, `Claude-User`, `Perplexity-User`, `MistralAI-User`, `Gemini-Deep-Research`, `Devin`, ...)
- `human_via_ai`: matched on `referer` for `chatgpt.com`, `claude.ai`, `perplexity.ai`, `gemini.google.com`, `copilot.microsoft.com`, `phind.com`, `meta.ai`, `chat.mistral.ai`, and more

The list is seeded from the community-maintained [ai-robots-txt](https://github.com/ai-robots-txt/ai.robots.txt) catalog plus vendor docs. Edit the YAML and redeploy to update rules; no reingest needed, because the raw user agent and referer are stored on every row. You can also reclassify historical rows in SQL against the `raw` JSON column.

## Limits and caveats

- **Cold start**: the first invocation after idle pays a ~500 ms-1 s MotherDuck extension load. Vercel Fluid Compute keeps functions warm well enough that this is rare in practice. For high-QPS sites, ping the function every few minutes or enable always-warm via Vercel's Fluid config.
- **Bundle size**: `@duckdb/node-api` ships a native binary. Fluid Compute gives you the headroom; classic Serverless Functions may run into the 50 MB zipped limit.
- **No appender on native MotherDuck tables (yet)**: we use multi-row `INSERT` instead. With ~500 rows per batch that's one network round-trip per drain POST, which is plenty fast. If you need the appender path, use the DuckLake variant.
- **At-least-once delivery**: on a 5xx response Vercel redelivers the batch. If you care about exact counts, dedupe on `event_id` in your queries.
- **Agentic browsers that spoof user agents**: this classifier only sees what the bot tells it. Advanced AI agents using plain Chromium UAs will fall into the `null` bucket.

## When to turn on `BOTS_ONLY`

Start with `BOTS_ONLY=false` so you have a real baseline including human traffic: you can measure AI share of traffic, see which pages humans land on from AI UIs, etc. Flip to `true` once you are convinced the table has grown large enough to justify dropping the rest.
