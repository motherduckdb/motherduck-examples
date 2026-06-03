---
title: Cloudflare Workers Map and Live Vote on MotherDuck
id: cloudflare-workers-duckoffee
description: >-
  A single Cloudflare Worker that reads analytics from a MotherDuck share over
  the Postgres endpoint and tallies live votes in a SQLite-backed Durable Object,
  with a D3 world map, sales chart, and leaderboard frontend. Use when you want a
  full-stack edge app that queries MotherDuck without bundling DuckDB and keeps a
  small piece of real-time shared state at the edge.
type: example
features: [pg_endpoint]
tags: [cloudflare, cloudflare-workers, durable-objects, pg, d3, wrangler, typescript]
---

# Cloudflare Workers Map and Live Vote on MotherDuck

A full-stack Cloudflare example that renders a world map of imaginary Duckoffee cafes, a live sales chart, and a live vote for where the next location should open. One Worker reads analytics from a MotherDuck share through the Postgres wire protocol (the `pg` driver, no DuckDB binary in the bundle), serves a static D3 single-page app via the Workers Assets binding, and tallies votes in a Durable Object backed by SQLite. The MotherDuck pattern it shows: query a read-only share from a serverless edge runtime over the Postgres endpoint, parameterize every statement, and keep all mutable state out of the warehouse.

## What you'll adjust

| Setting | Purpose | Options / example |
| --- | --- | --- |
| `MOTHERDUCK_TOKEN` (Wrangler secret / `.dev.vars`) | Auth for the Postgres endpoint connection string | `npx wrangler secret put MOTHERDUCK_TOKEN`; locally `MOTHERDUCK_TOKEN="ey..."` in `.dev.vars` |
| `MOTHERDUCK_HOST` (`[vars]` in `wrangler.toml`) | Postgres endpoint host, region-specific | `pg.us-east-1-aws.motherduck.com` |
| `MOTHERDUCK_DB` (`[vars]`) | Database in the connection string | `sample_data` |
| `DUCKOFFEE_SHARE` (`[vars]`) | Share URI attached as `duckoffee` in `withClient` (`src/index.ts`) | `md:_share/...`; or your own `md:_share/<org>/<uuid>` |
| `name` (`wrangler.toml`) | Deployed Worker name | `duckoffee-map` |
| SQL in `handleLocations` / `handleSales` / `handleSummary` (`src/index.ts`) | The three read-only queries against `duckoffee.locations`, `duckoffee.orders`, `duckoffee.order_items` | Repoint table/column names to your own share's schema |
| `CITY_COORDS` (`src/index.ts`) | Lat/lon lookup for cities in your locations table (sample data has no coordinates) | Add a `"City Name": [lon, lat]` entry per location |
| `CANDIDATES` (`src/index.ts`) | The 10 hardcoded voting candidates; `id` is the stable Durable Object key, do not rename | `{ id: "seoul", name: "Seoul", country: "South Korea", lon, lat }` |
| `DATA_CACHE_TTL_SECONDS` (`src/index.ts`) | Edge cache TTL for the three SQL endpoints | `15 * 60` (15 minutes) |
| `days` clamp in `handleSales` | Range and default for the daily series window | default 90, clamped to `[7, 365]` |
| Brand colors / assets (`public/style.css`, `public/assets/`) | Look and feel of the SPA | CSS custom properties at top of `style.css`; swap SVGs in `public/assets/` |

## Questions to ask the user

- Which MotherDuck share or database holds the analytics, and what is its share URI?
- What is the schema: a locations table, a per-day fact table, and a top-N dimension, plus the column names to use in the three queries?
- What region is the account in, so you set the right `MOTHERDUCK_HOST`?
- What is the voting question and the candidate list (8 to 12 items, kebab-case `id` values)?
- What lat/lon coordinates back each location and candidate city?
- What brand palette, copy, and assets should the frontend use?
- What Worker name and cache TTL do you want?

## Run it

Prerequisites: Node.js 18+, a Cloudflare account, and a MotherDuck account with an access token.

```sh
npm install

# Local dev: put your token in .dev.vars, then start the dev server
printf 'MOTHERDUCK_TOKEN="ey...MY_TOKEN"\n' > .dev.vars
npx wrangler dev
```

Visit http://localhost:8787. Open it in two tabs to watch votes propagate, each tab gets its own `sessionStorage` session ID, so each counts as a distinct voter.

Deploy:

```sh
# Set the token as a Worker secret (one time)
npx wrangler secret put MOTHERDUCK_TOKEN

npx wrangler deploy
```

The first deploy creates the `duckoffee-map` Worker, uploads `./public` through the `[assets]` binding, and provisions the `VoteTracker` Durable Object via the `[[migrations]]` entry. Subsequent deploys just upload new code.

## How it works / Learn more

- Routes and behavior: `GET /api/locations`, `GET /api/sales?location_id=&days=`, `GET /api/summary?location_id=`, `GET /api/votes?session_id=`, and `POST /api/votes`. All `/api/*` paths fall through to `env.ASSETS.fetch(req)` for the static SPA.
- Connection: `withClient` in `src/index.ts` builds `postgresql://anyusername:${TOKEN}@${HOST}:5432/${DB}?sslmode=require`, runs `ATTACH IF NOT EXISTS '<share>' AS duckoffee`, then always closes the client. `nodejs_compat` in `wrangler.toml` is what lets the `pg` driver run on the Workers runtime.
- State: the `VoteTracker` Durable Object keeps one global instance with a `votes` table keyed by `session_id` (re-voting is an UPSERT), so each session has exactly one changeable vote. Tallies are a `GROUP BY candidate_id`. No write path reaches MotherDuck.
- Security: every SQL statement uses numbered parameters (e.g. `WHERE location_id = $1::BIGINT`), `days` is clamped, `location_id` is validated, and `candidate_id` is whitelisted against the hardcoded set.
- Recreate it with your own theme: [`PROMPT.md`](./PROMPT.md) is a self-contained prompt you can paste into a coding agent to build a variant with your own brand, dataset, and voting question on the same architecture.
- Connecting to the Postgres endpoint from other clients and regions: run the `ask_docs_question` MCP tool, or see the MotherDuck Postgres endpoint docs.
