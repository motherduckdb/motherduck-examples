# MotherDuck examples

A catalog of opinionated, ready-to-use examples for building with MotherDuck.
Each example is a self-contained folder with working code and a README that
reads like an agent skill: minimal front matter for discovery, plus a body that
tells an agent (and a human) exactly what to adapt, what to ask, and how to run
or deploy it.

The same source feeds several surfaces from one place: GitHub, the docs site, an
agent via the MotherDuck MCP server, and the `motherduck` CLI.

There are two kinds of entry:

- **Flight Plans** live under [`flight-plans/`](flight-plans) and deploy as a
  MotherDuck Flight (Python runtime plus scheduling). They are non-deterministic
  plans: an agent reads the README, asks the user a few questions, adapts the
  snippet, and deploys it. They carry `features: [flights]`.
- **Examples** at the top level are everything else: dbt projects, app and edge
  integrations, ingestion scripts, and UI walkthroughs.

## Quick start

Pull a single example into a new folder with the get-starter script (short name
works for both top-level examples and Flight Plans):

```bash
curl -fsSL https://get.motherduck.com | bash -s <name>
# e.g.
curl -fsSL https://get.motherduck.com | bash -s dbt-ingestion-s3
```

## Flight Plans

Deploy as a MotherDuck Flight, on demand or on a schedule.

| Plan | Type | What it does |
|---|---|---|
| [dbt-runner](flight-plans/dbt-runner) | template | Run any dbt project from git as a Flight. The base plan the others adapt. |
| [dbt-ingestion-s3](flight-plans/dbt-ingestion-s3) | example | Build Hacker News models from Parquet in S3 with dbt. |
| [dbt-churn-prediction](flight-plans/dbt-churn-prediction) | example | Build churn feature and label tables with dbt for downstream scoring. |
| [dbt-ducklake](flight-plans/dbt-ducklake) | example | Run TPC-DS dbt models on a DuckLake-backed database (`features: ducklake`). |

## Examples

### dbt and transformation

- [dbt-ai-prompt](dbt-ai-prompt) - Extract structured data from review text with dbt and `prompt()`.
- [dbt-dual-execution](dbt-dual-execution) - Run dbt models across local DuckDB and MotherDuck.
- [dbt-local-ducklake](dbt-local-ducklake) - Run dbt on a local DuckLake catalog (`features: ducklake`).
- [dbt-metricflow](dbt-metricflow) - Define and query metrics with dbt MetricFlow.
- [sqlmesh-demo](sqlmesh-demo) - Transform stock data with SQLMesh and dlt.

### Ingestion and replication

- [python-ingestion](python-ingestion) - Ingest API data into MotherDuck with the Python client.
- [dlt-db-replication](dlt-db-replication) - Replicate PostgreSQL tables to MotherDuck with dlt.

### Apps, serverless, and edge

- [cloudflare-workers](cloudflare-workers) - Query MotherDuck from Cloudflare Workers via the Postgres endpoint (`features: pg_endpoint`).
- [cloudflare-workers-duckoffee](cloudflare-workers-duckoffee) - Full-stack Worker: world map, live chart, and Durable Object voting on a MotherDuck share (`features: pg_endpoint, shares`).
- [vercel-nextjs](vercel-nextjs) - Query MotherDuck from Vercel and Next.js API routes (`features: pg_endpoint`).
- [vercel-agent-analytics](vercel-agent-analytics) - Capture a Vercel log drain and classify AI-agent traffic in MotherDuck.
- [nodejs-motherduck](nodejs-motherduck) - Connect from Node.js with the DuckDB Neo driver, including a connection pool.

### Integrations and UI

- [postgres-demo](postgres-demo) - Bridge local Postgres and MotherDuck with pg_duckdb (`features: pg_duckdb`).
- [motherduck-grafana](motherduck-grafana) - Visualize MotherDuck data in Grafana.
- [motherduck-ui](motherduck-ui) - Clean and analyze a CSV in the MotherDuck UI.

## Anatomy of an example

Every example README starts with YAML front matter holding exactly six keys:

```yaml
---
title: Build Hacker News Models From S3 With dbt
id: dbt-ingestion-s3            # must equal the folder name
description: >-
  One or two sentences on what it does, then a "Use when ..." clause so an
  agent can route to it.
type: example                   # example | template
features: [flights]             # MotherDuck capabilities used; [] if none (see list below)
tags: [dbt, s3, parquet, hacker-news]   # lowercase slugs: tools, datasets, topics
---
```

- `type` is `example` (a concrete, worked instance) or `template` (a generic plan
  an agent parameterizes).
- `features` names the MotherDuck capabilities the code actually uses, from:
  `admin_api`, `dives`, `ducklake`, `flights`, `mcp`, `pg_duckdb`, `pg_endpoint`,
  `shares`, `wasm`. Note `pg_endpoint` (MotherDuck's Postgres wire endpoint) and
  `pg_duckdb` (the pg_duckdb extension) are different features.
- `tags` come from a curated list (`ALLOWED_TAGS` in `scripts/build-catalog.py`)
  of significant third-party tools, frameworks, languages, and platforms (for
  example `dbt`, `dlt`, `cloudflare`, `vercel`, `nextjs`, `pandas`,
  `node-postgres`). They are not for datasets, generic concepts (`sql`, `etl`),
  the DuckDB engine, redundant variants, or things already covered by `features`.
  Tags may be empty. Add a new tag only for a significant new tool.

The body follows a consistent, skimmable structure:

1. `# <title>` and one paragraph on what it is and the MotherDuck pattern it shows.
2. `## What you'll adjust` - a table of the real knobs found in the code, each with
   its purpose and options or an example value.
3. `## Questions to answer` - the inputs needed before adapting the example.
4. `## Run it` - exact commands for the project's runtime, plus a
   `### Deploy as a Flight` subsection for Flight Plans.
5. `## How it works / Learn more` - progressive disclosure: links to extra
   in-folder files, and pointers to the MotherDuck MCP guides (`get_flight_guide`,
   `get_dive_guide`) and `ask_docs_question` instead of duplicating them.

## Authoring and validation

- Flight Plans (anything with `features: [flights]`) live under `flight-plans/`.
- Shared dbt-on-Flights logic lives in
  [`flight-plans/dbt-runner`](flight-plans/dbt-runner); the dbt example plans
  adapt it. They are no longer kept byte-identical, so each plan can diverge.
- Validate front matter and build the catalog with:

  ```bash
  uv run scripts/build-catalog.py                 # validate only
  uv run scripts/build-catalog.py --output catalog.json   # write the catalog
  ```

  The catalog shape is defined by [`catalog.schema.json`](catalog.schema.json).

## Getting an example

Each example is self-contained. You can:

1. **Use the get-starter script** (recommended):
   ```bash
   curl -fsSL https://get.motherduck.com | bash -s dbt-ingestion-s3
   ```
   It uses git sparse checkout to fetch only that folder (resolving a Flight Plan
   under `flight-plans/` automatically) and drops it into a clean folder without
   git history.

   **Testing from a PR branch**: set `BRANCH` and fetch the script from that
   branch (since `get.motherduck.com` always serves `main`):
   ```bash
   BRANCH=my-branch curl -fsSL https://raw.githubusercontent.com/motherduckdb/motherduck-examples/my-branch/scripts/get-starter.sh | bash -s dbt-ingestion-s3
   ```

2. **Clone the repo** and navigate to a folder:
   ```bash
   git clone https://github.com/motherduckdb/motherduck-examples.git
   cd motherduck-examples/flight-plans/dbt-ingestion-s3
   ```

3. **Copy a folder** to start your project:
   ```bash
   cp -r flight-plans/dbt-ingestion-s3 my-new-project
   ```

## Requirements

Most examples require:

- **MotherDuck account** - [sign up](https://motherduck.com) (free tier available)
- **MotherDuck token** - from [Settings -> Access Tokens](https://app.motherduck.com)
- Tooling specific to each example (dbt, Node.js, Docker, etc.)

See each example's README for its exact prerequisites and commands.
