# MotherDuck examples

Ready-to-use starter projects for building with MotherDuck: self-contained, copy-or-clone projects with setup instructions, dependencies, and example code—perfect for bootstrapping your next MotherDuck project.

This repository is also evolving into a cookbook for MotherDuck recipes and templates. Recipe folders remain standalone starter projects. Shared assets, such as Flight and Dive templates, live under [`templates/`](templates) and can be referenced by recipes or surfaced in product/docs experiences.

## Quick Start

The fastest way to get started is using the get-starter script:

```bash
curl -fsSL https://get.motherduck.com | bash -s <starter-name>
```

Or browse the available starter projects below. Each folder contains a complete, working example with its own README:

### Getting Started

- **[nodejs-motherduck](nodejs-motherduck)** - Node.js connection and query examples using DuckDB Neo driver

### Data Ingestion

- **[python-ingestion](python-ingestion)** - Python data ingestion patterns (small and large datasets)
- **[dbt-ingestion-s3](dbt-ingestion-s3)** - Ingest data from S3 using dbt (local, cloud, and Flights)

### dbt Patterns

- **[dbt-ai-prompt](dbt-ai-prompt)** - Use AI functions in dbt to transform unstructured text
- **[dbt-churn-prediction](dbt-churn-prediction)** - Build a SQL-first churn prediction queue with dbt and MotherDuck native storage
- **[dbt-dual-execution](dbt-dual-execution)** - Run dbt models across local and cloud databases
- **[dbt-ducklake](dbt-ducklake)** - Run TPC-DS dbt models on DuckLake and MotherDuck
- **[dbt-metricflow](dbt-metricflow)** - Build semantic layer with MetricFlow (local and cloud)

### Data Replication

- **[dlt-db-replication](dlt-db-replication)** - Replicate PostgreSQL to MotherDuck using DLT

### Serverless & Edge

- **[cloudflare-workers](cloudflare-workers)** - Query MotherDuck from Cloudflare Workers via the Postgres endpoint
- **[cloudflare-workers-duckoffee](cloudflare-workers-duckoffee)** - Full-stack Cloudflare Workers demo: world map, live sales chart, and Durable Object voting, all backed by a MotherDuck share
- **[vercel-nextjs](vercel-nextjs)** - Query MotherDuck from Vercel + Next.js API routes via the Postgres endpoint

### Integrations

- **[motherduck-grafana](motherduck-grafana)** - Connect Grafana to MotherDuck for visualization

### Templates

- **[templates/flights/dbt-runner](templates/flights/dbt-runner)** - Reusable Flight template that clones a dbt project, writes a runtime profile, runs dbt, and records an audit row
- **[templates/dives](templates/dives)** - Reserved location for reusable Dive templates

Template-backed recipes are checked by [`scripts/check-flight-template-sync.py`](scripts/check-flight-template-sync.py) so example Flight source stays in sync with the reusable template.

## Getting a starter project

Each starter project is self-contained. You can:

1. **Clone the entire repo** and navigate to a folder:
   ```bash
   git clone https://github.com/motherduckdb/motherduck-examples.git
   cd motherduck-examples/<starter-name>
   ```

2. **Copy a folder** manually to start your project:
   ```bash
   cp -r <starter-name> my-new-project
   cd my-new-project
   ```

3. **Use the get-starter script** (recommended):
   ```bash
   curl -fsSL https://get.motherduck.com | bash -s <starter-name>
   ```

   This will download only the starter project you need. For example:
   ```bash
   curl -fsSL https://get.motherduck.com | bash -s dbt-ai-prompt
   ```

   The script will:
   - Download only the selected starter project (not the entire repo)
   - Use git sparse checkout to fetch just the needed folder
   - Create a clean copy without git history

   **Testing from a PR branch**: set the `BRANCH` environment variable and fetch the script from that branch directly (since `get.motherduck.com` always serves the `main` version):
   ```bash
   BRANCH=my-branch curl -fsSL https://raw.githubusercontent.com/motherduckdb/motherduck-examples/my-branch/scripts/get-starter.sh | bash -s dbt-ai-prompt
   ```

## Requirements

Most starter projects require:
- **MotherDuck account** - [Sign up](https://motherduck.com) (free tier available)
- **MotherDuck token** - Get from [Settings → Access Tokens](https://app.motherduck.com)
- **dbt** - For dbt-based starter projects (installed via project dependencies)

See each starter project's README for specific requirements.

## Cookbook metadata

Recipes and templates can include a `meta.yml` sidecar file. This keeps GitHub READMEs readable while giving the docs site and product UIs a stable catalog format.

Recipe metadata uses:

```yaml
metadata_version: 1
id: dbt-ingestion-s3
kind: recipe
title: Build Hacker News models from S3 with dbt
standalone: true
```

Template metadata uses:

```yaml
metadata_version: 1
id: dbt-runner
kind: flight_template
title: dbt Runner
```
