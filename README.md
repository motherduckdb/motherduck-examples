# Motherduck examples

Ready-to-use starter projects for building with MotherDuck: self-contained, copy-or-clone projects with setup instructions, dependencies, and example code—perfect for bootstrapping your next MotherDuck project.

## Quick Start

The fastest way to get started is using the get-starter script:

```bash
curl -fsSL https://raw.githubusercontent.com/motherduckdb/motherduck-examples/main/scripts/get-starter.sh | bash -s <starter-name>
```

Or browse the available starter projects below. Each folder contains a complete, working example with its own README:

### Data Ingestion

- **[python-ingestion](python-ingestion)** - Python data ingestion patterns (small and large datasets)
- **[dbt-ingestion-s3](dbt-ingestion-s3)** - Ingest data from S3 using dbt (local and cloud)

### dbt Patterns

- **[dbt-ai-prompt](dbt-ai-prompt)** - Use AI functions in dbt to transform unstructured text
- **[dbt-dual-execution](dbt-dual-execution)** - Run dbt models across local and cloud databases
- **[dbt-metricflow](dbt-metricflow)** - Build semantic layer with MetricFlow (local and cloud)

### Data Replication

- **[dlt-db-replication](dlt-db-replication)** - Replicate PostgreSQL to MotherDuck using DLT

### Integrations

- **[motherduck-grafana](motherduck-grafana)** - Connect Grafana to MotherDuck for visualization

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
   curl -fsSL https://raw.githubusercontent.com/motherduckdb/motherduck-examples/main/scripts/get-starter.sh | bash -s <starter-name>
   ```
   
   This will download only the starter project you need. For example:
   ```bash
   curl -fsSL https://raw.githubusercontent.com/motherduckdb/motherduck-examples/main/scripts/get-starter.sh | bash -s dbt-ai-prompt
   ```
   
   The script will:
   - Download only the selected starter project (not the entire repo)
   - Use git sparse checkout to fetch just the needed folder
   - Create a clean copy without git history

   **Testing from a PR branch**: Set the `BRANCH` environment variable to test from a PR branch:
   ```bash
   BRANCH=feat/reorg curl -fsSL https://raw.githubusercontent.com/motherduckdb/motherduck-examples/feat/reorg/scripts/get-starter.sh | bash -s dbt-ai-prompt
   ```

## Requirements

Most starter projects require:
- **MotherDuck account** - [Sign up](https://motherduck.com) (free tier available)
- **MotherDuck token** - Get from [Settings → Access Tokens](https://app.motherduck.com)
- **dbt** - For dbt-based starter projects (installed via project dependencies)

See each starter project's README for specific requirements.
