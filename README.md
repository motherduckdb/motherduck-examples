# MotherDuck Starters

Ready-to-use starter projects for building with MotherDuck. Each project is self-contained and can be copied or cloned to kickstart your development.

> **What's a starter?** These are complete, working projects you can copy and modify. Each includes setup instructions, dependencies, and example code. Perfect for bootstrapping your next MotherDuck project.

## Quick Start

The fastest way to get started is using the get-starter script:

```bash
curl -fsSL https://raw.githubusercontent.com/motherduckdb/motherduck-examples/main/scripts/get-starter.sh | bash -s <starter-name>
```

Or browse the available starters below. Each folder contains a complete, working example with its own README:

### Data Ingestion

- **[python-ingestion](python_ingestion)** - Python data ingestion patterns (small and large datasets)
- **[dbt-ingestion-s3](dbt-ingestion-s3)** - Ingest data from S3 using dbt (local and cloud)

### dbt Patterns

- **[dbt-ai-prompt](dbt-ai-prompt)** - Use AI functions in dbt to transform unstructured text
- **[dbt-dual-execution](dbt-dual-execution)** - Run dbt models across local and cloud databases
- **[dbt-metricflow](dbt-metricflow)** - Build semantic layer with MetricFlow (local and cloud)

### Data Replication

- **[dlt-db-replication](dlt-db-replication)** - Replicate PostgreSQL to MotherDuck using DLT

### Integrations

- **[motherduck-grafana](motherduck-grafana)** - Connect Grafana to MotherDuck for visualization

## Getting a Starter

Each starter is self-contained. You can:

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
   
   This will download only the starter you need. For example:
   ```bash
   curl -fsSL https://raw.githubusercontent.com/motherduckdb/motherduck-examples/main/scripts/get-starter.sh | bash -s dbt-ai-prompt
   ```
   
   The script will:
   - Download only the selected starter (not the entire repo)
   - Use the fastest method available (degit or git sparse checkout)
   - Create a clean copy without git history

   **Testing from a PR branch**: You can test the script from a PR by using your branch name in the URL:
   ```bash
   curl -fsSL https://raw.githubusercontent.com/motherduckdb/motherduck-examples/<your-branch>/scripts/get-starter.sh | bash -s dbt-ai-prompt
   ```

## Requirements

Most starters require:
- **MotherDuck account** - [Sign up](https://motherduck.com) (free tier available)
- **MotherDuck token** - Get from [Settings → Access Tokens](https://app.motherduck.com)
- **dbt** - For dbt-based starters (installed via project dependencies)

See each starter's README for specific requirements.
