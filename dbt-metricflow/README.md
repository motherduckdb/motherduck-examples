# MetricFlow with DuckDB & MotherDuck

An example showing dbt's MetricFlow semantic layer running on DuckDB (local) and MotherDuck. 

- DuckDB and MotherDuck have full MetricFlow support built into dbt
- Define metrics once, query from local or cloud

## Installation

```bash
# Clone and setup
git clone <your-repo-url>
cd metricsflow-md

# Create virtual environment and install
cd metricflow-example
python3 -m venv venv
source venv/bin/activate
pip install dbt-duckdb dbt-metricflow
```

## Quick Start - Local DuckDB

```bash
# From metricflow-example/ecommerce_metrics/
source ../venv/bin/activate

# Load data and build models
DBT_PROFILES_DIR=.. dbt seed
DBT_PROFILES_DIR=.. dbt run

# Query metrics
DBT_PROFILES_DIR=.. mf query --metrics revenue,orders,customers --group-by metric_time__month
```

Expected output:
```
metric_time__month      revenue    orders    customers
--------------------  ---------  --------  -----------
2024-01-01T00:00:00     1962.29        10            6
2024-02-01T00:00:00     2621.73        10           10
```

## Quick Start - MotherDuck

```bash
# Set your MotherDuck token
export motherduck_token='your_token_here'

# Create database (first time only)
python -c "import duckdb; conn = duckdb.connect('md:'); conn.execute('CREATE DATABASE ecommerce_test_db')"

# Load data and build models
DBT_PROFILES_DIR=.. dbt seed --target motherduck
DBT_PROFILES_DIR=.. dbt run --target motherduck

# Query metrics from the cloud
DBT_PROFILES_DIR=.. DBT_TARGET=motherduck mf query --metrics revenue,orders,customers --group-by metric_time__month
```

## Example Project Structure

```
metricflow-example/
├── profiles.yml                    # dbt configs (local & cloud)
└── ecommerce_metrics/
    ├── seeds/raw_orders.csv        # 20 sample orders
    └── models/
        ├── fct_orders.sql          # Fact table
        ├── metricflow_time_spine.sql
        └── semantic_models.yml     # Metrics definitions
```

## Example Metrics Included

- **revenue** - Total order revenue
- **orders** - Order count
- **customers** - Unique customers
- **avg_order_value** - Average order amount
- **revenue_per_customer** - Derived metric

All support time dimensions (day/month/year) and filtering.

## More Examples

```bash
# Single metric
mf query --metrics revenue

# Group by month
mf query --metrics revenue --group-by metric_time__month

# Multiple metrics
mf query --metrics revenue,orders,avg_order_value --group-by metric_time__month

# Derived metrics
mf query --metrics revenue_per_customer --group-by metric_time__month

# See the SQL
mf query --metrics revenue --group-by order_id__status --explain
```

See [EXAMPLES.md](EXAMPLES.md) for more query patterns.

## How It Works

```
Your Query → MetricFlow → dbt-duckdb Adapter → DuckDB/MotherDuck
                ↓
         Generates SQL
```

MetricFlow has a DuckDB SQL renderer. The dbt-duckdb adapter handles connections. MotherDuck uses the same code path as local DuckDB - just a different connection string.

## MotherDuck Token

Get your token from the [MotherDuck UI](https://app.motherduck.com) Settings → Access Tokens

## Resources

- [dbt Semantic Layer docs](https://docs.getdbt.com/docs/use-dbt-semantic-layer/dbt-sl)
- [MetricFlow GitHub](https://github.com/dbt-labs/metricflow)
- [dbt-duckdb adapter](https://github.com/duckdb/dbt-duckdb)
- [MotherDuck docs](https://motherduck.com/docs)

