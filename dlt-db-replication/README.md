# PostgreSQL to MotherDuck Replication with DLT

This repository demonstrates how to efficiently replicate data from a PostgreSQL database to [MotherDuck](https://motherduck.com/) using [DLT](https://dlthub.com/) (Data Loading Tool). The pipeline is designed to extract tables from PostgreSQL, transform them as needed, and load them into MotherDuck with optimal performance.

## 📋 Project Overview

This pipeline allows you to:
- Extract data from specified tables in a PostgreSQL database
- Parallelize the extraction process for improved performance
- Transform the data during normalization (if needed)
- Load the data into MotherDuck using configurable batch sizes
- Track detailed metrics about each pipeline run

The project is configured to use [ConnectorX](https://github.com/sfu-db/connector-x) for efficient data extraction and Parquet format for interim storage.

## 🛠️ Requirements

- Python 3.11 or higher
- PostgreSQL database (source)
- MotherDuck account (destination)

## 📦 Dependencies

The project requires the following Python packages:
- `dlt[motherduck]>=1.7.0`: Core data loading tool with MotherDuck support
- `connectorx<0.4.2`: Efficient database connector for PostgreSQL
- `humanize>=4.12.1`: For human-readable output formatting
- `psycopg2-binary>=2.9.10`: PostgreSQL database adapter
- `sqlalchemy>=2.0.38`: SQL toolkit and Object-Relational Mapping

## 🚀 Setup

### 1. Using uv (Recommended)

[uv](https://github.com/astral-sh/uv) is a modern Python package installer and resolver built in Rust, designed to be significantly faster than traditional tools.

If you don't have uv installed:

```bash
# Install uv (macOS/Linux)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Or with pip
pip install uv
```

The simplest way to run the pipeline is with:

```bash
# This will handle environment creation, dependency installation and execution in one step
uv run sql_database_pipeline.py
```

Alternatively, if you want to sync dependencies first:

```bash
# Install project dependencies from pyproject.toml
uv sync

# Then run the pipeline
python sql_database_pipeline.py
```

### 2. Traditional Setup (Alternative)

If you prefer the traditional approach:

```bash
# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -e .
```

### 3. Configure Credentials

Create or update the `.dlt/secrets.toml` file with your database credentials:

```toml
# PostgreSQL credentials
[sources.sql_database.credentials]
drivername = "postgresql"
database = "your_database_name"
host = "your_postgres_host"
password = "your_postgres_password"
port = 5432
username = "your_postgres_username"

# MotherDuck credentials
[destination.motherduck.credentials]
token = "your_motherduck_token"
```

## ⚙️ Configuration

The pipeline behavior is controlled by the `.dlt/config.toml` file. Key configuration sections include:

### Runtime Settings
Control log levels and telemetry options.

### Source Configuration
Define which PostgreSQL schema and tables to extract, as well as parallelization settings.

### Destination Configuration
Configure MotherDuck-specific settings like batch sizes.

### Pipeline Stage Configuration
Set parallelization levels for each pipeline stage (extract, normalize, load).

## 🏃‍♂️ Usage

Run the pipeline with:

```bash
# If using uv
uv run sql_database_pipeline.py

# Or if using traditional setup with activated environment
python sql_database_pipeline.py
```

This will:
1. Connect to your PostgreSQL database
2. Extract the configured tables in parallel
3. Transform the data as needed
4. Load the data into MotherDuck
5. Output detailed metrics about the run

## 📊 Performance Optimization

The pipeline is configured for optimal performance with:

- Parallel extraction from PostgreSQL (8 workers)
- Matching connection pool size (8 connections)
- Parquet format for efficient interim storage
- Large batch sizes for MotherDuck loading (1,000,000 rows)
- Separate worker configurations for extract, normalize, and load stages

## 🔍 Troubleshooting

### Common Issues

1. **Connection errors**: Verify your PostgreSQL credentials and network connectivity
2. **Memory issues**: Reduce batch sizes or worker counts if experiencing OOM errors
3. **Performance issues**: Increase parallelization settings for faster processing

## 📝 License

This project is part of the MotherDuck examples repository and is intended for educational purposes.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.