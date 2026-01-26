# dbt_ingestion_s3

This project demonstrates how to ingest data directly from S3 into DuckDB or MotherDuck using dbt, and how to build analytical models on top of that data. The main goal is to highlight a modern analytics workflow where you can:

- Ingest and query data directly from S3 
- Build and run dbt models either locally (DuckDB) or in the cloud (MotherDuck)

## Prerequisites

- [dbt-duckdb](https://github.com/dbt-labs/dbt-duckdb) installed
- [MotherDuck](https://motherduck.com/) account (for cloud runs)
- A database created in MotherDuck (see below)
- Access to the public S3 dataset: `s3://us-prd-motherduck-open-datasets/hacker_news/parquet/hacker_news_2024_2025.parquet`

## Creating the Database in MotherDuck

Before running dbt on MotherDuck, you must create the target database. You can do this in the MotherDuck web UI or with SQL:

```
CREATE DATABASE IF NOT EXISTS hacker_news_stats;
```

## Running dbt Locally (DuckDB)

1. Ensure your `profiles.yml` has a `dev` target pointing to a local DuckDB file (e.g., `local.db`).
2. Run:

```
dbt run --target dev
```

This will build the models using your local DuckDB database.

## Running dbt on MotherDuck (prod)

1. Ensure your `profiles.yml` has a `prod` target pointing to your MotherDuck database, e.g.:

```yaml
prod:
  type: duckdb
  path: "md:hacker_news_stats"
  threads: 1
```

2. Set your MotherDuck token (if required):

```
export MOTHERDUCK_TOKEN=your_token_here
```

3. Run:

```
dbt run --target prod
```

This will build the models in your MotherDuck database.

## Project Structure

- **models/sources.yml**: Defines the S3 Parquet file as a dbt source using DuckDB's external file support.
- **models/top_story_by_comments.sql**: Finds the top Hacker News story by comments per month.
- **models/duckdb_keyword_mentions.sql**: Counts monthly mentions of DuckDB in HN stories.
- **models/top_domains.sql**: Lists the top 20 domains from HN story URLs.

## Notes
- The project does not copy data from S3; it queries the Parquet file directly using DuckDB/MotherDuck's external file capabilities.
- You can use the same dbt project and models for both local and cloud analytics workflows.
