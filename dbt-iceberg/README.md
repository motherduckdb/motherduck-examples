# MotherDuck + dbt + Iceberg

Run dbt models in MotherDuck while reading Iceberg tables from a Iceberg REST catalog (defaults to Polaris). The dbt-duckdb Iceberg plugin (PyIceberg) queries Polaris directly. results are materialized into MotherDuck for analysis. Iceberg is not yet supported server-side in MotherDuck—this example relies on DuckDB’s Iceberg support.

## What’s included
- `models/tpcds/raw`: Iceberg sources that mirror the Polaris namespace defined by `vars.polaris_namespace` (default `s3`).
- `models/tpcds/queries`: 99 TPC-DS analytical queries, materialized as tables in the `iceberg_analytics` database on MotherDuck.
- `profiles.yml`: MotherDuck target with the Polaris REST catalog plugin, plus optional catalog examples (Glue, Unity, Nessie).

## Prereqs
- MotherDuck account and token (`motherduck auth login`).
- Polaris REST catalog credentials (client id, client secret, OAuth token URL, warehouse).
- `uv` for dependency management.
- TPC-DS data already landed in your Polaris (or any DuckDB-supported Iceberg) catalog; ingestion is out of scope for this example.

## Configure credentials
Fill in `.env` (loaded automatically by `uv`):
```
MOTHERDUCK_TOKEN=<md_token>
POLARIS_CLIENT_ID=<client_id>
POLARIS_CLIENT_SECRET=<client_secret>
POLARIS_TOKEN_URL=https://polaris.fivetran.com/api/catalog/v1/oauth/tokens
POLARIS_SCOPE=PRINCIPAL_ROLE:ALL
POLARIS_WAREHOUSE=<warehouse>
# Optional for dbt-fivetran:
# POLARIS_CREDENTIALS=<client_id>:<client_secret>
# POLARIS_CATALOG=<fivetran_group_id>
# FIVETRAN_POLARIS_BUCKET=<s3_bucket_backing_polaris>
# FIVETRAN_POLARIS_SCHEMA=analytics
# FIVETRAN_MAX_MEMORY=4096
```
The MotherDuck path in `profiles.yml` embeds the token when `MOTHERDUCK_TOKEN` is set:
```yaml
path: "{% set md_token = env_var('MOTHERDUCK_TOKEN', '') %}{{ 'md:dbt_iceberg?motherduck_token=' ~ md_token if md_token else 'md:dbt_iceberg' }}"
```

## Install and run
```
uv sync
uv run dbt debug
uv run dbt build
```
- Create the MotherDuck database once if it does not exist: `uv run python -c "import duckdb; duckdb.connect('md:').execute('CREATE DATABASE IF NOT EXISTS dbt_iceberg')"`
- Override the Polaris namespace at runtime if needed: `uv run dbt build --vars "polaris_namespace: <namespace>"`.
- Switch to the Fivetran adapter by setting `DBT_TARGET=fivetran` (target is `motherduck` by default).

## Refreshing TPC-DS data (optional)
Generate TPC-DS locally with DuckDB’s extension, write Parquet to a bucket mirrored by Polaris, then rerun `dbt build` against the refreshed catalog. See `models/tpcds/raw/_sources.yml` for the expected table list.

## Security hygiene
- Secrets live only in environment variables or your untracked shell; no defaults are bundled.
- Generated artifacts (`logs/`, `target/`) are gitignored. Remove local runs before sharing the project.
