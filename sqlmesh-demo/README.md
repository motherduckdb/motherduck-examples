# sqlmesh demo

This demo servers as basic replication of `matsonj/stocks` demo which is built on dbt.

The goal is to extend the project to SQLMesh and show of some nice features.

## Getting started

1. clone this repo.
2. setup your python environment with `uv` - if not installed, check out the [uv docs](https://docs.astral.sh/uv/getting-started/installation/)
 - run `uv venv` to create your venv, then activate it with `source .venv/bin/activate` and use `uv sync` to install the dependencies.
3. add your motherduck db & token to in `.dlt/secrets.toml` - see [more here](https://dlthub.com/docs/dlt-ecosystem/destinations/motherduck#setup-guide).
4. run `python3 load/stock_data_pipeline.py` to hydrate the data.
5. cd to transform dir and run `sqlmesh info` to make sure everything checks out.
 - if it does not find your md token, make sure to `source` the token as MOTHERDUCK_TOKEN (Web UI also works).
6. run `sqlmesh plan` to execute your first run (type `y` to push the data in).

