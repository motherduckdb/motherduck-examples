# Python ingestion data pipeline with MotherDuck

This folder shows how to use MotherDuck to ingest data with Python to MotherDuck. Please refers to our [documentation](https://motherduck.com/docs/key-tasks/loading-data-into-motherduck/loading-data-md-python/) for more guidelines.

There are two scripts to demonstrate how to ingest data with Python to MotherDuck:
- (simple)`load_to_motherduck_small.py`: This script loads data from an API into a Pandas dataframe and then ingests it to MotherDuck.
- (best practices)`load_to_motherduck_large.py`: This script loads data from an API into a Pyarrow table through chunks and then ingests it to MotherDuck. Both Pyarrow and target DuckDB table are typed to optimize the ingestion process and avoid any data inference issues.

## Requirements
- Python 3.12
- Docker (optional, for devcontainer)
- uv (Package manager for Python)
- [`motherduck_token`](https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/authenticating-to-motherduck/#creating-an-access-token) to connect to MotherDuck

## Commands
You can use the Makefile to run the scripts. Here are the commands:
```
make load-md-small # Run the load_to_motherduck_small.py script
make load-md-large # Run the load_to_motherduck_large.py script
make test # Run tests
```

