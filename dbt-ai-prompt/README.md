# dbt-ai-prompt

This is an example project of using the MotherDuck AI function `prompt()` in a dbt pipeline to transform unstructured text data - reviews, feedback - into structured data like - json.

## Using the project

Install the requirements:

With `pip`

```sh
pip install dbt-duckdb duckdb==v1.4.3
```

With 'uv`

```sh
uv venv --python 3.13 # Create a virtual environment 
uv pip install dbt-duckdb duckdb==v1.4.3 #install the latest MotherDuck compatible version
source .venv/bin/activate # activate the virtual environment
```

To be able to connect to MotherDuck you'll need to set the `MOTHERDUCK_TOKEN` 
environment variable to a read/write access token that you have created in your
MotherDuck account.

Once you're all set up, try running the following commands:

- `dbt run`
- `dbt test`
- `dbt show --select reviews_attributes_by_product`
