# MotherDuck Dual Execution example with dbt

The objective of this project is to show a minimal example of dual execution in MotherDuck with dbt. The basic premise is `dbt init` with one additional model and a simple `dev` profile in the `profiles.yml` file. By using `attach`, we can add a second database in the execution context of DuckDB and write it to a physical file.

## Example Profiles.yml

```
dual_execution:
  outputs:
    dev:
      type: duckdb
      path: "md:my_db"   # motherduck path
      attach:
        - path: local.db # local path
  target: dev
```

## Example local model

In order to use the local model, we need to use the `database` parameter in the model configuration. Then we reference the model like any another model.

```
{{ config(
    database="local",
    materialized="table"
) }}
```

## Data flow

In this example, data flows from:

```mermaid
graph LR
    A[my_first_dbt_model (cloud)] --> B[my_second_dbt_model (local)]
    B --> C[my_third_dbt_model (cloud)]
```

## Running the project

Install `dbt-duckdb` and run `dbt build`. Your browser will prompt you for MotherDuck authentication unless you have token authentication configured in your shell.

