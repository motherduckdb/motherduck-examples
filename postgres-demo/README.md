# Connecting to Postgres with MotherDuck

This folder shows how to connect to Postgres with MotherDuck. Please refers to our [documentation](https://motherduck.com/docs/key-tasks/loading-data-into-motherduck/loading-data-md-postgres/) for more guidelines.

## Local PG configuration

We are going to use the pg_duckdb docker image for this demo. 

The first step is to run the following command to start the docker container (replace your_token with a motherduck token)

```bash
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD="very_secur3_pw" -e MOTHERDUCK_TOKEN="your_token" pgduckdb/pgduckdb:17-main -c duckdb.motherduck_enabled=true
```

By adding your motherduck token to the `MOTHERDUCK_TOKEN` environment variable, the pg_duckdb container will be able to connect to MotherDuck.

The second step is to install the DuckDB CLI. You can follow the instructions [here](https://duckdb.org/docs/installation/?version=stable&environment=cli&download_method=package_manager).

## Using the pg_scanner

Start the duckdb CLI with `./duckdb` (or if you have installed with brew or similar, just use `duckdb`).

You need to install & load the pg_scanner, which is do with the following sql.

```sql
install postgres;
load postgres;
```

Then you can attach pg with the following command:

```sql
ATTACH 'dbname=postgres user=postgres password=very_secur3_pw host=127.0.0.1' AS pg (TYPE POSTGRES);
```

To make sure all looks good, lets check the tables:

```sql
show all tables;
```

Now we can query our previously created winelist:

```sql
select * from pg.public.winelist;
```

### Exercise

Use Create table as select (CTAS) to replicate data from pg into duckdb.

## Using pg_duckdb

First we need to jump into the pg_duckdb container:

```bash
docker exec -it <container_name>  /bin/bash
```

note: if you don't know your container name, you can get it with `docker ps`.

From there we can connect to the postgres database:

```bash
psql
```

And then we can query our previously created winelist:

```sql
select *
from public.winelist;
```

### Exercise

Again we will use CTAS, this time to replicate data from MotherDuck into Postgres. Then, write a hybrid query that uses data in both postgres & motherduck together.