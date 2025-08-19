# Overall outline:
# [If local testing]
    # Spin up a local Postgres
        # brew install postgresql@16
        # brew services run postgresql@16
            # note: the base user is your macOS username - check with`echo $(whoami)`
    # Create a database on Postgres to hold the catalog
    
# Attach the ducklake that is pointed at Postgres and with an S3 bucket as the backing store
# Create a blank MotherDuck Ducklake
# Copy the local Ducklake metadata DB into a MD database
# Copy the tables from the MD database into the blank MD Ducklake metadata database
    

import duckdb
import psycopg2
import os

is_local_test = True

# Review the following variables and set config variables in your environment
AWS_REGION = 'us-east-1'
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_SESSION_TOKEN = os.getenv('AWS_SESSION_TOKEN')

pg_ducklake_dbname = 'ducklake_catalog'
pg_host = 'localhost'
data_path = os.getenv('DATA_PATH')
local_ducklake_name = 'my_ducklake'
md_ducklake_name = 'my_md_ducklake'


def remove_if_exists(path: str) -> None:
    try:
        os.remove(path)
    except FileNotFoundError:
        pass
    except Exception as e:
        print(e)


def ensure_extensions(con: duckdb.DuckDBPyConnection) -> None:
    con.execute('force install ducklake from core;')
    con.execute('install postgres;')


def create_s3_secret(
    con: duckdb.DuckDBPyConnection,
    secret_name: str,
    region: str,
    key_id: str | None,
    secret: str | None,
    session_token: str | None = None,
    scope: str | None = None,
    in_motherduck: bool = False,
) -> None:
    md_clause = ' in motherduck' if in_motherduck else ''
    scope_clause = f",\n        scope '{scope}'\n    " if scope else ''
    session_clause = f",\n        session_token '{session_token}'\n    " if session_token else ''
    con.execute(
        f"""
        create or replace secret {secret_name}{md_clause} (
            type s3,
            region '{region}',
            key_id '{key_id}',
            secret '{secret}'{session_clause}{scope_clause}
        )
        """
    )


def attach_ducklake(
    con: duckdb.DuckDBPyConnection,
    dbname: str,
    host: str,
    alias: str,
    data_path_value: str,
) -> None:
    con.execute(
        f"""
        ATTACH 'ducklake:postgres:dbname={dbname} host={host}' AS {alias}
            (DATA_PATH '{data_path_value}');
        USE {alias};
        """
    )


def copy_database_to_local(
    con: duckdb.DuckDBPyConnection,
    source_database_name: str,
    local_db_name: str,
) -> None:
    remove_if_exists(f"{local_db_name}.duckdb")
    remove_if_exists(f"{local_db_name}.wal")
    con.execute(f"attach '{local_db_name}.duckdb' as {local_db_name}")
    con.execute(
        f"copy from database {source_database_name} to {local_db_name}"
    )
    con.execute(f"checkpoint {local_db_name}")
    con.execute(f"detach {local_db_name}")


def copy_tables_to_md_ducklake(
    con: duckdb.DuckDBPyConnection,
    source_db_name: str,
    target_md_ducklake_name: str,
) -> None:
    tables_to_copy_from = con.execute(
        f"""
        from duckdb_tables() select schema_name, table_name
        where database_name = '{source_db_name}'
        """
    ).fetchall()

    con.execute('BEGIN TRANSACTION')
    try:
        for schema_name, table_name in tables_to_copy_from:
            print('schema, table', schema_name, table_name)
            con.execute(
                f"truncate table __ducklake_metadata_{target_md_ducklake_name}.main.{table_name}"
            )
            con.execute(
                f"""
                insert into __ducklake_metadata_{target_md_ducklake_name}.main.{table_name}
                                from {source_db_name}.{schema_name}.{table_name}
                """
            )
        print('COMMIT')
        con.execute('COMMIT')
    except Exception as e:
        print(e)
        print('ROLLBACK')
        con.execute('ROLLBACK')

def setup_local_postgres_catalog(pg_ducklake_dbname: str) -> None:
    """Local-only: drop and recreate the Postgres DB that backs the local Duck Lake metadata."""
    user = os.getenv('PGUSER')
    pg_con = psycopg2.connect(f"dbname=postgres host=localhost user={user}")
    try:
        with pg_con.cursor() as pg_cursor:
            pg_con.autocommit = True
            pg_cursor.execute(f"drop database if exists {pg_ducklake_dbname}")
            pg_cursor.execute(f"create database {pg_ducklake_dbname}")
            _ = pg_cursor.fetchall()
    except Exception as e:
        print(e)

if is_local_test:
    setup_local_postgres_catalog(pg_ducklake_dbname)

local_con = duckdb.connect()
ensure_extensions(local_con)

# Set up S3 credentials for local connection
create_s3_secret(
    local_con,
    secret_name='s3_data',
    region=AWS_REGION,
    key_id=AWS_ACCESS_KEY_ID,
    secret=AWS_SECRET_ACCESS_KEY,
    session_token=AWS_SESSION_TOKEN,
)

attach_ducklake(
    local_con,
    dbname=pg_ducklake_dbname,
    host=pg_host,
    alias=local_ducklake_name,
    data_path_value=data_path,
)

if is_local_test:
    local_con.execute(
        """
        CREATE TABLE IF NOT EXISTS nl_train_stations AS
            FROM 'https://blobs.duckdb.org/nl_stations.csv';
        """
    )
    test_results = local_con.execute("from nl_train_stations limit 5").fetchall()
    print(test_results)

    dbs = local_con.execute("from duckdb_databases()").fetchall()
    print(dbs)

# Next, copy the PG metadata DB into a DuckDB metadata DB locally (since we have the postgres extension installed)
local_db_name = f"local_duckdb__ducklake_metadata_{local_ducklake_name}"
copy_database_to_local(
    local_con,
    source_database_name=f"__ducklake_metadata_{local_ducklake_name}",
    local_db_name=local_db_name,
)



# So, I can't pass parameters into a MD attach. So I need to persist the local metadata db to a duckdb file (delete the file if it exists ahead of time), then detach it.
# and then, make a MD connection directly, then attach to the local duckdb.
md_con = duckdb.connect('md:my_db?attach_mode=single')
md_con.execute(f"attach '{local_db_name}.duckdb' as {local_db_name}")

print(md_con.execute('show databases;').fetchall())

md_con.execute(
    f"""
    create or replace database {md_ducklake_name} (
        type ducklake,
        data_path '{data_path}'
    )
    """
)

md_con.execute(
    f"create or replace database copy_of_local_ducklake_catalog from {local_db_name}"
)

md_con.execute(f"attach 'md:__ducklake_metadata_{md_ducklake_name}';")
print(md_con.execute('from duckdb_databases();').df())

copy_tables_to_md_ducklake(
    md_con,
    source_db_name='copy_of_local_ducklake_catalog',
    target_md_ducklake_name=md_ducklake_name,
)

# Set up S3 credentials for remote MotherDuck connection
create_s3_secret(
    md_con,
    secret_name='s3_data',
    region=AWS_REGION,
    key_id=AWS_ACCESS_KEY_ID,
    secret=AWS_SECRET_ACCESS_KEY,
    session_token=AWS_SESSION_TOKEN,
    scope=data_path,
    in_motherduck=True,
)

if is_local_test:
    md_con.execute(f"use {md_ducklake_name}")
    select_test_results = md_con.execute("from nl_train_stations select count(*)").fetchall()
    print(select_test_results)

    _ = md_con.execute("insert into nl_train_stations from nl_train_stations limit 1")

    final_test_results = md_con.execute("from nl_train_stations select count(*)").fetchall()
    print(final_test_results)