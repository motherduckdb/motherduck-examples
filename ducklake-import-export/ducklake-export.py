import duckdb
import psycopg2
import os
import pandas  # noqa: F401

# This script creates a Duck Lake in MotherDuck, exports its metadata to a local DuckDB file,
# then loads that metadata into a local Postgres-backed Duck Lake which points to the same S3 data path.

is_local_test = True

# Review and/or set these via your shell environment
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


def attach_postgres_alias(
    con: duckdb.DuckDBPyConnection,
    alias: str,
    dbname: str,
    host: str,
    user: str | None,
) -> None:
    user_clause = f" user={user}" if user else ''
    con.execute(
        f"""
        ATTACH 'dbname={dbname} host={host}{user_clause}' AS {alias} (TYPE postgres);
        """
    )


def export_md_ducklake_metadata(md_lake_name: str, local_meta_db_name: str) -> None:
    remove_if_exists(f"{local_meta_db_name}.duckdb")
    remove_if_exists(f"{local_meta_db_name}.wal")

    md_meta_con = duckdb.connect(
        f"md:__ducklake_metadata_{md_lake_name}?attach_mode=single"
    )
    try:
        md_meta_con.execute(
            f"attach '{local_meta_db_name}.duckdb' as {local_meta_db_name}"
        )
        md_meta_con.execute(
            f"copy from database __ducklake_metadata_{md_lake_name} to {local_meta_db_name}"
        )
        md_meta_con.execute(f"checkpoint {local_meta_db_name}")
        md_meta_con.execute(f"detach {local_meta_db_name}")
    finally:
        md_meta_con.close()


def copy_metadata_tables_to_postgres(
    con: duckdb.DuckDBPyConnection,
    source_local_meta_db_name: str,
    target_pg_alias: str,
) -> None:
    con.execute(f"attach '{source_local_meta_db_name}.duckdb' as {source_local_meta_db_name}")

    tables_to_copy = con.execute(
        f"""
        from duckdb_tables()
        select schema_name, table_name
        where database_name = '{source_local_meta_db_name}'
        """
    ).fetchall()

    con.execute('BEGIN TRANSACTION')
    try:
        for schema_name, table_name in tables_to_copy:
            print('schema, table', schema_name, table_name)
            con.execute(f"truncate table {target_pg_alias}.{table_name}")
            con.execute(
                f"""
                insert into {target_pg_alias}.{table_name}
                from {source_local_meta_db_name}.{schema_name}.{table_name}
                """
            )
        print('COMMIT')
        con.execute('COMMIT')
    except Exception as e:
        print(e)
        print('ROLLBACK')
        con.execute('ROLLBACK')


def setup_local_postgres_catalog(pg_dbname: str) -> None:
    """Local-only: drop and recreate the Postgres DB that backs the local Duck Lake metadata.

    Assumes passwordless local Postgres for the current user.
    """
    user = os.getenv('PGUSER')
    pg_con = psycopg2.connect(f"dbname=postgres host=localhost user={user}")
    try:
        with pg_con.cursor() as pg_cursor:
            pg_con.autocommit = True
            pg_cursor.execute(f"drop database if exists {pg_dbname}")
            pg_cursor.execute(f"create database {pg_dbname}")
            _ = pg_cursor.fetchall()
    except Exception as e:
        print(e)


if is_local_test:
    setup_local_postgres_catalog(pg_ducklake_dbname)


# 1) Create MotherDuck connection and Duck Lake
md_con = duckdb.connect('md:my_db?attach_mode=single')

# Register S3 credentials in MotherDuck with a scoped secret for the Duck Lake data path
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

# Create or replace the MotherDuck Duck Lake database
md_con.execute(f"""
    create or replace database {md_ducklake_name} (
        type ducklake,
        data_path '{data_path}'
    )
""")

if is_local_test:
    # Create a demo table in MD Duck Lake to verify export works locally
    md_con.execute(f"use {md_ducklake_name}")
    md_con.execute(
        """
        create table if not exists nl_train_stations as
            from 'https://blobs.duckdb.org/nl_stations.csv'
        """
    )
    print(md_con.execute("from nl_train_stations limit 5").fetchall())

# 2) Export MD Duck Lake metadata to a local DuckDB file
local_md_meta_db_name = f"local_duckdb__ducklake_metadata_{md_ducklake_name}"

md_con.close()

export_md_ducklake_metadata(md_ducklake_name, local_md_meta_db_name)


# 3) Create local Duck Lake (Postgres-backed) and load the exported metadata
local_con = duckdb.connect()
ensure_extensions(local_con)

# Local S3 secret for the Duck Lake runtime
create_s3_secret(
    local_con,
    secret_name='s3_data',
    region=AWS_REGION,
    key_id=AWS_ACCESS_KEY_ID,
    secret=AWS_SECRET_ACCESS_KEY,
    session_token=AWS_SESSION_TOKEN,
)

# Attach the local Duck Lake to Postgres and point it at the same S3 data path
attach_ducklake(
    local_con,
    dbname=pg_ducklake_dbname,
    host=pg_host,
    alias=local_ducklake_name,
    data_path_value=data_path,
)

local_con.close()

# Now that the Duck Lake exists, load metadata into it
local_con = duckdb.connect()
ensure_extensions(local_con)
user = os.getenv('PGUSER')
attach_postgres_alias(
    local_con,
    alias='pg_db',
    dbname=pg_ducklake_dbname,
    host='localhost',
    user=user,
)

copy_metadata_tables_to_postgres(
    local_con,
    source_local_meta_db_name=local_md_meta_db_name,
    target_pg_alias='pg_db',
)

# close local con
local_con.close()

# reopen local con and connect to ducklake
local_con = duckdb.connect()
ensure_extensions(local_con)
attach_ducklake(
    local_con,
    dbname=pg_ducklake_dbname,
    host=pg_host,
    alias=local_ducklake_name,
    data_path_value=data_path,
)


# 4) Validate locally against the same S3 data path
if is_local_test:
    local_con.execute(f"use {local_ducklake_name}")
    select_test_results = local_con.execute(
        "from nl_train_stations select count(*)"
    ).fetchall()
    print(select_test_results)

    _ = local_con.execute(
        "insert into nl_train_stations from nl_train_stations limit 1"
    )
    final_test_results = local_con.execute(
        "from nl_train_stations select count(*)"
    ).fetchall()
    print(final_test_results)


