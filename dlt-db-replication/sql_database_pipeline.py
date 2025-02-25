# flake8: noqa
import humanize

import dlt

from dlt.sources.sql_database import sql_database


def use_config_tables() -> None:
    """Load tables specified in .dlt/config.toml into MotherDuck using replace mode."""
    pipeline = dlt.pipeline(pipeline_name="pg2md", destination='motherduck', dataset_name="pg2md_data")
     # pipeline.schemas["default"].update_column("ship_mode", "_dlt_load_id", nullable=True)
    
    # Get the list of tables from the configuration
    tables = dlt.config.get("sources.sql_database.tables")
    # tables = dlt.config.get("sources.sql_database.table")

    
    if not tables:
        raise ValueError("No tables configured in .dlt/config.toml under [sources.sql_database.tables]")
    
    # Create the source with the configured tables
    # source = sql_database(backend="connectorx", backend_kwargs={"return_type": "arrow"}).with_resources(*tables)
    source = sql_database(backend="connectorx").with_resources(*tables)
    
    # Run the pipeline in replace mode
    info = pipeline.run(source, write_disposition="replace")
    print('*****RUNTIME INFO*****')
    print(humanize.precisedelta(pipeline.last_trace.finished_at - pipeline.last_trace.started_at))
    print('*****EXTRACT INFO*****')
    print(pipeline.last_trace.last_extract_info)
    print('*****NORMALIZE INFO***** ')
    print(pipeline.last_trace.last_normalize_info)
    print('*****LOAD INFO*****')
    print(pipeline.last_trace.last_load_info)


if __name__ == "__main__":
    # Load selected tables with different settings
    #load_select_tables_from_database()
    use_config_tables()

    # load a table and select columns
    # select_columns()

    # load_entire_database()
    # select_with_end_value_and_row_order()

    # Load tables with the standalone table resource
    # load_standalone_table_resource()

    # Load all tables from the database.
    # Warning: The sample database is very large
    # load_entire_database()
