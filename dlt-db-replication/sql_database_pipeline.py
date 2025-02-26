# flake8: noqa
# Import necessary libraries
import humanize  # Used for human-readable time formatting
from datetime import timedelta

# Import the DLT (Data Loading Tool) library
import dlt

# Import the SQL database source from DLT
from dlt.sources.sql_database import sql_database

# Import the pipeline metrics printing function
from timing_logs import print_pipeline_metrics


def use_config_tables() -> None:
    """
    Load tables specified in .dlt/config.toml into MotherDuck using replace mode.
    
    This function:
    1. Creates a DLT pipeline with MotherDuck as the destination
    2. Retrieves table configuration from the DLT config file
    3. Sets up a SQL database source with those tables
    4. Executes the pipeline to extract, transform, and load the data
    5. Prints detailed runtime metrics about the pipeline execution
    
    Raises:
        ValueError: If no tables are configured in the configuration file
    """
    # Initialize a DLT pipeline with MotherDuck as the destination
    # - pipeline_name: Unique identifier for the pipeline
    # - destination: Target data warehouse (MotherDuck in this case)
    # - dataset_name: Name of the dataset where tables will be created
    pipeline = dlt.pipeline(pipeline_name="pg2md", destination='motherduck', dataset_name="pg2md_data")
    
    # Retrieve the configured tables from the DLT config file
    # This expects a section in .dlt/config.toml under [sources.sql_database.tables]
    tables = dlt.config.get("sources.sql_database.tables")
    
    # Validate that tables are configured
    if not tables:
        raise ValueError("No tables configured in .dlt/config.toml under [sources.sql_database.tables]")
    
    # Create the SQL database source with the following settings:
    # - backend="connectorx": Uses the ConnectorX library for efficient data extraction
    # - parallelize(): Enables parallel processing for improved performance
    # - with_resources(*tables): Specifies which tables to extract based on config
    source = sql_database(backend="connectorx").parallelize().with_resources(*tables)
    
    # Execute the pipeline with write_disposition="replace" which:
    # - Drops existing tables and recreates them (vs. append or merge strategies)
    # - Ensures a fresh copy of the data is loaded each time
    info = pipeline.run(source, write_disposition="replace")

    # more about write disposition: https://dlthub.com/docs/general-usage/incremental-loading
    
    # Print pipeline metrics using the imported function
    print_pipeline_metrics(pipeline)


# Script entry point - executes the function when the script is run directly
if __name__ == "__main__":
    use_config_tables()
