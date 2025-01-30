import duckdb
import pyarrow as pa

import requests
import pyarrow as pa
import duckdb
import logging
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ArrowTableLoadingBuffer:
    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        pyarrow_schema: pa.Schema,
        table_name: str,
        chunk_size: int = 100000,  # typical good chunk size for DuckDB
    ):
        self.conn = conn
        self.pyarrow_schema = pyarrow_schema
        self.table_name = table_name
        self.total_inserted = 0
        self.chunk_size = chunk_size

    def insert(self, table: pa.Table):
        total_rows = table.num_rows
        for batch_start in range(0, total_rows, self.chunk_size):
            batch_end = min(batch_start + self.chunk_size, total_rows)
            chunk = table.slice(batch_start, batch_end - batch_start)
            self.insert_chunk(chunk)
            logging.info(f"Inserted chunk {batch_start} to {batch_end}")
        self.total_inserted += total_rows
        logging.info(f"Total inserted: {self.total_inserted} rows")

    def insert_chunk(self, chunk: pa.Table):
        self.conn.register("buffer_table", chunk)
        insert_query = f"INSERT INTO {self.table_name} SELECT * FROM buffer_table"
        self.conn.execute(insert_query)
        self.conn.unregister("buffer_table")



def fetch_github_data():
    url = 'https://api.github.com/repos/duckdb/duckdb/stats/contributors'
    logger.info(f"Fetching data from {url}")
    response = requests.get(url)
    response.raise_for_status()
    logger.info("Data fetched successfully")
    return response.json()

def process_data(data):
    logger.info("Processing data")
    records = []
    for author in data:
        total_commits = sum(week['c'] for week in author['weeks'])
        records.append({
            'login': author['author']['login'],
            'total_commits': total_commits
        })
    schema = pa.schema([
        ('login', pa.string()),
        ('total_commits', pa.int64())
    ])
    table = pa.Table.from_pylist(records, schema=schema)
    logger.info("Data processed into Arrow Table")
    return table

def main():
    logger.info("Starting main process")
    data = fetch_github_data()
    table = process_data(data)
    
    # Connect to MotherDuck and create a table
    con = duckdb.connect()
    logger.info("Connecting to MotherDuck")
    con.execute("ATTACH 'md:'")
    con.execute("CREATE DATABASE IF NOT EXISTS github")
    
    con.execute("""
        CREATE TABLE IF NOT EXISTS github.github_commits_large (
            login VARCHAR,
            total_commits BIGINT
        )
    """)
    
    # Insert data using ArrowTableLoadingBuffer
    buffer = ArrowTableLoadingBuffer(
        conn=con,
        pyarrow_schema=table.schema,
        table_name="github.github_commits_large",
        chunk_size=10  # Small chunk size for demonstration
    )
    buffer.insert(table)

    logger.info("Data loaded into MotherDuck successfully")

if __name__ == "__main__":
    main()
