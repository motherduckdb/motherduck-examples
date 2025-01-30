import boto3
import uuid6
import duckdb
import os
import pyarrow as pa


class ArrowTableLoadingBuffer:
    def __init__(
        self,
        conn,
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
