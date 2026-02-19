/**
 * Basic MotherDuck connection and query examples using DuckDB Neo driver.
 *
 * Run: npm run basic
 */

import "dotenv/config";
import { DuckDBInstance } from "@duckdb/node-api";

const token = process.env.MOTHERDUCK_TOKEN;
const database = process.env.MOTHERDUCK_DATABASE || "my_db";

if (!token) {
  console.error("Error: MOTHERDUCK_TOKEN environment variable is not set");
  console.error("Copy .env.template to .env and add your token");
  process.exit(1);
}

async function main() {
  // Connect to MotherDuck using fromCache() to avoid reinitializing
  // the MotherDuck extension on every connection. Unlike Python/R/JDBC,
  // the Node.js client does NOT cache instances automatically.
  console.log(`Connecting to MotherDuck database: ${database}...`);
  const instance = await DuckDBInstance.fromCache(
    `md:${database}?motherduck_token=${token}`
  );
  const connection = await instance.connect();
  console.log("Connected!\n");

  // Example 1: Simple query
  console.log("--- Example 1: Simple query ---");
  const reader = await connection.runAndReadAll("SELECT 42 AS answer");
  console.table(reader.getRowObjects());
  console.log();

  // Example 2: Create and query a table
  console.log("--- Example 2: Create and query a table ---");
  await connection.run(`
    CREATE OR REPLACE TABLE example_users (
      id INTEGER,
      name VARCHAR,
      email VARCHAR
    )
  `);

  await connection.run(`
    INSERT INTO example_users VALUES
      (1, 'Alice', 'alice@example.com'),
      (2, 'Bob', 'bob@example.com'),
      (3, 'Charlie', 'charlie@example.com')
  `);

  const users = await connection.runAndReadAll("FROM example_users");
  console.table(users.getRowObjects());
  console.log();

  // Example 3: Parameterized queries
  console.log("--- Example 3: Parameterized query ---");
  const prepared = await connection.prepare(
    "SELECT * FROM example_users WHERE id = $1"
  );
  prepared.bindInteger(1, 2);
  const userReader = await prepared.runAndReadAll();
  console.table(userReader.getRowObjects());
  console.log();

  // Example 4: Aggregate query
  console.log("--- Example 4: Aggregate query ---");
  const countReader = await connection.runAndReadAll(`
    SELECT COUNT(*) AS total_users FROM example_users
  `);
  console.table(countReader.getRowObjects());
  console.log();

  // Example 5: Query MotherDuck sample data
  console.log("--- Example 5: Query sample data from MotherDuck ---");
  const sampleReader = await connection.runAndReadAll(`
    SELECT * FROM sample_data.nyc.yellow_tripdata_2024_01 LIMIT 5
  `);
  console.log("NYC taxi sample (5 rows):");
  console.table(sampleReader.getRowObjects());
  console.log();

  // Example 6: Multiple connections via cached instance
  console.log("--- Example 6: Reuse cached instance for a second connection ---");
  const instance2 = await DuckDBInstance.fromCache(
    `md:${database}?motherduck_token=${token}`
  );
  const connection2 = await instance2.connect();
  const versionReader = await connection2.runAndReadAll(
    "SELECT version() AS duckdb_version, current_database() AS db"
  );
  console.table(versionReader.getRowObjects());
  connection2.closeSync();
  console.log();

  // Cleanup
  await connection.run("DROP TABLE IF EXISTS example_users");
  connection.closeSync();
  console.log("Done!");
}

main().catch(console.error);
