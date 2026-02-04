/**
 * Connection pool example for MotherDuck using DuckDB Neo driver.
 *
 * Uses generic-pool to manage multiple connections for concurrent queries.
 * Connections are recycled after a timeout to ensure freshness.
 *
 * Run: npm run pool
 */

import "dotenv/config";
import { DuckDBInstance } from "@duckdb/node-api";
import * as genericPool from "generic-pool";

const token = process.env.MOTHERDUCK_TOKEN;
const database = process.env.MOTHERDUCK_DATABASE || "my_db";

if (!token) {
  console.error("Error: MOTHERDUCK_TOKEN environment variable is not set");
  console.error("Copy .env.template to .env and add your token");
  process.exit(1);
}

/**
 * Extended pool that recycles connections after a configurable timeout.
 */
class RecyclingPool extends genericPool.Pool {
  constructor(Evictor, Deque, PriorityQueue, factory, options) {
    super(Evictor, Deque, PriorityQueue, factory, options);
    this._config["recycleTimeoutMillis"] =
      typeof options.recycleTimeoutMillis === "undefined"
        ? undefined
        : parseInt(options.recycleTimeoutMillis);
    console.log(
      "Created RecyclingPool with recycleTimeout:",
      this._config["recycleTimeoutMillis"],
      "ms"
    );
  }

  release(resource) {
    const loan = this._resourceLoans.get(resource);
    const creationTime =
      typeof loan === "undefined" ? 0 : loan.pooledResource.creationTime;
    if (
      this._config["recycleTimeoutMillis"] &&
      new Date(creationTime + this._config["recycleTimeoutMillis"]) <= new Date()
    ) {
      return this.destroy(resource);
    }
    return super.release(resource);
  }
}

/**
 * Factory for creating MotherDuck connections.
 */
class MDConnectionFactory {
  constructor(opts) {
    this.opts = opts;
  }

  async create() {
    console.log("Creating new connection...");
    const instance = await DuckDBInstance.create(
      `md:${this.opts.database}?motherduck_token=${this.opts.token}`
    );
    const connection = await instance.connect();
    // Limit threads per connection for better concurrency
    await connection.run("SET THREADS='1';");
    return connection;
  }

  async destroy(connection) {
    console.log("Destroying connection...");
    return connection.closeSync();
  }
}

/**
 * Create a connection pool with recycling support.
 */
function createPool(config) {
  const factory = new MDConnectionFactory(config);
  return new RecyclingPool(
    genericPool.DefaultEvictor,
    genericPool.Deque,
    genericPool.PriorityQueue,
    factory,
    config
  );
}

async function main() {
  // Pool configuration
  const poolConfig = {
    // Connection settings
    token: token,
    database: database,

    // Pool sizing
    min: 2, // Minimum connections to keep ready
    max: 5, // Maximum concurrent connections

    // Eviction settings (idle connection cleanup)
    evictionRunIntervalMillis: 30000, // Check every 30s
    softIdleTimeoutMillis: 60000, // Soft-close after 1 min idle
    idleTimeoutMillis: 120000, // Hard-close after 2 min idle

    // Recycling (replace connections periodically for freshness)
    recycleTimeoutMillis: 300000, // Recycle after 5 min
  };

  const pool = createPool(poolConfig);
  console.log("\n--- Running concurrent queries ---\n");

  // Simulate concurrent queries
  const queries = [
    "SELECT 'Query 1' AS name, COUNT(*) AS cnt FROM sample_data.nyc.yellow_tripdata_2024_01",
    "SELECT 'Query 2' AS name, AVG(trip_distance) AS avg_dist FROM sample_data.nyc.yellow_tripdata_2024_01",
    "SELECT 'Query 3' AS name, MAX(total_amount) AS max_amount FROM sample_data.nyc.yellow_tripdata_2024_01",
    "SELECT 'Query 4' AS name, MIN(trip_distance) AS min_dist FROM sample_data.nyc.yellow_tripdata_2024_01",
  ];

  // Execute all queries concurrently
  const promises = queries.map(async (query, index) => {
    const connection = await pool.acquire();
    console.log(`[Query ${index + 1}] Acquired connection, executing...`);

    try {
      const reader = await connection.runAndReadAll(query);
      const result = reader.getRowObjects()[0];
      console.log(`[Query ${index + 1}] Result: ${result.name} = ${Object.values(result)[1]}`);
      return result;
    } finally {
      pool.release(connection);
      console.log(`[Query ${index + 1}] Released connection`);
    }
  });

  const results = await Promise.all(promises);
  console.log("\n--- All queries completed ---");
  console.table(results);

  // Cleanup
  await pool.drain();
  await pool.clear();
  console.log("\nPool closed. Done!");
}

main().catch(console.error);
