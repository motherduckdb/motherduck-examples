/**
 * Connection pool example for MotherDuck using DuckDB Neo driver.
 *
 * Uses generic-pool to manage multiple connections for concurrent queries.
 * Connections are recycled via validate() to ensure freshness.
 *
 * Run: npm run pool
 */

import "dotenv/config";
import { DuckDBInstance } from "@duckdb/node-api";
import { createPool } from "generic-pool";

const token = process.env.MOTHERDUCK_TOKEN;
const database = process.env.MOTHERDUCK_DATABASE || "my_db";

if (!token) {
  console.error("Error: MOTHERDUCK_TOKEN environment variable is not set");
  console.error("Copy .env.template to .env and add your token");
  process.exit(1);
}

/**
 * Factory for creating MotherDuck connections.
 *
 * Resources are wrapped as { connection, createdAt } so the pool's validate()
 * can recycle stale connections without touching pool internals.
 */
class MDConnectionFactory {
  constructor(opts) {
    this.opts = opts;
    this.recycleTimeoutMillis = opts.recycleTimeoutMillis || 300000;
  }

  async create() {
    console.log("Creating new connection...");
    // Use fromCache() so all pooled connections share the same cached instance,
    // avoiding repeated MotherDuck extension reloads and catalog re-fetches.
    const instance = await DuckDBInstance.fromCache(`md:${this.opts.database}`, {
      motherduck_token: this.opts.token,
    });
    const connection = await instance.connect();
    // Limit threads per connection so pooled connections don't compete for CPU
    await connection.run("SET THREADS='1';");
    return { connection, createdAt: Date.now() };
  }

  async destroy(resource) {
    console.log("Destroying connection...");
    resource.connection.closeSync();
  }

  async validate(resource) {
    // Return false for stale connections — the pool will destroy and replace them
    return Date.now() - resource.createdAt < this.recycleTimeoutMillis;
  }
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

    // Validate connections before handing them out (triggers validate())
    testOnBorrow: true,
  };

  const factory = new MDConnectionFactory(poolConfig);
  const pool = createPool(factory, poolConfig);
  console.log("\n--- Running concurrent queries ---\n");

  // Simulate concurrent queries
  const queries = [
    "SELECT 'Query 1' AS name, COUNT(*) AS cnt FROM sample_data.nyc.taxi",
    "SELECT 'Query 2' AS name, AVG(trip_distance) AS avg_dist FROM sample_data.nyc.taxi",
    "SELECT 'Query 3' AS name, MAX(total_amount) AS max_amount FROM sample_data.nyc.taxi",
    "SELECT 'Query 4' AS name, MIN(trip_distance) AS min_dist FROM sample_data.nyc.taxi",
  ];

  // Execute all queries concurrently
  const promises = queries.map(async (query, index) => {
    const resource = await pool.acquire();
    console.log(`[Query ${index + 1}] Acquired connection, executing...`);

    try {
      const reader = await resource.connection.runAndReadAll(query);
      const row = reader.getRowObjects()[0];
      const metricKey = reader.columnNames().find((n) => n !== "name");
      console.log(`[Query ${index + 1}] Result: ${row.name} = ${row[metricKey]}`);
      return row;
    } finally {
      await pool.release(resource);
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
