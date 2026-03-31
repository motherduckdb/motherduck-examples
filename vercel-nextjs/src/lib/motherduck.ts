import { Pool, PoolClient } from "pg";
import { attachDatabasePool } from "@vercel/functions";

const token = process.env.MOTHERDUCK_TOKEN;
const host = process.env.MOTHERDUCK_HOST ?? "pg.us-east-1-aws.motherduck.com";
const db = process.env.MOTHERDUCK_DB ?? "sample_data";

if (!token) {
  throw new Error("MOTHERDUCK_TOKEN environment variable is required");
}

const pool = new Pool({
  connectionString: `postgresql://user:${token}@${host}:5432/${db}`,
  ssl: { rejectUnauthorized: true },
  max: 10,
  idleTimeoutMillis: 5000,
});

attachDatabasePool(pool);

export async function withClient<T>(
  fn: (client: PoolClient) => Promise<T>
): Promise<T> {
  const client = await pool.connect();
  try {
    return await fn(client);
  } finally {
    client.release();
  }
}
