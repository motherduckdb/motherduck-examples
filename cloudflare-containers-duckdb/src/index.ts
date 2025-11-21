import 'dotenv/config'
import { serve } from '@hono/node-server'
import { Hono } from 'hono'
import duckdb from 'duckdb'
import 'dotenv/config'

const app = new Hono()

if (!process.env.MOTHERDUCK_TOKEN) {
  console.warn('MOTHERDUCK_TOKEN is not set. MotherDuck auth will fail until it is provided.')
}

const db = new duckdb.Database('md:')
const conn = db.connect()

const runQuery = (sql: string, params: unknown[] = []): Promise<any[]> =>
  new Promise((resolve, reject) => {
    conn.all(sql, params, (err, rows) => {
      if (err) reject(err)
      else resolve(rows ?? [])
    })
  })

const r2Bucket = process.env.R2_BUCKET || 'nyc-taxi-data'
const r2Key = process.env.R2_OBJECT_KEY || 'nyc_taxi_sample.parquet'
const defaultObjectUri = `r2://${r2Bucket}/${r2Key}`

async function registerR2Secret() {
  const keyId = process.env.R2_ACCESS_KEY_ID
  const secret = process.env.R2_SECRET_ACCESS_KEY
  const accountId = process.env.R2_ACCOUNT_ID

  if (!keyId || !secret || !accountId) {
    console.warn('R2 credentials not fully set; skipping CREATE SECRET (R2 reads will fail).')
    return
  }

  try {
    await runQuery(
      `
      CREATE OR REPLACE SECRET r2_secret (
        TYPE R2,
        KEY_ID ?,
        SECRET ?,
        ACCOUNT_ID ?
      );
      `,
      [keyId, secret, accountId]
    )
    console.log('Registered R2 secret in MotherDuck session.')
  } catch (err) {
    console.error('Failed to register R2 secret:', err)
  }
}

void registerR2Secret()

app.get('/', (c) => c.text('DuckDB Container API is running.'))

app.get('/analytics/nyc/daily_fares', async (c) => {
  const objectUri = c.req.query('object') || defaultObjectUri
  const limit = Math.min(
    Math.max(Number.parseInt(c.req.query('limit') || '14', 10) || 14, 1),
    90
  )

  try {
    const rows = await runQuery(
      `
      SELECT
        date_trunc('day', tpep_pickup_datetime) AS day,
        COUNT(*) AS trips,
        ROUND(SUM(total_amount), 2) AS total_fare,
        ROUND(AVG(total_amount), 2) AS avg_fare
      FROM read_parquet(?)
      GROUP BY day
      ORDER BY day DESC
      LIMIT ?;
      `,
      [objectUri, limit]
    )

    return c.json({ object: objectUri, limit, rows })
  } catch (err: any) {
    console.error('Query failed:', err)
    return c.json({ error: err?.message || 'Query failed' }, 500)
  }
})

app.post('/query', async (c) => {
  let body: { sql?: string; params?: unknown[] }
  try {
    body = await c.req.json()
  } catch (err) {
    return c.json({ error: 'Invalid JSON body' }, 400)
  }

  if (!body.sql) {
    return c.json({ error: "Missing 'sql' in request body" }, 400)
  }

  try {
    const rows = await runQuery(body.sql, Array.isArray(body.params) ? body.params : [])
    return c.json(rows)
  } catch (err: any) {
    console.error('Custom query failed:', err)
    return c.json({ error: err?.message || 'Query failed' }, 500)
  }
})

const port = Number.parseInt(process.env.PORT || '3000', 10)
console.log(`Server listening on port ${port}`)

serve({
  fetch: app.fetch,
  port
})
