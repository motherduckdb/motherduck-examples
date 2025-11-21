import 'dotenv/config'
import { serve } from '@hono/node-server'
import { Hono } from 'hono'
import duckdb from 'duckdb'

const app = new Hono()

const mdToken = process.env.MOTHERDUCK_TOKEN || (process.env as Record<string, string | undefined>)['motherduck_token']

if (!mdToken) {
  console.warn('MOTHERDUCK_TOKEN is not set. MotherDuck auth will fail until it is provided.')
}

const db = new duckdb.Database(mdToken ? `md:?motherduck_token=${mdToken}` : 'md:')
const conn = db.connect()

const runQuery = (sql: string, params: unknown[] = []): Promise<any[]> =>
  new Promise((resolve, reject) => {
    conn.all(sql, params, (err, rows) => {
      if (err) reject(err)
      else resolve(rows ?? [])
    })
  })

const defaultTable = process.env.MOTHERDUCK_TABLE || 'nyc_taxi_sample'

app.get('/', (c) => c.text('DuckDB Container API is running.'))

app.get('/analytics/nyc/daily_fares', async (c) => {
  const requestedTable = c.req.query('table') || defaultTable
  if (!requestedTable || !/^[A-Za-z0-9_.]+$/.test(requestedTable)) {
    return c.json(
      { error: 'Provide a MotherDuck table (letters/numbers/_/.) via MOTHERDUCK_TABLE or ?table=.' },
      400
    )
  }

  const limit = Math.min(
    Math.max(Number.parseInt(c.req.query('limit') || '14', 10) || 14, 1),
    90
  )

  try {
    const rows = await runQuery(`
      SELECT
        date_trunc('day', tpep_pickup_datetime) AS day,
        COUNT(*) AS trips,
        ROUND(SUM(total_amount), 2) AS total_fare,
        ROUND(AVG(total_amount), 2) AS avg_fare
      FROM ${requestedTable}
      GROUP BY day
      ORDER BY day DESC
      LIMIT ${limit};
    `)

    return c.json({ table: requestedTable, limit, rows })
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

async function start() {
  console.log(`Server listening on port ${port}`)
  serve({
    fetch: app.fetch,
    port
  })
}

void start()
