import 'dotenv/config'
import duckdb from 'duckdb'
const mdToken = process.env.MOTHERDUCK_TOKEN || process.env.motherduck_token || ''
const table = process.env.MOTHERDUCK_TABLE || 'nyc_taxi_sample'
const db = new duckdb.Database(mdToken ? `md:?motherduck_token=${mdToken}` : 'md:')
const conn = db.connect()

if (!/^[A-Za-z0-9_.]+$/.test(table)) {
  console.error('Set MOTHERDUCK_TABLE to a simple table path (letters/numbers/_/.)')
  process.exit(1)
}

const sql = `
SELECT
  date_trunc('day', tpep_pickup_datetime) AS day,
  COUNT(*) AS trips,
  ROUND(SUM(total_amount), 2) AS total_fare,
  ROUND(AVG(total_amount), 2) AS avg_fare
FROM ${table}
GROUP BY day
ORDER BY day DESC
LIMIT 3;`
conn.all(sql, (err, rows) => {
  if (err) {
    console.error('query error', err)
    process.exit(1)
  }
  console.log({ table, rows })
})
