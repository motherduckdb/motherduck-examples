# Cloudflare Workers Containers + MotherDuck (Bun, NYC Taxi)

Node-compatible DuckDB (native C++) running inside a Cloudflare Workers Container (Beta), connecting to MotherDuck and reading Parquet straight from Cloudflare R2. Bun is used for install/run. The sample query aggregates NYC taxi fares by day.

## Quick start (local smoke)
1) Install deps with Bun (scripts need Node-API ABI like Node 20+):
   ```bash
   bun install
   ```
2) Copy envs and fill real values (MotherDuck + R2):
   ```bash
   cp .env.example .env
   # set MOTHERDUCK_TOKEN, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ACCOUNT_ID
   # optional: R2_BUCKET, R2_OBJECT_KEY, PORT
   ```
3) Put NYC taxi data in R2 (small slice, ~50k rows) so the sample endpoint works:
   ```bash
   wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
   duckdb -c "COPY (SELECT vendorid, tpep_pickup_datetime, total_amount FROM read_parquet('yellow_tripdata_2024-01.parquet') LIMIT 50000) TO 'nyc_taxi_sample.parquet' (FORMAT PARQUET);"
   aws s3 cp nyc_taxi_sample.parquet s3://$R2_BUCKET/nyc_taxi_sample.parquet \
     --endpoint-url https://$R2_ACCOUNT_ID.r2.cloudflarestorage.com
   ```
4) Run locally:
   ```bash
   bun dev
   curl "http://localhost:3000/analytics/nyc/daily_fares?limit=7"
   curl -X POST http://localhost:3000/query -H 'content-type: application/json' \
     -d '{"sql":"select 1 as ok"}'
   ```

## Deploy to Cloudflare Workers Containers (Beta)
```bash
bun install
bunx wrangler login
bunx wrangler secret put MOTHERDUCK_TOKEN
bunx wrangler secret put R2_ACCESS_KEY_ID
bunx wrangler secret put R2_SECRET_ACCESS_KEY
bunx wrangler secret put R2_ACCOUNT_ID
bunx wrangler deploy
```
Test after deploy:
```bash
curl "https://duckdb-r2-container.<your-zone>.workers.dev/analytics/nyc/daily_fares?limit=7"
```

## API
- `GET /` health text
- `GET /analytics/nyc/daily_fares` → daily trips/avg/total fares from `r2://$R2_BUCKET/$R2_OBJECT_KEY`; params: `limit` (default 14, max 90), `object` to override URI
- `POST /query` → run arbitrary SQL (`{ "sql": string, "params"?: any[] }`)

## Config
- Secrets via Wrangler: `MOTHERDUCK_TOKEN`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_ACCOUNT_ID`
- Vars in `wrangler.toml` (override as needed): `R2_BUCKET`, `R2_OBJECT_KEY`, `PORT`

## Notes
- This needs **Workers Containers (Beta)** because the native `duckdb` package cannot run on standard Workers.
- Bun image (`oven/bun`) is used in the Dockerfile; it installs native DuckDB and runs `bun run src/index.ts`.
- MotherDuck connection string is `md:`; secrets are registered once at startup with `CREATE OR REPLACE SECRET r2_secret`.
