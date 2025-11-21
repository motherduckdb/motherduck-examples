# Cloudflare Workers Containers + MotherDuck (Bun, NYC Taxi)

Node-compatible DuckDB (native C++) running inside a Cloudflare Workers Container (Beta), connecting to MotherDuck. Bun is used for install/run. The sample query aggregates NYC taxi fares by day from a MotherDuck table.

## Quick start (local smoke)
1) Install deps with Bun (scripts need Node-API ABI like Node 20+):
   ```bash
   bun install
   ```
2) Copy envs and fill real values (MotherDuck only):
   ```bash
   cp .env.example .env
   # set MOTHERDUCK_TOKEN
   # optional: MOTHERDUCK_TABLE, PORT
   ```
3) Load the small NYC taxi sample into MotherDuck (one-time). Using the included Parquet file in this repo:
   ```bash
   duckdb -c "ATTACH 'md:?motherduck_token=$MOTHERDUCK_TOKEN' AS md;
   CREATE TABLE IF NOT EXISTS md.main.nyc_taxi_sample AS
   SELECT * FROM read_parquet('nyc_taxi_sample.parquet');"
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
bunx wrangler deploy
```
Test after deploy:
```bash
curl "https://duckdb-md-container.<your-zone>.workers.dev/analytics/nyc/daily_fares?limit=7"
```

## API
- `GET /` health text
- `GET /analytics/nyc/daily_fares` → daily trips/avg/total fares from a MotherDuck table; params: `limit` (default 14, max 90), `table` to override the configured table
- `POST /query` → run arbitrary SQL (`{ "sql": string, "params"?: any[] }`)

## Config
- Secrets via Wrangler: `MOTHERDUCK_TOKEN`
- Vars in `wrangler.toml` (override as needed): `MOTHERDUCK_TABLE` (default `nyc_taxi_sample`), `PORT`

## Notes
- This needs **Workers Containers (Beta)** because the native `duckdb` package cannot run on standard Workers.
- Bun image (`oven/bun`) is used in the Dockerfile; it installs native DuckDB and runs `bun run src/index.ts`.
- MotherDuck connection string is `md:`; the sample endpoint expects a table with `tpep_pickup_datetime` and `total_amount` columns.
