# Query MotherDuck from Cloudflare Workers

Query NYC taxi data on MotherDuck using the Postgres wire protocol from Cloudflare Workers — no DuckDB binary needed.

## Prerequisites

- [Node.js](https://nodejs.org/) (v18+)
- [Cloudflare account](https://dash.cloudflare.com/sign-up)
- [MotherDuck account](https://motherduck.com/) and access token

## Setup

```bash
cd cloudflare-workers
npm install
npx wrangler secret put MOTHERDUCK_TOKEN
# Paste your MotherDuck token when prompted
```

## Local development

```bash
npx wrangler dev
```

## Routes

### `GET /`

Returns the 20 most recent taxi trips from `sample_data.nyc.taxi`.

### `GET /stats?start=YYYY-MM-DD&end=YYYY-MM-DD`

Returns total passengers and total fare for the given date range.

**Parameters:**
- `start` — Start date (default: `2022-01-01`)
- `end` — End date (default: `2022-12-31`)

**Example:**
```
/stats?start=2022-06-01&end=2022-07-01
```

```json
{
  "start": "2022-06-01",
  "end": "2022-07-01",
  "total_passengers": 1234567,
  "total_fare": 23456789.12
}
```

## Deploy

```bash
npx wrangler deploy
```

## Connection details

This example connects to MotherDuck via the [Postgres wire protocol endpoint](https://motherduck.com/docs/getting-started/interfaces/serverless/cloudflare-workers/) at `pg.us-east-1-aws.motherduck.com:5432`. The `sample_data` database is available on every MotherDuck account.
