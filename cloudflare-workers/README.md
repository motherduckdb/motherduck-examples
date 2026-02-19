# Query MotherDuck from Cloudflare Workers

Query NYC taxi data on MotherDuck using the Postgres wire protocol from Cloudflare Workers — no DuckDB binary needed.

## Prerequisites

- [Node.js](https://nodejs.org/) (v18+)
- [Cloudflare account](https://dash.cloudflare.com/sign-up)
- [MotherDuck account](https://motherduck.com/) and access token

## Setup

```sh
cd cloudflare-workers
npm install
```

Add the MOTHERDUCK_TOKEN environment variable to your worker with the following command.
This will prompt you to sign in to Cloudflare if you haven't already, ask you to create the worker `motherduck-taxi-stats`
if it doesn't exist yet and finally ask you for the value of MOTHERDUCK_TOKEN.

```sh
npx wrangler secret put MOTHERDUCK_TOKEN
# Paste your MotherDuck token when prompted
```

## Local development

To test the worker locally you will first need to set your secret locally as well in a `.dev.vars` file.

```text
MOTHERDUCK_TOKEN="ey...MY_TOKEN"
```

Once your token is available you can start the development server.

```sh
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

```text
/stats?start=2022-11-01&end=2022-12-01
```

```json
{
  "start": "2022-11-01",
  "end": "2022-12-01",
  "total_passengers": 1234567,
  "total_fare": 1234567.89
}
```

## Deploy

Once you've adjusted the worker to your needs, you can deploy it with:

```sh
npx wrangler deploy
```

## Connection details

This example connects to MotherDuck via the [Postgres wire protocol endpoint](https://motherduck.com/docs/getting-started/interfaces/serverless/cloudflare-workers/) at `pg.us-east-1-aws.motherduck.com:5432`. The `sample_data` database is available on every MotherDuck account.
If you're connecting to an EU organization you can use `pg.eu-central-1-aws.motherduck.com:5432`

## Security

Make sure to always sanitize your inputs whenever you accept input into your application. To minimize risks you can:

### 1. Validate inputs

We use a regex to check for YYYY-MM-DD format input.

```js
const datePattern = /^\d{4}-\d{2}-\d{2}$/;
if (!datePattern.test(startDate) || !datePattern.test(endDate)) {
  return Response.json(
    { error: "Invalid date format. Use YYYY-MM-DD." },
    { status: 400 }
  );
}
```

### 2. Use parameterized inputs

Instead of directly inserting text input into your query, it is safer to use named or numbered parameters.
Make sure to use the native type (`Date()`, `Int()`, etc.) you want to use in your query.

```js
const result = await client.query(
  `SELECT
    sum(passenger_count)::INTEGER AS total_passengers,
    round(sum(fare_amount), 2) AS total_fare
  FROM nyc.taxi
  WHERE tpep_pickup_datetime >= $1
    AND tpep_pickup_datetime < $2`,
  [new Date(startDate), new Date(endDate)]
);
```
