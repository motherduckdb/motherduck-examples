# Query MotherDuck from Vercel + Next.js

Query NYC taxi data on MotherDuck using the Postgres wire protocol from Next.js API routes — no DuckDB binary needed.

## Prerequisites

- [Node.js](https://nodejs.org/) (v18+)
- [Vercel account](https://vercel.com/signup)
- [MotherDuck account](https://motherduck.com/) and access token

## Setup

```sh
cd vercel-nextjs
npm install
```

Copy the example env file and add your MotherDuck token:

```sh
cp .env.local.example .env.local
```

Edit `.env.local` and replace `your_token_here` with your MotherDuck access token.

## Local development

```sh
npm run dev
```

## Routes

### `GET /api/trips`

Returns the 20 most recent taxi trips from `sample_data.nyc.taxi`.

### `GET /api/stats?start=YYYY-MM-DD&end=YYYY-MM-DD`

Returns total passengers and total fare for the given date range.

**Parameters:**

- `start` — Start date (required, YYYY-MM-DD)
- `end` — End date (required, YYYY-MM-DD)

**Example:**

```text
/api/stats?start=2022-11-01&end=2022-12-01
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

Push to a Vercel-connected Git repository, or deploy manually:

```sh
npx vercel deploy
```

Add your token as a production environment variable:

```sh
npx vercel env add MOTHERDUCK_TOKEN
```

If you installed the [MotherDuck Native Integration](https://vercel.com/marketplace/motherduck), your access token is already available as an environment variable.

## Connection details

This example connects to MotherDuck via the [Postgres wire protocol endpoint](https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/postgres-endpoint) at `pg.us-east-1-aws.motherduck.com:5432`. The `sample_data` database is available on every MotherDuck account. If you're connecting to an EU organization you can use `pg.eu-central-1-aws.motherduck.com:5432`.

### Connection pooling

The example uses a module-level `pg.Pool` so connections are reused across requests within the same function instance. [`attachDatabasePool`](https://vercel.com/kb/guide/connection-pooling-with-functions) from `@vercel/functions` ensures idle connections are cleaned up before instance suspension.

### SSL

The connection uses `ssl: { rejectUnauthorized: true }` which is equivalent to PostgreSQL's `sslmode=verify-full` — Node.js verifies the server certificate against the system CA bundle and checks hostname matching. MotherDuck's endpoint uses a publicly trusted certificate, so no custom CA is needed.

## Security

Make sure to always sanitize your inputs whenever you accept input into your application. To minimize risks you can:

### 1. Validate inputs

We use a regex to check for YYYY-MM-DD format input.

```js
const datePattern = /^\d{4}-\d{2}-\d{2}$/;
if (!datePattern.test(startDate) || !datePattern.test(endDate)) {
  return NextResponse.json(
    { error: "Invalid date format. Use YYYY-MM-DD." },
    { status: 400 }
  );
}
```

### 2. Use parameterized inputs

Instead of directly inserting text input into your query, it is safer to use named or numbered parameters. Make sure to use the native type (`Date()`, `Int()`, etc.) you want to use in your query.

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
