---
title: Incrementally Ingest a BigQuery Query into MotherDuck
id: flight-bigquery-ingest
description: >-
  A reusable Flight that runs a GoogleSQL query against BigQuery through the
  bigquery DuckDB community extension and loads the result into a MotherDuck
  table incrementally, using a date-partition watermark with idempotent
  per-partition DELETE plus INSERT. Use when you want scheduled, incremental
  BigQuery to MotherDuck ingestion as part of a migration off BigQuery.
type: template
features: [flights]
tags: [bigquery, ingest, migrate]
---

# Incrementally Ingest a BigQuery Query into MotherDuck

A single-file Flight that pulls a BigQuery query result into a MotherDuck table,
one date partition at a time. It is the BigQuery side of a migration onto
MotherDuck: keep BigQuery as the source of truth while you build out MotherDuck,
and let a scheduled Flight keep a MotherDuck copy current.

The pattern is a date-partition watermark. Each run figures out which days are
missing or stale, runs your GoogleSQL query for just those days through the
`bigquery` community extension, and loads each day with a DELETE plus INSERT
inside a transaction so re-running a day replaces it instead of duplicating it.
An `OVERLAP_DAYS` window re-loads the last few partitions every run so
late-arriving rows get healed.

Like [flight-freshness-alert](../flight-freshness-alert), the thing you edit is a
small set of constants at the top of `flight.py` (your `QUERY`, the destination,
the partition column), not a config blob. The generic engine below those
constants stays untouched.

## What you'll adjust

The query and destination live in USER-EDIT BLOCKS at the top of `flight.py`.
A few inputs come from outside the code: the MotherDuck token, the GCP billing
project, and the service-account credentials.

| Knob | Where | Default | Purpose |
|---|---|---|---|
| `DEFAULT_DB` | top of `flight.py` | `bigquery_ingest` | Destination database in MotherDuck. Validated as a SQL identifier. Override per run with `BIGQUERY_DEST_DB`. |
| `SCHEMA` | top of `flight.py` | `main` | Destination schema. Validated as a SQL identifier. |
| `TABLE` | top of `flight.py` | `events` | Destination table. Validated as a SQL identifier. |
| `PARTITION_DATE_COLUMN` | top of `flight.py` | `event_date` | Column in `QUERY`'s result holding the partition date. Drives the watermark. Must be a DATE and must exist in the result. |
| `COLD_START_DT` | top of `flight.py` | `2024-01-01` | First day to load when the destination table is empty. A `datetime.date`. |
| `OVERLAP_DAYS` | top of `flight.py` | `3` | Watermark re-load window in days. Each run re-loads the last `OVERLAP_DAYS` partitions to heal late data. `0` = forward-only. |
| `QUERY` | top of `flight.py` | sample events query | The BigQuery GoogleSQL string. Must return `PARTITION_DATE_COLUMN` and filter on `{start_dt}`/`{end_dt}` so BigQuery prunes partitions. |
| `MD_DELETE_PREDICATE` | top of `flight.py` | mirrors `QUERY` | A DuckDB WHERE clause that clears one partition before re-insert. Must mirror `QUERY`'s predicates. |
| `build_filters()` | top of `flight.py` | `start_dt`, `end_dt`, `event_source` | Returns the `{placeholder}` values for `QUERY` and `MD_DELETE_PREDICATE`. Reads per-run overrides (e.g. `EVENT_SOURCE`) from the environment. |
| `GCP_PROJECT_ID` | Flight config / env var | (required) | The GCP billing project charged for `bigquery_query`. Bound as a parameter and validated as a project id. |
| `GOOGLE_APPLICATION_CREDENTIALS` | env var (local) | (unset) | Path to a service-account JSON file on disk. Used for a local run. |
| `GOOGLE_APPLICATION_CREDENTIALS_JSON` | Flight secret | (unset) | The service-account JSON itself, provided through a `TYPE flights` secret. Deployed, it arrives as `<secret_name>_GOOGLE_APPLICATION_CREDENTIALS_JSON`; `flight.py` resolves either name. |
| `start_dt` / `end_dt` / `target_dt` | env var (optional) | (unset) | Override the window: explicit inclusive backfill range, or a single day. Unset = watermark mode. |
| `BIGQUERY_DEST_DB` | Flight config / env var (optional) | `DEFAULT_DB` | Override the destination database without editing code. |
| `MOTHERDUCK_TOKEN` | Flight-injected | (Flight-injected) | Auth. Select a token on the Flight; never hard-code it. |

## Questions to answer

- What GoogleSQL query do you want to load, and which result column is the partition date?
- Which BigQuery table or view does the query read, and is it partitioned on that date so the `{start_dt}`/`{end_dt}` filter actually prunes scans?
- What is the destination `database.schema.table` in MotherDuck?
- What is your GCP billing project (the project charged for the query), and is it the right one for cost attribution?
- How far back should a cold start go (`COLD_START_DT`), and how many days of overlap heal late-arriving data (`OVERLAP_DAYS`)?
- Do `QUERY` and `MD_DELETE_PREDICATE` filter on exactly the same predicates, so the DELETE never clears rows you are not re-loading?
- Which service account can run the query, and how is its JSON key stored (a file locally, a `TYPE flights` secret when deployed)?
- What schedule (cron, UTC) matches how often the source data lands?

## Run it

You need a MotherDuck account and access token, a GCP project to bill the
query, and a service-account JSON key with permission to run the query in
BigQuery. Unlike the `sample_data` templates here, there is no credential-free
smoke test: BigQuery access is required (see [Caveats](#caveats)).

```bash
export MOTHERDUCK_TOKEN=your_token_here
export GCP_PROJECT_ID=your-gcp-billing-project
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json
# optional: a one-day load or an explicit inclusive backfill range
# export target_dt=2024-03-01
# export start_dt=2024-03-01
# export end_dt=2024-03-07
uv run --with-requirements requirements.txt flight.py
```

With the constants edited to your query and destination, this connects to
MotherDuck, installs and loads the `bigquery` extension, bootstraps the
destination table from `QUERY`'s schema, resolves the partition window, and
loads each day. With no `target_dt`/`start_dt`/`end_dt` set it runs in watermark
mode: a cold start loads `COLD_START_DT`, and later runs load the days after the
destination's current maximum partition date (plus the `OVERLAP_DAYS` overlap).

### Deploy as a Flight

The service-account JSON is a secret, so store it as a MotherDuck **Flights
secret**. The simplest way is the MotherDuck UI: open
[Settings > Secrets](https://app.motherduck.com/settings/secrets), add a secret of
type **Flights**, and give it a `GOOGLE_APPLICATION_CREDENTIALS_JSON` parameter
whose value is the entire JSON key as one string. If you would rather use SQL, you
can create the same secret from the DuckDB client or any SQL connection (the
read-only `query` MCP tool rejects `CREATE SECRET`, so use `query_rw` or a direct
connection):

```sql
CREATE SECRET gcp_creds IN motherduck (
  TYPE flights,
  PARAMS MAP { 'GOOGLE_APPLICATION_CREDENTIALS_JSON': '{"type":"service_account", ...}' }
);
```

A `TYPE flights` secret injects each param under the env var
`<secret_name>_<PARAM>`, not the bare param name: the param above arrives as
`gcp_creds_GOOGLE_APPLICATION_CREDENTIALS_JSON`, not
`GOOGLE_APPLICATION_CREDENTIALS_JSON`. (DuckDB lowercases the unquoted secret name
into the prefix.) `flight.py` handles this: it reads the bare
`GOOGLE_APPLICATION_CREDENTIALS_JSON` for local runs and otherwise picks up any
env var ending in `_GOOGLE_APPLICATION_CREDENTIALS_JSON`, then writes the JSON to
a temp file and points `GOOGLE_APPLICATION_CREDENTIALS` at it. The secret name you
choose does not matter.

Then deploy with the MotherDuck MCP server rather than checked-in SQL. Call
`get_flight_guide` first for the exact tool arguments, then `create_flight` with:

- `source_code`: the contents of [`flight.py`](flight.py), with the USER-EDIT
  BLOCKS edited to your query and destination
- `requirements_txt`: the contents of [`requirements.txt`](requirements.txt)
- `access_token_name`: a service account token that can write the destination
  (list tokens with the `md_access_tokens()` table function); the runtime
  injects its value as `MOTHERDUCK_TOKEN`
- `flight_secret_names`: `["gcp_creds"]` so the SA JSON is injected (as
  `gcp_creds_GOOGLE_APPLICATION_CREDENTIALS_JSON`; `flight.py` resolves it)
- `config`: `GCP_PROJECT_ID` (and optionally `BIGQUERY_DEST_DB`, `EVENT_SOURCE`).
  The billing project is not a secret, so it belongs in config. The SA JSON does
  NOT: keep it in the secret above.

Create the Flight without a schedule first, trigger one manual run with
`run_flight`, and confirm it loads the cold-start partition. Then add a schedule
(for example `0 6 * * *`, daily at 06:00 UTC) by updating the Flight's
`schedule_cron`. Schedule updates are metadata-only and do not create a new
Flight version. For a one-off backfill, set `start_dt`/`end_dt` (or `target_dt`)
in the run config.

## How it works

`flight.py` runs a fixed sequence; only the USER-EDIT BLOCKS change its inputs:

1. Materialize GCP credentials. If `GOOGLE_APPLICATION_CREDENTIALS` already
   points at a file (local run), use it. Otherwise read the SA JSON from the
   resolved secret env var, validate it parses as JSON, write it to a private
   temp file, and set `GOOGLE_APPLICATION_CREDENTIALS` to that path.
2. Connect DuckDB in memory with community extensions enabled, `INSTALL`/`LOAD`
   the `bigquery` extension, `LOAD motherduck`, `ATTACH 'md:'`, and set
   `preserve_insertion_order = FALSE`.
3. Ensure the destination exists. Create the database and schema, then bootstrap
   the table by running `QUERY` over a 1970-01-01 window (BigQuery prunes the
   partitioned scan to nothing, so the probe is nearly free) and
   `CREATE TABLE IF NOT EXISTS ... AS SELECT * FROM bigquery_query(?, ?) LIMIT 0`,
   which takes the result schema without rows.
4. Resolve the partition window. Priority: explicit `start_dt`+`end_dt`
   (inclusive backfill range), then `target_dt` (single day), otherwise watermark
   mode (`prev_max = MAX(partition column)`; load
   `[prev_max + 1 - OVERLAP_DAYS, prev_max + 1]`; cold start at `COLD_START_DT`
   when empty).
5. For each day in the window, run one transaction: `DELETE` rows matching
   `MD_DELETE_PREDICATE` for that day, then `INSERT` the day's
   `bigquery_query(GCP_PROJECT_ID, QUERY)` result. Commit on success, roll back
   on error. Per-phase timing (DELETE, then BigQuery scan plus MotherDuck insert)
   is logged to stdout, which Flight logs capture.

The DELETE plus INSERT per partition is what makes a re-run safe: re-loading a
day clears its rows first, so you never get duplicates, and a failed day rolls
back cleanly rather than leaving a half-loaded partition.

## Caveats

- **No credential-free smoke test.** Unlike the `sample_data` templates here,
  this Flight needs a real BigQuery source: a GCP billing project and a
  service-account key. There is no public dataset path that exercises the whole
  flow without credentials.
- **BigQuery costs money.** `bigquery_query` runs through the BigQuery Jobs API,
  which is billed by bytes scanned. Push your date filter into `QUERY`'s WHERE on
  a partitioned column so BigQuery prunes partitions and only scans the days you
  load. A query that scans the full table every run gets expensive fast. See
  [Learn more](#learn-more) for `bigquery_scan` as a cheaper alternative for
  simple table reads.
- **Predicate mirroring is a footgun.** `MD_DELETE_PREDICATE` must select exactly
  the rows one partition's `QUERY` run produces. If `QUERY` filters on
  `event_source` but the DELETE does not, the DELETE clears rows you are not
  re-loading and you lose data. Keep them in lockstep, and note the quoting
  difference: GoogleSQL uses double quotes for string literals, DuckDB uses single
  quotes.
- **Watermark and time zones.** The watermark is computed from
  `MAX(PARTITION_DATE_COLUMN)`, a date, so it is timezone-free as long as the
  column is a true partition DATE. If your partition date is derived from a
  timestamp, derive it the same way in `QUERY` every run (for example
  `DATE(event_timestamp)` in a consistent zone), or the day boundaries will drift.
  A deployed Flight runs in UTC; a local `uv run` uses your machine's timezone.
- **`OVERLAP_DAYS` trades cost for freshness.** A larger overlap heals
  later-arriving data but re-scans and re-loads more partitions every run. Set it
  to the longest delay you expect for late rows, no more.
- **Cold start can be large.** A watermark cold start loads a single day
  (`COLD_START_DT`). To backfill history, run once with `start_dt`/`end_dt`
  spanning the range before you rely on the watermark.

## Security

- **SA JSON in a secret, not config.** The service-account JSON is a credential,
  so it must come from a MotherDuck `TYPE flights` secret, never from plaintext
  Flight `config`. Putting it in config (as the original internal template did) is
  a hack: config is not treated as sensitive. The team decision is to move the SA
  JSON to a secret, so this template reads it from the secret-injected env var and
  only ever writes it to a private temp file in the Flight's ephemeral runtime.
- **Identifier validation.** `DEFAULT_DB` (and its `BIGQUERY_DEST_DB` override),
  `SCHEMA`, `TABLE`, and `PARTITION_DATE_COLUMN` are checked against
  `^[A-Za-z_][A-Za-z0-9_]*$` before any SQL runs, because they flow into
  `CREATE`/`USE`/`SELECT`/`DELETE`/`INSERT` statements that cannot be
  parameterized, and are quoted with `ident()`. `GCP_PROJECT_ID` is validated as a
  GCP project id.
- **Parameterized data values.** The billing project and the rendered query string
  are bound as parameters to `bigquery_query(?, ?)`, never f-string'd into the SQL
  statement. The `{placeholder}` values in `QUERY`/`MD_DELETE_PREDICATE` are values
  you control in `build_filters()`, not untrusted input.

## Learn more

- Flight mechanics (creating, running, scheduling, secrets): use the MotherDuck
  MCP `get_flight_guide` tool.
- `bigquery_query` (Jobs API, billed by bytes scanned) vs `bigquery_scan('table',
  filter=...)` (Storage Read API, cheaper when you do not need server-side joins,
  views, or GoogleSQL functions): the
  [duckdb-bigquery community extension](https://github.com/hafenkran/duckdb-bigquery)
  documents both. This template uses `bigquery_query` so you can run arbitrary
  GoogleSQL.
- Deeper MotherDuck or DuckDB questions: use the `ask_docs_question` MCP tool.
- Files in this template: [`flight.py`](flight.py) (the single-file Flight source)
  and [`requirements.txt`](requirements.txt) (just `duckdb`; the `bigquery`
  extension is a runtime community extension, not a pip package).
