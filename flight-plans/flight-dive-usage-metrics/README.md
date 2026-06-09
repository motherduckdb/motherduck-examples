---
title: Track Most-Used Tables Across Dives
id: flight-dive-usage-metrics
description: >-
  A config-driven Flight that scans the shared Dives in your MotherDuck
  organization, parses the SQL embedded in each Dive, and refreshes a metrics
  table of the most-referenced tables, databases, schemas, and columns. Use when
  you want a scheduled, trend-aware view of which data objects your Dives lean on
  most.
type: template
features: [flights, dives]
tags: []
---

# Track Most-Used Tables Across Dives

A single-file Flight that turns the SQL inside your organization's Dives into a
metrics table. Each run enumerates the Dives, extracts the SQL each one embeds,
parses that SQL with DuckDB's own parser, and counts which tables, databases,
schemas, and columns get referenced. It writes one timestamped batch of rows per
run, so you can watch which data objects your Dives depend on and how that shifts
over time.

This answers questions like "which tables are most central to our Dives", "which
databases would be risky to rename", and "what columns do our dashboards lean on".
It reads Dives read-only and writes only to the target database you configure.

Everything is driven by Flight config, so you adapt it by setting config values,
not by editing `flight.py`.

## What you'll adjust

Every knob is a config/env value read at the top of `flight.py`. Set them as
Flight config, not by editing code.

| Config key | Default | Purpose |
|---|---|---|
| `TARGET_DATABASE` | `dive_metrics` | Database that holds the metrics table. Created if missing. Validated as a SQL identifier. |
| `TARGET_SCHEMA` | `main` | Schema for the metrics table. Created if missing. Validated as a SQL identifier. |
| `METRICS_TABLE` | `dive_usage_metrics` | Metrics table name. A companion `<table>_latest` view exposes the newest run. Validated as a SQL identifier. |
| `INCLUDE_ORG_SHARES` | `true` | When true, scan every Dive shared in the organization via `MD_LIST_DIVES(include_org_shares := true)`. When false, scan only the token owner's own Dives. |
| `DIVE_LIMIT` | (unset) | Optional cap on how many Dives to scan, useful for a quick first run. Unset or `0` means no limit. |
| `MOTHERDUCK_TOKEN` | (Flight-injected) | Auth. Use a token that can read the Dives you want counted and write `TARGET_DATABASE`. Never put it in config. |

## Questions to answer

- Should the metrics cover every Dive shared in the organization (`INCLUDE_ORG_SHARES=true`), or only the token owner's own Dives?
- Where should the metrics live (`TARGET_DATABASE`, `TARGET_SCHEMA`, `METRICS_TABLE`), and is that database writable by the chosen token?
- Which service account token can read the Dives you care about and write the target database?
- How often should the snapshot refresh (daily or weekly), given how often Dives change?
- Do you need a quick bounded first run (`DIVE_LIMIT`) before scanning everything?

## Run it

You need a MotherDuck account and an access token. This template has a
credential-free smoke test: with only a `MOTHERDUCK_TOKEN`, a fresh run produces
real metrics from your own and your organization's shared Dives. No other
credentials or setup are needed, because Dives, `MD_LIST_DIVES`, `MD_GET_DIVE`,
and `json_serialize_sql` are all built into MotherDuck.

```bash
export MOTHERDUCK_TOKEN=your_token_here
# optional: bound the first run while you confirm the output
export DIVE_LIMIT=25
uv run --with-requirements requirements.txt flight.py
```

This enumerates the Dives, parses the SQL in each, and writes the metric rows to
`dive_metrics.main.dive_usage_metrics` (creating the database and schema on first
run). It prints how many Dives it scanned, how many SQL statements it parsed and
skipped, and the row count per object type. Query the result:

```sql
SELECT object_type, object_name, dive_count, reference_count
FROM dive_metrics.main.dive_usage_metrics_latest
WHERE object_type = 'table'
ORDER BY dive_count DESC, reference_count DESC
LIMIT 20;
```

### Deploy as a Flight

Deploy with the MotherDuck MCP server rather than checked-in SQL. Call
`get_flight_guide` first for the exact tool arguments, then `create_flight` with:

- `source_code`: the contents of [`flight.py`](flight.py)
- `requirements_txt`: the contents of [`requirements.txt`](requirements.txt)
- `access_token_name`: a service account token that can read the Dives you want
  counted and write `TARGET_DATABASE` (list tokens with the `md_access_tokens()`
  table function); the runtime injects its value as `MOTHERDUCK_TOKEN`
- `config`: the keys from [What you'll adjust](#what-youll-adjust) you want to
  override (omit any you are keeping at default)

Create the Flight without a schedule, trigger one manual run with `run_flight`,
and read the run logs and the `_latest` view to confirm the metrics look right.
Then add a schedule by updating the Flight's `schedule_cron`. A daily run
(`0 6 * * *`) or weekly run (`0 6 * * 1`) is a reasonable cadence, since Dive
source SQL changes slowly. Schedule updates are metadata-only and do not create a
new Flight version.

## How it works

`flight.py` runs a fixed sequence; the config values only change its inputs:

1. Connect to MotherDuck (`md:`).
2. List Dives with `MD_LIST_DIVES(include_org_shares := true)` when
   `INCLUDE_ORG_SHARES` is set (otherwise `MD_LIST_DIVES()`), honoring `DIVE_LIMIT`.
3. For each Dive, call `MD_GET_DIVE(id => ?::UUID)` to get its React/JSX `content`.
4. Extract candidate SQL strings from that source: capture template-literal and
   quoted-string contents, neutralize JS `${...}` interpolations into a harmless
   placeholder literal, and keep only strings that start like SQL (`SELECT` /
   `WITH` / `FROM`).
5. Parse each candidate with `json_serialize_sql(?)` (the SQL is bound as a
   parameter), skip anything that does not parse, and walk the resulting JSON AST
   for `BASE_TABLE` nodes (database / schema / table) and `COLUMN_REF` nodes
   (column names).
6. Aggregate across all Dives: `dive_count` counts the distinct Dives referencing
   each object (an object referenced many times in one Dive counts once),
   `reference_count` is the raw total of occurrences.
7. Append one timestamped batch of rows to `METRICS_TABLE` and refresh the
   `<table>_latest` view (its database and schema are created on first run).

## Caveats

- **Columns are attributed by name only.** The AST does not always resolve which
  table a bare column belongs to, so columns are counted by name across all Dives.
  A column named `id` in two unrelated tables is counted as one `id`.
- **`SELECT *` is not expanded.** A `SELECT *` records the table reference but
  contributes no column rows, so column counts undercount wildcard-heavy Dives.
- **Interpolated or unparseable queries are skipped.** A statement that still
  fails `json_serialize_sql` after interpolation neutralization (heavily nested
  `${...}`, partial SQL fragments, dialect quirks) is skipped, and the run logs
  the skip count. A Dive whose source cannot be read is also counted as skipped.
- **Org-shares behavior.** Verified against live MotherDuck: `MD_LIST_DIVES`
  accepts a named `include_org_shares` argument
  (`MD_LIST_DIVES(include_org_shares := true)`), and its full signature is
  `MD_LIST_DIVES(offset, include_org_shares, limit)`. With the argument it returns
  every Dive shared in the organization; without it (or `MD_LIST_DIVES()`) it
  returns only the caller's own Dives. The Flight falls back to the no-argument
  call if a MotherDuck build does not support the argument. `MD_GET_DIVE` requires
  the id as a named UUID argument (`MD_GET_DIVE(id => ?::UUID)`) and accepts only a
  literal, so it is called once per Dive id rather than in a lateral join.
- **Metrics reflect Dive source SQL, not executed query frequency.** A table that
  appears in many Dives ranks high even if those Dives are rarely opened. For how
  often queries actually run, see `MD_INFORMATION_SCHEMA.QUERY_HISTORY`, but note
  that query history records executed SQL and cannot be attributed back to a
  specific Dive.

## Security

- **Identifier validation.** Config-supplied names (`TARGET_DATABASE`,
  `TARGET_SCHEMA`, `METRICS_TABLE`) are checked against `^[A-Za-z_][A-Za-z0-9_]*$`
  before any SQL runs, because they flow into `CREATE`/`INSERT` statements that
  cannot be parameterized.
- **Parameterized writes.** Every data value (object name, counts) and the Dive id
  passed to `MD_GET_DIVE`, and each candidate SQL string passed to
  `json_serialize_sql`, are bound as parameters, never string-formatted into SQL.
  Metric rows are written in one bulk multi-row `INSERT`.
- **Read-only against Dives.** The Flight only reads Dives (`MD_LIST_DIVES`,
  `MD_GET_DIVE`) and writes solely to `TARGET_DATABASE`. Scope the token
  accordingly: read access to the Dives you want counted, write access to the
  target database.

## Learn more

- Flight mechanics (creating, running, scheduling): use the MotherDuck MCP
  `get_flight_guide` tool.
- Dive functions (`MD_LIST_DIVES`, `MD_GET_DIVE`) and SQL parsing
  (`json_serialize_sql`): use the `ask_docs_question` MCP tool.
- Files in this template: [`flight.py`](flight.py) (the single-file Flight source)
  and [`requirements.txt`](requirements.txt) (its one dependency, `duckdb`).
