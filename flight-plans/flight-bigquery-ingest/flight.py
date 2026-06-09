import datetime
import json
import logging
import os
import re
import tempfile
import time

import duckdb


# ===========================================================================
# USER-EDIT BLOCKS
# ===========================================================================
# Everything in this section is what you change to point the template at your
# own BigQuery query and MotherDuck destination. The generic engine below is
# meant to stay untouched.

# ---- Destination in MotherDuck -------------------------------------------
# Where the query result lands. Each of these is validated as a plain SQL
# identifier before any SQL runs (see validate_identifier), so use simple
# names without dots or quoting.
DEFAULT_DB = "bigquery_ingest"
SCHEMA = "main"
TABLE = "events"

# ---- Watermark / partitioning --------------------------------------------
# Column in QUERY's result that holds the partition date. It drives the
# watermark: each run loads days after the destination's current MAX of this
# column. It must be a DATE (or castable to DATE) and must exist in the result.
PARTITION_DATE_COLUMN = "event_date"

# First day to load when the destination table is empty (cold start). A
# datetime.date so it is type-checked, not a loose string.
COLD_START_DT = datetime.date(2024, 1, 1)

# Watermark-mode re-load window, in days. Each watermark run re-loads the last
# OVERLAP_DAYS partitions as well as the new one, so late-arriving rows in
# already-loaded partitions get healed. 0 = forward-only (never re-load a day).
OVERLAP_DAYS = 3

# ---- The BigQuery query ---------------------------------------------------
# A GoogleSQL string run against BigQuery through the `bigquery` community
# extension. It MUST:
#   - return a column named exactly PARTITION_DATE_COLUMN, and
#   - filter on the partition window using the {start_dt} / {end_dt}
#     placeholders so BigQuery prunes partitions and only scans the days being
#     loaded (this is what keeps the Jobs API bytes-billed cost down).
# Add any other {placeholders} you need and return their values from
# build_filters(). Placeholder values are substituted with str.format(); they
# are values you control here, never end-user input.
#
# Quoting note: GoogleSQL string literals here use double quotes ("..."). The
# DuckDB-side MD_DELETE_PREDICATE below uses single quotes ('...') for the same
# literals. Keep that distinction in mind when you mirror predicates.
QUERY = """
SELECT
  DATE(event_timestamp) AS event_date,
  user_id,
  event_name,
  event_timestamp
FROM `my_project.analytics.events`
WHERE DATE(event_timestamp) >= DATE("{start_dt}")
  AND DATE(event_timestamp) <  DATE("{end_dt}")
  AND event_source = "{event_source}"
"""

# ---- Idempotent re-load predicate ----------------------------------------
# A DuckDB WHERE clause (NO "WHERE" keyword) that selects exactly the rows a
# single partition's QUERY run would produce. Before each per-day INSERT, the
# engine DELETEs rows matching this predicate, so re-running a day replaces it
# instead of duplicating it. It MUST mirror QUERY's predicates: if QUERY
# filters on event_source but the DELETE does not, the DELETE clears rows you
# are not re-loading. Same {start_dt}/{end_dt}/{placeholders} as QUERY.
#
# Quoting note: this runs in DuckDB, so string literals use single quotes,
# unlike the double quotes in the GoogleSQL QUERY above.
MD_DELETE_PREDICATE = """
event_date >= DATE '{start_dt}'
AND event_date < DATE '{end_dt}'
AND event_source = '{event_source}'
"""


def build_filters(start_dt: datetime.date, end_dt: datetime.date) -> dict[str, str]:
    """Return the substitution values for QUERY and MD_DELETE_PREDICATE.

    start_dt and end_dt bound a single half-open partition window
    [start_dt, end_dt). Any value a run should be able to override (here
    EVENT_SOURCE) is read from os.environ with a default, so you can re-point
    the Flight via config without editing code. These values are formatted into
    SQL strings, so keep them to values you control, not untrusted input.
    """
    return {
        "start_dt": start_dt.isoformat(),
        "end_dt": end_dt.isoformat(),
        "event_source": env("EVENT_SOURCE", "web"),
    }


# ===========================================================================
# GENERIC ENGINE (leave this alone)
# ===========================================================================

IDENTIFIER_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")

# The community-extension name and the secret param that carries the GCP
# service-account JSON. The deployed secret injects this param prefixed with
# the (lowercased) secret name, e.g. `gcp_creds_GOOGLE_APPLICATION_CREDENTIALS_JSON`.
CREDS_JSON_ENV = "GOOGLE_APPLICATION_CREDENTIALS_JSON"
CREDS_PATH_ENV = "GOOGLE_APPLICATION_CREDENTIALS"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("flight-bigquery-ingest")


def main() -> None:
    # Validate the destination identifiers up front: they flow into SQL that
    # cannot be parameterized, so reject anything that is not a plain identifier
    # before any SQL runs.
    db = validate_identifier("BIGQUERY_DEST_DB", env("BIGQUERY_DEST_DB", DEFAULT_DB))
    schema = validate_identifier("SCHEMA", SCHEMA)
    table = validate_identifier("TABLE", TABLE)
    partition_col = validate_identifier("PARTITION_DATE_COLUMN", PARTITION_DATE_COLUMN)

    # GCP_PROJECT_ID is the billing project for bigquery_query (the project that
    # is charged for the scan, not necessarily where the data lives). Validate it
    # as an identifier because it is bound as a parameter to bigquery_query but
    # also logged and used to shape the connection; GCP project IDs are
    # lowercase letters, digits, and hyphens, so allow hyphens here.
    billing_project = require_project_id(env("GCP_PROJECT_ID", ""))

    if OVERLAP_DAYS < 0:
        raise ValueError(f"OVERLAP_DAYS must be >= 0, got {OVERLAP_DAYS}")

    _materialize_gcp_creds()
    con = _connect_duckdb()
    try:
        destination = f"{ident(db)}.{ident(schema)}.{ident(table)}"
        _ensure_destination(con, db, schema, table, billing_project)

        start, end = resolve_window(con, destination, partition_col)
        if start > end:
            log.info("nothing to load (resolved start %s is after end %s)", start, end)
            return

        days = [start + datetime.timedelta(days=offset) for offset in range((end - start).days + 1)]
        log.info(
            "loading %d partition(s): %s .. %s into %s.%s.%s",
            len(days), days[0].isoformat(), days[-1].isoformat(), db, schema, table,
        )

        for day in days:
            load_partition(con, destination, partition_col, billing_project, day)
    finally:
        con.close()
    log.info("done")


def resolve_window(
    con: duckdb.DuckDBPyConnection, destination: str, partition_col: str
) -> tuple[datetime.date, datetime.date]:
    """Pick the inclusive [start, end] partition window for this run.

    Priority, highest first:
      1. start_dt + end_dt env vars: backfill that explicit inclusive range.
      2. target_dt env var: just that one day.
      3. watermark mode: prev_max = MAX(partition column) in the destination;
         load [prev_max + 1 - OVERLAP_DAYS, prev_max + 1]. When the destination
         is empty, start from COLD_START_DT.
    Dates are parsed as plain ISO YYYY-MM-DD values, not interpolated into SQL.
    """
    start_env = env("start_dt", "")
    end_env = env("end_dt", "")
    if start_env and end_env:
        start = parse_date("start_dt", start_env)
        end = parse_date("end_dt", end_env)
        log.info("explicit backfill range %s .. %s", start.isoformat(), end.isoformat())
        return start, end
    if start_env or end_env:
        raise ValueError("set both start_dt and end_dt for a backfill, or neither")

    target_env = env("target_dt", "")
    if target_env:
        target = parse_date("target_dt", target_env)
        log.info("explicit single day %s", target.isoformat())
        return target, target

    # Watermark mode. The partition column name is a validated identifier; the
    # destination is a quoted-identifier FQN. No data values go into this SQL.
    prev_max = con.execute(
        f"SELECT max({ident(partition_col)}) FROM {destination}"
    ).fetchone()[0]

    if prev_max is None:
        log.info("destination empty; cold start at %s", COLD_START_DT.isoformat())
        return COLD_START_DT, COLD_START_DT

    prev_max = to_date(prev_max)
    end = prev_max + datetime.timedelta(days=1)
    start = end - datetime.timedelta(days=OVERLAP_DAYS)
    log.info(
        "watermark: prev_max=%s, overlap=%d -> %s .. %s",
        prev_max.isoformat(), OVERLAP_DAYS, start.isoformat(), end.isoformat(),
    )
    return start, end


def load_partition(
    con: duckdb.DuckDBPyConnection,
    destination: str,
    partition_col: str,
    billing_project: str,
    day: datetime.date,
) -> None:
    """Idempotently (re)load one day inside a single transaction.

    DELETE the day's rows then INSERT the day's QUERY result. Wrapped in
    BEGIN/COMMIT with a rollback on error so a failed day leaves the
    destination unchanged (no half-loaded partition). Each partition is its own
    transaction, so an earlier day staying committed does not block a retry of a
    later one.
    """
    next_day = day + datetime.timedelta(days=1)
    filters = build_filters(day, next_day)
    delete_sql = f"DELETE FROM {destination} WHERE {MD_DELETE_PREDICATE.format(**filters)}"
    query_sql = QUERY.format(**filters)

    con.execute("BEGIN TRANSACTION")
    try:
        t0 = time.perf_counter()
        deleted = con.execute(delete_sql).fetchone()
        t1 = time.perf_counter()
        # bigquery_query runs the GoogleSQL through the Jobs API; the BigQuery
        # scan, the network transfer, and the MotherDuck insert are billed and
        # timed together here as the "insert" phase. The DELETE phase is timed
        # separately above. billing_project is bound as a parameter.
        con.execute(
            f"INSERT INTO {destination} SELECT * FROM bigquery_query(?, ?)",
            [billing_project, query_sql],
        )
        t2 = time.perf_counter()
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        log.error("partition %s failed; rolled back", day.isoformat())
        raise

    deleted_rows = deleted[0] if deleted and deleted[0] is not None else 0
    log.info(
        "partition %s: delete %d row(s) in %.2fs, bq scan + md insert in %.2fs",
        day.isoformat(), deleted_rows, t1 - t0, t2 - t1,
    )


def _materialize_gcp_creds() -> None:
    """Make a GCP service-account credential file available to the extension.

    The `bigquery` extension authenticates via Application Default Credentials,
    which it reads from a file path in GOOGLE_APPLICATION_CREDENTIALS. There are
    two ways the credential reaches this process:

      - Local run: you point GOOGLE_APPLICATION_CREDENTIALS at an SA JSON file on
        disk. If that file path is already set, this is a no-op.
      - Deployed Flight: the SA JSON is a secret, so it must NOT travel in
        plaintext Flight config. It is stored as a MotherDuck `TYPE flights`
        secret with a GOOGLE_APPLICATION_CREDENTIALS_JSON param, which the
        runtime injects as the env var `<secret_name>_GOOGLE_APPLICATION_CREDENTIALS_JSON`.
        We read that JSON blob, validate it parses, write it to a private temp
        file, and set GOOGLE_APPLICATION_CREDENTIALS to that path.
    """
    existing_path = os.environ.get(CREDS_PATH_ENV, "").strip()
    if existing_path:
        log.info("using GOOGLE_APPLICATION_CREDENTIALS file already on disk")
        return

    blob = resolve_creds_json()
    if not blob:
        raise RuntimeError(
            "no GCP credentials found: set GOOGLE_APPLICATION_CREDENTIALS to an SA "
            "JSON file path (local), or provide the SA JSON through a TYPE flights "
            f"secret with a {CREDS_JSON_ENV} param (deployed)."
        )
    try:
        json.loads(blob)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{CREDS_JSON_ENV} is not valid JSON: {exc}") from exc

    # delete=False so the file survives the with-block for the extension to read.
    # It lives in the Flight's ephemeral runtime and is discarded with it.
    handle = tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", prefix="gcp_sa_", delete=False
    )
    with handle:
        handle.write(blob)
    os.environ[CREDS_PATH_ENV] = handle.name
    log.info("materialized SA JSON from secret to a temp credentials file")


def resolve_creds_json() -> str:
    """Find the SA JSON blob in the environment.

    Mirrors flight-freshness-alert's resolve_webhook(): a local run sets the
    bare GOOGLE_APPLICATION_CREDENTIALS_JSON; deployed, a TYPE flights secret
    injects it under `<secret_name>_GOOGLE_APPLICATION_CREDENTIALS_JSON`, where
    the secret name (whatever you call it) becomes a lowercased prefix. Accept
    the exact name first, then any env var ending in the suffix, so the secret
    name does not matter.
    """
    direct = os.environ.get(CREDS_JSON_ENV, "").strip()
    if direct:
        return direct
    suffix = f"_{CREDS_JSON_ENV}"
    for key, value in os.environ.items():
        if key.endswith(suffix) and value.strip():
            return value.strip()
    return ""


def _connect_duckdb() -> duckdb.DuckDBPyConnection:
    # Community extensions must be enabled at connect time, so the flag goes in
    # the connect config, not a later SET. INSTALL ... FROM community pulls the
    # bigquery extension; LOAD motherduck + ATTACH 'md:' gives the in-memory
    # connection access to MotherDuck. preserve_insertion_order = FALSE lets the
    # engine stream large result sets without buffering to keep ordering.
    con = duckdb.connect(":memory:", config={"allow_community_extensions": True})
    con.execute("INSTALL bigquery FROM community")
    con.execute("LOAD bigquery")
    con.execute("LOAD motherduck")
    con.execute("ATTACH 'md:'")
    con.execute("SET preserve_insertion_order = FALSE")
    return con


def _ensure_destination(
    con: duckdb.DuckDBPyConnection,
    db: str,
    schema: str,
    table: str,
    billing_project: str,
) -> None:
    """Create the destination database/schema/table if they do not exist.

    The table schema is bootstrapped from QUERY itself by running it over a
    1970-01-01 .. 1970-01-02 window. BigQuery prunes the partitioned scan to
    (effectively) nothing for that ancient range, so the probe is nearly free
    but still returns the exact result columns and types. LIMIT 0 means we only
    take the schema, not rows.
    """
    con.execute(f"CREATE DATABASE IF NOT EXISTS {ident(db)}")
    con.execute(f"USE {ident(db)}")
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {ident(db)}.{ident(schema)}")

    probe_filters = build_filters(datetime.date(1970, 1, 1), datetime.date(1970, 1, 2))
    probe_sql = QUERY.format(**probe_filters)
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {ident(db)}.{ident(schema)}.{ident(table)} AS "
        "SELECT * FROM bigquery_query(?, ?) LIMIT 0",
        [billing_project, probe_sql],
    )


# ---- helpers (shared idioms with the other flight-plans templates) --------

def env(name: str, default: str) -> str:
    value = os.environ.get(name, default).strip()
    return value or default


def env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def validate_identifier(name: str, value: str) -> str:
    if not IDENTIFIER_RE.fullmatch(value):
        raise ValueError(f"{name} must be a simple SQL identifier, got {value!r}")
    return value


def require_project_id(value: str) -> str:
    # GCP_PROJECT_ID is the billing project for bigquery_query. It is bound as a
    # parameter, but validate it anyway so a missing/malformed value fails early
    # with a clear message. GCP project IDs are 6-30 chars of lowercase letters,
    # digits, and hyphens, starting with a letter.
    value = value.strip()
    if not value:
        raise ValueError(
            "GCP_PROJECT_ID is required (the GCP billing project for bigquery_query)"
        )
    if not re.fullmatch(r"[a-z][a-z0-9-]{4,28}[a-z0-9]", value):
        raise ValueError(f"GCP_PROJECT_ID does not look like a GCP project id: {value!r}")
    return value


def ident(value: str) -> str:
    # Quote a dynamic SQL identifier by escaping embedded double quotes.
    return '"' + value.replace('"', '""') + '"'


def parse_date(name: str, value: str) -> datetime.date:
    try:
        return datetime.date.fromisoformat(value.strip())
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO date (YYYY-MM-DD), got {value!r}") from exc


def to_date(value: object) -> datetime.date:
    # MAX(partition column) comes back as a date or datetime depending on the
    # column type; normalize to a date for window arithmetic.
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, datetime.date):
        return value
    return datetime.date.fromisoformat(str(value)[:10])


if __name__ == "__main__":
    main()
