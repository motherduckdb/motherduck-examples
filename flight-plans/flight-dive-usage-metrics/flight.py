import json
import os
import re

import duckdb


IDENTIFIER_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")

# A candidate string is treated as SQL only if it starts with one of these
# keywords (after stripping leading whitespace/comments). This keeps non-SQL
# template literals (CSS, JSX text, prop strings) out of the parser.
SQL_START_RE = re.compile(r"^\s*(--[^\n]*\n\s*)*(SELECT|WITH|FROM)\b", re.IGNORECASE)

# Capture the contents of backtick template literals and of single/double quoted
# strings. Template literals carry almost all dive SQL; quoted strings catch the
# occasional one-line query. The capture groups hold the inner text only.
TEMPLATE_LITERAL_RE = re.compile(r"`((?:\\.|[^`\\])*)`", re.DOTALL)
QUOTED_STRING_RE = re.compile(r"""(?<![A-Za-z0-9_])(?:"((?:\\.|[^"\\])*)"|'((?:\\.|[^'\\])*)')""")

# JS template interpolations (`${...}`) are not valid SQL. Replace each with a
# harmless placeholder literal so the surrounding SQL still parses. Nested braces
# inside an interpolation are rare in dive SQL, so a non-greedy single-level match
# is enough in practice; anything left unparseable is skipped and counted.
#
# An interpolation already wrapped in quotes (e.g. `'${orgId}'`) must become a
# single quoted literal, not a quoted-then-quoted-again string, so the
# quote-wrapped form is collapsed first; bare interpolations then become a quoted
# literal too.
QUOTED_INTERPOLATION_RE = re.compile(r"""(['"])\$\{[^{}]*\}\1""", re.DOTALL)
INTERPOLATION_RE = re.compile(r"\$\{[^{}]*\}", re.DOTALL)
INTERPOLATION_PLACEHOLDER = "'__param__'"

OBJECT_TYPES = ("database", "schema", "table", "column")

# SQL date/time (and a few other) keywords that the parser represents as
# COLUMN_REF nodes, e.g. CURRENT_DATE. They are not real columns, so they are
# skipped when collecting column references. A denylist (rather than an all-caps
# heuristic) avoids dropping legitimately upper-cased column names.
KEYWORD_COLUMNS = {
    "current_date", "current_time", "current_timestamp", "localtime",
    "localtimestamp", "now", "current_user", "session_user", "user", "excluded",
}

# Emit a progress line every this many dives so a long org-wide scan is not
# silent for minutes.
HEARTBEAT_EVERY = 200


def main() -> None:
    # Every knob is read from Flight config/env, so you adapt this template by
    # setting config values rather than editing code.
    target_database = validate_identifier("TARGET_DATABASE", env("TARGET_DATABASE", "dive_metrics"))
    target_schema = validate_identifier("TARGET_SCHEMA", env("TARGET_SCHEMA", "main"))
    metrics_table = validate_identifier("METRICS_TABLE", env("METRICS_TABLE", "dive_usage_metrics"))
    include_org_shares = env_bool("INCLUDE_ORG_SHARES", True)
    dive_limit = env_int("DIVE_LIMIT", 0)  # 0 means no limit

    metrics_fqn = f"{target_database}.{target_schema}.{metrics_table}"

    con = duckdb.connect("md:")

    dives = list_dives(con, include_org_shares=include_org_shares, dive_limit=dive_limit)
    scope = "own + org-shared" if include_org_shares else "own"
    print(f"scanning {len(dives)} dive(s) ({scope})")

    # Aggregates keyed by (object_type, object_name): one set of dive ids (for the
    # distinct dive_count) and a running total reference_count.
    dive_ids: dict[tuple[str, str], set[str]] = {}
    reference_counts: dict[tuple[str, str], int] = {}
    parsed = 0
    skipped = 0

    total = len(dives)
    for i, (dive_id, title) in enumerate(dives, start=1):
        try:
            source = get_dive_source(con, dive_id)
        except duckdb.Error as exc:
            skipped += 1
            print(f"skip dive {dive_id} ({title!r}): could not read source: {str(exc).splitlines()[0]}")
            continue

        candidates = extract_sql_candidates(source)
        # De-dupe object references per dive so a table referenced five times in
        # one dive counts once toward dive_count, while reference_count keeps the
        # raw total across all parsed statements.
        dive_objects: set[tuple[str, str]] = set()
        dive_parsed = 0
        for sql in candidates:
            ast = serialize_sql(con, sql)
            if ast is None:
                skipped += 1
                continue
            dive_parsed += 1
            for obj in objects_from_ast(ast):
                key = obj
                dive_objects.add(key)
                reference_counts[key] = reference_counts.get(key, 0) + 1

        parsed += dive_parsed
        for key in dive_objects:
            dive_ids.setdefault(key, set()).add(dive_id)

        # Heartbeat so a long org-wide scan reports progress instead of going
        # silent for minutes; always print the final dive too.
        if i % HEARTBEAT_EVERY == 0 or i == total:
            print(f"processed {i}/{total} dive(s); {parsed} statement(s) parsed, {skipped} skipped so far")

    print(f"parsed {parsed} SQL statement(s); skipped {skipped} (unreadable / unparseable / not SQL)")

    rows = [
        (object_type, object_name, len(ids), reference_counts.get((object_type, object_name), 0))
        for (object_type, object_name), ids in dive_ids.items()
    ]
    # Stable, readable order in the table: by type, then by descending dive_count.
    rows.sort(key=lambda r: (OBJECT_TYPES.index(r[0]), -r[2], r[1]))

    write_metrics(con, target_database, target_schema, metrics_fqn, rows)
    con.close()

    print(f"wrote {len(rows)} metric row(s) to {metrics_fqn}")
    for object_type in OBJECT_TYPES:
        n = sum(1 for r in rows if r[0] == object_type)
        print(f"  {object_type}: {n} distinct")


def list_dives(
    con: duckdb.DuckDBPyConnection, *, include_org_shares: bool, dive_limit: int
) -> list[tuple[str, str]]:
    # MD_LIST_DIVES accepts a named INCLUDE_ORG_SHARES argument (verified against
    # live MotherDuck): MD_LIST_DIVES(include_org_shares := true) returns every
    # dive shared in the organization, not just the caller's own. If a MotherDuck
    # build does not support the argument, fall back to the no-argument call so the
    # Flight still runs against the caller's own dives.
    limit_clause = f" LIMIT {int(dive_limit)}" if dive_limit and dive_limit > 0 else ""
    if include_org_shares:
        try:
            return con.execute(
                f"SELECT id::VARCHAR, title FROM MD_LIST_DIVES(include_org_shares := true)"
                f" ORDER BY title{limit_clause}"
            ).fetchall()
        except duckdb.Error as exc:
            print(f"INCLUDE_ORG_SHARES not supported by MD_LIST_DIVES ({str(exc).splitlines()[0]}); "
                  "falling back to the caller's own dives")
    return con.execute(
        f"SELECT id::VARCHAR, title FROM MD_LIST_DIVES() ORDER BY title{limit_clause}"
    ).fetchall()


def get_dive_source(con: duckdb.DuckDBPyConnection, dive_id: str) -> str:
    # MD_GET_DIVE is a table function that requires the id as a named UUID
    # argument and only accepts a literal (no lateral column reference), so it is
    # called once per dive id here. The dive's React/JSX source is in the `content`
    # column; bind the id and cast it to UUID inside SQL.
    row = con.execute(
        "SELECT content FROM MD_GET_DIVE(id => ?::UUID)", [dive_id]
    ).fetchone()
    return row[0] if row and row[0] else ""


def extract_sql_candidates(source: str) -> list[str]:
    """Pull candidate SQL strings out of a dive's JSX source.

    Dives embed SQL in template literals (the common case, e.g.
    ``useSQLQuery(`SELECT ...`)``) and occasionally in quoted strings. Capture
    both, neutralize JS ``${...}`` interpolations so the SQL parses, and keep only
    strings that look like SQL. Pure-Python and side-effect free so it is testable
    in isolation.
    """
    seen: set[str] = set()
    candidates: list[str] = []
    raw_strings: list[str] = []

    for match in TEMPLATE_LITERAL_RE.finditer(source):
        raw_strings.append(match.group(1))
    for match in QUOTED_STRING_RE.finditer(source):
        raw_strings.append(match.group(1) if match.group(1) is not None else match.group(2))

    for raw in raw_strings:
        sql = neutralize_interpolations(raw)
        if not SQL_START_RE.match(sql):
            continue
        normalized = sql.strip()
        if normalized in seen:
            continue
        seen.add(normalized)
        candidates.append(normalized)
    return candidates


def neutralize_interpolations(text: str) -> str:
    # Replace `${...}` with a harmless placeholder literal so an interpolated
    # template literal still parses as SQL. Collapse a quote-wrapped interpolation
    # (`'${...}'`) into one quoted literal first, so it does not become a doubled
    # quoted string, then handle bare interpolations. Heavily nested
    # interpolations may not be fully neutralized; those statements fail
    # json_serialize_sql and are skipped (and counted) rather than guessed at.
    text = QUOTED_INTERPOLATION_RE.sub(INTERPOLATION_PLACEHOLDER, text)
    return INTERPOLATION_RE.sub(INTERPOLATION_PLACEHOLDER, text)


def serialize_sql(con: duckdb.DuckDBPyConnection, sql: str) -> dict | None:
    # json_serialize_sql returns a JSON string AST. Bind the SQL as a parameter
    # (never string-formatted). On a parse error the top-level object carries
    # "error": true, so return None and let the caller count it as skipped.
    try:
        raw = con.execute("SELECT json_serialize_sql(?)", [sql]).fetchone()[0]
    except duckdb.Error:
        return None
    try:
        ast = json.loads(raw)
    except (TypeError, ValueError):
        return None
    if not isinstance(ast, dict) or ast.get("error"):
        return None
    return ast


def objects_from_ast(ast: dict) -> set[tuple[str, str]]:
    """Walk a json_serialize_sql AST and collect referenced objects.

    Returns a set of ``(object_type, object_name)`` pairs:

    - ``BASE_TABLE`` nodes carry ``catalog_name`` / ``schema_name`` /
      ``table_name``; each contributes a ``database`` (catalog), a ``schema``
      (catalog.schema), and a ``table`` (catalog.schema.table) entry, using only
      the parts that are present.
    - ``COLUMN_REF`` nodes carry a ``column_names`` array; the last element is the
      column name. Columns are attributed by NAME only (see README Caveats).

    Pure-Python so it is testable in isolation on a parsed AST.
    """
    objects: set[tuple[str, str]] = set()

    def walk(node: object) -> None:
        if isinstance(node, dict):
            node_type = node.get("type")
            node_class = node.get("class")
            if node_type == "BASE_TABLE":
                catalog = (node.get("catalog_name") or "").strip()
                schema = (node.get("schema_name") or "").strip()
                table = (node.get("table_name") or "").strip()
                if catalog:
                    objects.add(("database", catalog))
                if catalog and schema:
                    objects.add(("schema", f"{catalog}.{schema}"))
                # Only count fully-qualified tables (database.schema.table). A
                # bare or partially-qualified name is almost always a CTE or an
                # alias, which the parser also reports as a BASE_TABLE, so it is
                # skipped to keep the table metric clean.
                if catalog and schema and table:
                    objects.add(("table", f"{catalog}.{schema}.{table}"))
            if node_type == "COLUMN_REF" or node_class == "COLUMN_REF":
                names = node.get("column_names")
                if isinstance(names, list) and names:
                    column = str(names[-1]).strip()
                    # Skip "*" and SQL keywords the parser surfaces as columns
                    # (e.g. CURRENT_DATE) so the column metric reflects real columns.
                    if column and column != "*" and column.lower() not in KEYWORD_COLUMNS:
                        objects.add(("column", column))
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(ast)
    return objects


def write_metrics(
    con: duckdb.DuckDBPyConnection,
    target_database: str,
    target_schema: str,
    metrics_fqn: str,
    rows: list[tuple[str, str, int, int]],
) -> None:
    # The metrics live in a writable database, so create it (and its schema) on
    # first run. History-append model: every run stamps its rows with one run_at,
    # so trends over time stay visible. A `<table>_latest` view always exposes the
    # most recent run.
    con.execute(f"CREATE DATABASE IF NOT EXISTS {target_database}")
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {target_database}.{target_schema}")
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {metrics_fqn} (
            run_at TIMESTAMPTZ,
            object_type VARCHAR,
            object_name VARCHAR,
            dive_count INTEGER,
            reference_count INTEGER
        )
        """
    )

    if rows:
        # One bulk multi-row INSERT (not executemany / row-by-row) with bound
        # parameters. run_at uses now() so every row in a run shares one timestamp.
        row_sql = "(now(), ?, ?, ?, ?)"
        params: list = []
        for object_type, object_name, dive_count, reference_count in rows:
            params.extend([object_type, object_name, dive_count, reference_count])
        con.execute(
            f"INSERT INTO {metrics_fqn} "
            "(run_at, object_type, object_name, dive_count, reference_count) VALUES "
            + ", ".join([row_sql] * len(rows)),
            params,
        )

    # A latest-run view so consumers do not have to know the newest run_at.
    con.execute(
        f"""
        CREATE OR REPLACE VIEW {metrics_fqn}_latest AS
        SELECT * FROM {metrics_fqn}
        WHERE run_at = (SELECT max(run_at) FROM {metrics_fqn})
        """
    )


def env(name: str, default: str) -> str:
    value = os.environ.get(name, default).strip()
    return value or default


def env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def env_int(name: str, default: int) -> int:
    value = os.environ.get(name, "").strip()
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        raise ValueError(f"{name} must be an integer, got {value!r}")


def validate_identifier(name: str, value: str) -> str:
    # Config-supplied database, schema, and table names flow into SQL that cannot
    # be parameterized, so reject anything that is not a plain SQL identifier
    # before any SQL runs. All data values (object_name, counts) are bound as
    # parameters instead.
    if not IDENTIFIER_RE.fullmatch(value):
        raise ValueError(f"{name} must be a simple SQL identifier, got {value!r}")
    return value


if __name__ == "__main__":
    main()
