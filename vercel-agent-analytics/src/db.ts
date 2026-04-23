import { DuckDBInstance, DuckDBConnection } from "@duckdb/node-api";

const TOKEN = process.env.MOTHERDUCK_TOKEN;
const DATABASE = process.env.MD_DATABASE ?? "dumky_share";
const SCHEMA = process.env.MD_SCHEMA ?? "raw";
const TABLE = process.env.MD_TABLE ?? "vercel_request_logs";

if (!TOKEN) {
  // Fail fast at cold start rather than on the first request.
  throw new Error("MOTHERDUCK_TOKEN is required");
}

// Fully qualified target, escaped to survive schema/table names with unusual
// characters. Built once; env vars are read at cold start.
const TARGET = [DATABASE, SCHEMA, TABLE].map(quoteIdent).join(".");

// Module-scoped so warm invocations reuse the MotherDuck connection.
// First invocation pays a ~500 ms-1 s cold start while the extension loads.
let connPromise: Promise<DuckDBConnection> | null = null;

export function getConnection(): Promise<DuckDBConnection> {
  if (!connPromise) {
    connPromise = (async () => {
      const instance = await DuckDBInstance.fromCache(`md:${DATABASE}`, {
        motherduck_token: TOKEN!,
      });
      return instance.connect();
    })().catch((err) => {
      connPromise = null;
      throw err;
    });
  }
  return connPromise;
}

export interface LogRow {
  event_id: string | null;
  received_at: Date;
  event_ts: Date;
  event_hour: Date;
  project_id: string | null;
  deployment_id: string | null;
  source: string | null;
  host: string | null;
  path: string | null;
  method: string | null;
  status_code: number | null;
  user_agent: string | null;
  referer: string | null;
  client_ip: string | null;
  region: string | null;
  request_id: string | null;
  ai_category: string | null;
  ai_name: string | null;
  raw: string;
}

// MotherDuck native tables do not yet support the DuckDB appender over the
// wire, so we build a single multi-row INSERT per invocation. Vercel log
// drains already batch ~100-1000 rows per POST, so one network round-trip
// per batch is fine.
//
// Column order here MUST match the CREATE TABLE in sql/01_setup.sql.
export async function insertRows(rows: LogRow[]): Promise<void> {
  if (rows.length === 0) return;
  const conn = await getConnection();

  const values = rows.map(
    (r) =>
      `(${[
        sqlStr(r.event_id),
        sqlTs(r.received_at),
        sqlTs(r.event_ts),
        sqlTs(r.event_hour),
        sqlStr(r.project_id),
        sqlStr(r.deployment_id),
        sqlStr(r.source),
        sqlStr(r.host),
        sqlStr(r.path),
        sqlStr(r.method),
        r.status_code === null ? "NULL" : String(r.status_code),
        sqlStr(r.user_agent),
        sqlStr(r.referer),
        sqlStr(r.client_ip),
        sqlStr(r.region),
        sqlStr(r.request_id),
        sqlStr(r.ai_category),
        sqlStr(r.ai_name),
        `${sqlStr(r.raw)}::JSON`,
      ].join(", ")})`
  );

  const stmt = `INSERT INTO ${TARGET} VALUES\n  ${values.join(",\n  ")}`;
  await conn.run(stmt);
}

function sqlStr(v: string | null): string {
  if (v === null) return "NULL";
  return `'${v.replace(/'/g, "''")}'`;
}

function sqlTs(d: Date): string {
  return `TIMESTAMP '${d.toISOString().replace("T", " ").replace("Z", "")}'`;
}

function quoteIdent(id: string): string {
  return `"${id.replace(/"/g, '""')}"`;
}
