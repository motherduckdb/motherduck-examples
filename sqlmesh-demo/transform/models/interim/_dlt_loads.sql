MODEL (
  name interim.full__dlt_loads,
  kind FULL,
  cron '@daily'
);

SELECT
  load_id::TEXT AS load_id,
  schema_name::TEXT AS schema_name,
  status::BIGINT AS status,
  inserted_at::TIMESTAMP AS inserted_at,
  schema_version_hash::TEXT AS schema_version_hash,
  TO_TIMESTAMP(load_id::DOUBLE) AS _dlt_load_time
FROM stock_data._dlt_loads