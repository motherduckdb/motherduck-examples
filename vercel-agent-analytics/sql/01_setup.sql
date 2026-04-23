-- Run this once in the MotherDuck SQL UI. Creates the schema + table the
-- Vercel Function writes to.
--
-- Collector env vars:
--   MD_DESTINATION=agent_analytics.raw
--   MD_TABLE=vercel_request_logs
-- If you override them, replace the database/schema/table identifiers below
-- before running this file.

USE agent_analytics;

CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.vercel_request_logs (
    event_id        VARCHAR,
    received_at     TIMESTAMP,    -- when the function saw the batch
    event_ts        TIMESTAMP,    -- timestamp from the Vercel payload
    event_hour      TIMESTAMP,    -- date_trunc('hour', event_ts), handy for pruning
    project_id      VARCHAR,
    deployment_id   VARCHAR,
    source          VARCHAR,      -- edge, lambda, build, static, ...
    host            VARCHAR,
    path            VARCHAR,
    method          VARCHAR,
    status_code     INTEGER,
    user_agent      VARCHAR,
    referer         VARCHAR,
    client_ip       VARCHAR,      -- IPv4 stored with the last octet zeroed
    region          VARCHAR,
    request_id      VARCHAR,
    ai_category     VARCHAR,      -- 'crawler' | 'agent' | 'human_via_ai' | NULL
    ai_name         VARCHAR,      -- matched pattern label, e.g. 'GPTBot'
    raw             JSON          -- full original payload, for replaying classification
);

-- Convenience view for AI-only traffic.
CREATE OR REPLACE VIEW raw.ai_requests AS
SELECT *
FROM raw.vercel_request_logs
WHERE ai_category IS NOT NULL;
