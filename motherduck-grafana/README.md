---
title: Visualize MotherDuck Data in Grafana
id: motherduck-grafana
description: >-
  Runs a local Grafana instance in Docker with the MotherDuck DuckDB datasource
  plugin auto-provisioned, plus example dashboards that query MotherDuck with
  SQL. Use when you want to build Grafana dashboards or time-series panels on top
  of MotherDuck data.
type: example
features: []
tags: [grafana, docker, dashboards, time-series]
---

# Visualize MotherDuck Data in Grafana

A local Grafana setup that connects to MotherDuck through the `motherduck-duckdb-datasource` plugin. The `setup.sh` script downloads the latest plugin release, starts a Grafana container, and mounts a `provisioning/` directory so the datasource and example dashboards are configured on boot. It shows the MotherDuck pattern of using a read-scaling token as a Grafana datasource credential and running DuckDB SQL directly in panel queries (with results formatted as time series).

## What you'll adjust

| Setting | Purpose | Options / example |
| --- | --- | --- |
| `motherduck_token` env var | MotherDuck auth token passed to Grafana and the datasource (`setup.sh` requires it; injected as `$__env{motherduck_token}`) | `export motherduck_token=<your-token>` |
| `provisioning/datasources/sample_data.yaml` `initSql` | DuckDB SQL run when the datasource connects, used to attach databases | `ATTACH IF NOT EXISTS 'md:sample_data'`, or `ATTACH IF NOT EXISTS 'md:my_db'` |
| `provisioning/datasources/sample_data.yaml` `name` / `isDefault` | Datasource display name and default flag panels reference | `MotherDuck-Sample-Data`, `isDefault: true` |
| Panel `rawSql` in `provisioning/dashboards/json/*.json` | The DuckDB query each dashboard panel runs | `SELECT created_date, COUNT(status) FROM sample_data.nyc.service_requests GROUP BY created_date LIMIT 50` |
| `provisioning/dashboards/json/*.json` | Dashboard definitions auto-loaded from this folder | `nyc_services.json`, `nyc_rideshare.json`, or your own exported dashboard |
| `provisioning/dashboards/dashboards.yaml` `path` | Where Grafana looks for dashboard JSON inside the container | `/etc/grafana/provisioning/dashboards/json` |
| Grafana port mapping in `setup.sh` | Host port Grafana is published on | `-p 3000:3000` |
| Grafana image tag in `setup.sh` | Grafana version that runs in the container | `grafana/grafana:latest-ubuntu` |

## Questions to ask the user

- Which MotherDuck database(s) and schema(s) should be attached (the `initSql` `ATTACH` target)?
- Which tables or queries power the dashboards, and what is the time column for time-series panels?
- Is a MotherDuck access token available, and should it be a read-scaling token?
- Should this stay a local Docker setup, or be adapted for a hosted Grafana deployment?
- Are there existing dashboard JSON exports to drop into `provisioning/dashboards/json/`?

## Run it

Prerequisites: Docker installed and running, plus a MotherDuck access token.

```bash
export motherduck_token=<your-motherduck-token>
cd motherduck-grafana
./setup.sh
```

Then open Grafana at `http://localhost:3000` (default login `admin` / `admin`). The MotherDuck datasource and the example NYC dashboards are already provisioned. When building a new panel, set the format to time series and switch the query editor from builder to code to enter DuckDB SQL manually, for example:

```sql
SELECT created_date, COUNT(status)
FROM sample_data.nyc.service_requests
GROUP BY created_date LIMIT 1000
```

To add your own dashboard, export it from the Grafana UI (or copy the JSON model from dashboard settings), save it as `provisioning/dashboards/json/<name>.json`, then commit it.

## How it works / Learn more

- `setup.sh`: pulls the latest `motherduck-duckdb-datasource` release from GitHub, unzips it into `plugins/`, mounts it plus `provisioning/` into the Grafana container, and allows the unsigned plugin via `GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS`. It also stops/removes any prior Grafana container on port 3000 and Windows is not supported (unzip the plugin manually).
- `provisioning/datasources/sample_data.yaml`: defines the datasource, attaches `md:sample_data` on connect, and reads the token from the `motherduck_token` environment variable.
- `provisioning/dashboards/`: `dashboards.yaml` registers the file provider; `json/` holds the dashboard definitions Grafana loads on boot.
- `image.png`: screenshot showing where to set the time-series format and the code-mode query editor.
- For DuckDB SQL syntax, attaching databases, or read-scaling tokens, use the `ask_docs_question` MCP tool or the MotherDuck docs.
