# nba-box-scores — flight (ingest pipeline)

Python ingest pipeline for `nba_box_scores_v3`, deployed as MotherDuck Flights.
Replaces the legacy TypeScript pipeline + GitHub Actions cron at
[`matsonj/nba-box-scores`](https://github.com/matsonj/nba-box-scores). The
companion Dive (frontend) lives in [`../dive`](../dive).

`nba_nightly` (cron `0 16 * * *` UTC) ingests the current season's box scores
into the production table set; `nba_backfill` is on-demand for historical
season ranges.

## Layout

```
nba-box-scores/flight/
├── pyproject.toml                # uv / hatchling
├── src/nba_box_scores_pipeline/  # importable package, used by every Flight
└── flights/                      # one folder per Flight
    ├── nba_nightly/              # scheduled (0 16 * * * UTC)
    └── nba_backfill/             # on-demand (no schedule_cron)
```

## Develop locally

```bash
cd nba-box-scores/flight
uv venv
uv pip install -e ".[dev]"

export MOTHERDUCK_TOKEN=<your token with v3 read+write>
python flights/nba_nightly/main.py
```

`NBA_INGEST_TABLE_SUFFIX` defaults to `""` (production); set it (e.g. `_new`)
to write an isolated sandbox table set for validation. In the Flight runtime,
`MOTHERDUCK_TOKEN` is injected from the token named `dives-loader-nba`.

## Deploy

Each Flight's registered `source_code` is the thin bootstrapper in
`flights/<name>/main.py`: it clones this repo at `NBA_FLIGHT_REPO_BRANCH`
(default `main`), `uv sync`s the package, and runs the entrypoint from the
synced venv. Pushing to the branch updates what the next run executes — no
re-registration needed. Register/update via the MotherDuck MCP
`create_flight` / `update_flight` tools, pointing `md_token_name` at
`dives-loader-nba`.
