# Flight Plans catalog: minimal working product

**Status:** approved 2026-06-03. **Owner:** Dumky (DevRel).
**Source PRD:** "Authoring, managing, and deploying Flight Plans" (Notion).

## Goal

Re-architect `motherduck-examples` from a `meta.yml` + README catalog into a flat,
agent-crawlable catalog where each example README reads like an agent SKILL:
minimal frontmatter for discovery, plus a body that tells an agent (and a human)
exactly what to adapt and how to deploy. Templates become non-deterministic
"Flight Plans" the agent assembles and adapts, not byte-identical programs.

## Decisions

1. **Scope:** every example in the repo gets the rework (frontmatter + skill body).
2. **`meta.yml` is removed.** All catalog metadata moves to README frontmatter.
3. **`type` axis:** `example` (a concrete, end-to-end worked instance) or
   `template` (a generic, reusable plan the agent parameterizes). The
   flight-vs-dive distinction is carried by `features`, not `type`.
4. **One unified format for all READMEs.** Flight-specific sections appear only
   when `features` includes `flights`. Non-flight examples document their own
   runtime (Cloudflare, Vercel, `uv run`, etc.).
5. **`flight-plans/` directory** holds the flight-deployable plans. Non-flight
   examples stay at the repository top level.
6. **`create_flight.sql` is removed.** A plan folder keeps one or more code
   snippets plus optional extra markdown/artifacts that the README progressively
   discloses.
7. **Byte-sync is retired.** `check-flight-template-sync.py` is deleted; plans
   may diverge from the `dbt-runner` template.

## Repository layout

```
flight-plans/
  dbt-runner/            # type: template, features: [flights]
    README.md  flight.py  requirements.txt
  dbt-ingestion-s3/      # type: example,  features: [flights]
    README.md  flight.py  requirements.txt  dbt_project.yml  models/ ...
  dbt-churn-prediction/  # type: example,  features: [flights]
  dbt-ducklake/          # type: example,  features: [flights, ducklake]
<top-level example folders>   # cloudflare-workers, vercel-nextjs, nodejs-motherduck, ...
scripts/
  build-catalog.py       # validates front matter + builds catalog.json
  get-starter.sh         # resolves flight-plans/<id>
README.md                # catalog index + authoring spec
```

For each flight example: move `flights/flight.py` -> `flight.py`,
`flights/requirements.txt` -> `requirements.txt`, delete `flights/create_flight.sql`,
fold `flights/README.md` into the main README, remove the empty `flights/` folder.

## Frontmatter schema (exactly these keys)

```yaml
---
title: Build Hacker News models from S3 with dbt
id: dbt-ingestion-s3            # must equal the folder name
description: >-
  One or two sentences on what it does, then "Use when ..." routing guidance
  so an agent can pick it.
type: example                   # example | template
features: [flights]             # subset of: dives, ducklake, flights, pg_endpoint, wasm; [] if none
tags: [dbt, s3, parquet, hacker-news]   # lowercase slugs; tools, datasets, topics
---
```

Rules (enforced by `scripts/build-catalog.py`, which validates and builds the
catalog in one tool; running it with no `--output` validates only):
- All 6 keys present. `title`, `id`, `description` non-empty strings.
- `id` equals the containing folder name.
- `type` in {example, template}.
- `features` a list (may be empty); every value in
  {dives, ducklake, flights, pg_endpoint, wasm}.
- `tags` a list of lowercase slugs (no spaces, no uppercase). Reserved tags
  `motherduck` and `dbt-duckdb` are rejected (use a feature or a simpler tag).
- Any README whose frontmatter has `features` including `flights` must live
  under `flight-plans/`. Any README under `flight-plans/` must include `flights`
  in `features`.

## README body structure (the SKILL)

Keep it short and skimmable. Sections, in order:

1. `# <title>` + one paragraph: what it is and the MotherDuck pattern it shows.
2. `## What you'll adjust` — a table derived from **reading the code**. Each row:
   the knob, its purpose, and options/example value. These are the parts a user
   or agent changes to fit their own case (the old template `parameters`, the S3
   path, target database, model selection, schedule, etc.).
3. `## Questions to ask the user` — the inputs an agent should gather before
   adapting the plan (source table(s), incremental vs full, target database and
   schema, schedule). Always present; this is what makes the README agent-usable.
4. `## Run it` — exact local commands (`uv run ...`, `npm ...`, `wrangler deploy`,
   etc.) and a short prerequisites note when meaningful.
   - `### Deploy as a Flight` — **only when `features` includes `flights`.** How to
     deploy: create a Flight from `flight.py` + `requirements.txt`, set the knobs
     from "What you'll adjust" as Flight config/env, add an optional schedule,
     then run it. Use the MotherDuck MCP (`create_flight`, `run_flight`).
5. `## How it works / Learn more` — progressive disclosure. Link to extra
   in-folder snippets/markdown, and point to the MCP guides instead of
   duplicating them:
   - Flights runtime, scheduling, secrets: run the `get_flight_guide` MCP tool.
   - Dives: run the `get_dive_guide` MCP tool.
   - Deeper MotherDuck/DuckDB questions: `ask_docs_question` or the docs.

Do not duplicate the contents of the MCP guides in the README. Reference them.

## Tooling changes

- Validation is consolidated into `scripts/build-catalog.py` (the repo already had
  a WIP frontmatter->catalog builder). It walks every `README.md` with frontmatter,
  validates the rules above, and emits `catalog.json` with `--output`. The root
  `README.md` has no frontmatter and is skipped, as are dotfile/`node_modules` paths.
  `catalog.schema.json` is updated to the new minimal item shape (`type` instead of
  `kind`; no `categories`/`entrypoints`).
- Delete `scripts/check-meta.py` (superseded by `build-catalog.py`).
- Delete `scripts/check-flight-template-sync.py`.
- `scripts/get-starter.sh`: drop `templates` from `EXCLUDED_DIRS`; when a starter
  name is not a top-level dir, resolve it under `flight-plans/<name>` so the short
  name still works.
- Root `README.md`: remove the `meta.yml` documentation; add the catalog index,
  the frontmatter schema, the README body structure, the `type`/`features`/`tags`
  taxonomy, and the `uv run scripts/build-catalog.py` command.

## Implementation order

1. Structural moves (git mv, deletions) — done first, sequentially.
2. Fan out: one agent per example folder reads the code and writes the new
   README. `meta.yml` files stay in place during this pass so agents can reuse
   the existing metadata, then are deleted.
3. Tooling: update `build-catalog.py` + `catalog.schema.json`, update `get-starter.sh`, rewrite root
   README. Delete all `meta.yml`. Run the validator.
