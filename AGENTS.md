# Agent Guide

This repository is an examples catalog for MotherDuck. Read
[.agents/skills/motherduck-examples/SKILL.md](.agents/skills/motherduck-examples/SKILL.md)
before changing README metadata, Flight Plans, the generated catalog workflow,
or the starter script.

## Current Direction

- README front matter is the source of truth for catalog metadata.
- The catalog is generated in CI and published as a GitHub Release asset. Do not
  commit a generated `catalog.json` unless the user explicitly changes that
  decision.
- `flight-plans/` is only for reusable, single-file Flight templates
  (`type: template`). It currently holds `flight-scheduled-s3-ingest`,
  `flight-dlt-ingest`, and `flight-provision-user-databases`.
- All concrete examples live at the repo root, including flight-capable ones that
  carry `features: [flights]` and a "Deploy as a Flight" section. Do not move them
  under `flight-plans/` or a new `examples/` folder.
- Flight Plan templates (`flight-plans/`) are non-deterministic agent plans:
  deploy them via README prose and the MotherDuck MCP Flight tools, not
  checked-in create-flight SQL.
- A concrete example that deploys as a Flight may ship a deploy script that calls
  the Flight SQL surface (`MD_CREATE_FLIGHT` / `MD_UPDATE_FLIGHT`, resolving by
  name via `MD_FLIGHTS()`) — the same shape as a Dive example's `deploy-dive.sh`.
- README bodies should read like compact agent skills: what to adjust, questions
  to ask, how to run, and where to progressively disclose more detail.

## Validation

Use `uv` for Python commands.

```bash
uv run scripts/build-catalog.py
uv run scripts/build-catalog.py --output .catalog-preview/catalog.json
uv run --with pytest --with pyyaml --with jsonschema python -m pytest tests/test_build_catalog.py tests/test_catalog_workflow.py -q
uv run --with check-jsonschema check-jsonschema --schemafile catalog.schema.json .catalog-preview/catalog.json
```

The root README documents the public authoring contract. Keep this file short;
put detailed agent workflow guidance in the skill linked above.
