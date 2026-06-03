# Agent Guide

This repository is an examples catalog for MotherDuck. Read
[.agents/skills/motherduck-examples/SKILL.md](.agents/skills/motherduck-examples/SKILL.md)
before changing README metadata, Flight Plans, the generated catalog workflow,
or the starter script.

## Current Direction

- README front matter is the source of truth for catalog metadata.
- The catalog is generated in CI and deployed to GitHub Pages. Do not commit a
  generated `catalog.json` unless the user explicitly changes that decision.
- Flight-capable examples live under `flight-plans/` and carry
  `features: [flights]`.
- Top-level folders remain normal examples. Do not move them under a new
  `examples/` folder.
- Flight Plans are non-deterministic agent plans. Deployment guidance belongs in
  README prose and MotherDuck MCP tool usage, not checked-in create-flight SQL.
- README bodies should read like compact agent skills: what to adjust, questions
  to ask, how to run, and where to progressively disclose more detail.

## Validation

Use `uv` for Python commands.

```bash
uv run scripts/build-catalog.py
uv run scripts/build-catalog.py --output .catalog-pages/catalog.json
uv run --with pytest --with pyyaml --with jsonschema python -m pytest tests/test_build_catalog.py -q
uv run --with check-jsonschema check-jsonschema --schemafile catalog.schema.json .catalog-pages/catalog.json
```

The root README documents the public authoring contract. Keep this file short;
put detailed agent workflow guidance in the skill linked above.
