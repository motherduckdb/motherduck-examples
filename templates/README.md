# MotherDuck templates

Reusable templates for MotherDuck examples. Templates are building blocks for
product surfaces such as the Flight UI and docs-generated cookbook pages.

Templates are not standalone starter projects. If you want a runnable example,
use one of the recipe folders at the repository root.

## Structure

```text
templates/
├── flights/   # Python programs that can be selected as Flight templates
└── dives/     # Dive app templates
```

Each template has a `meta.yml` file that describes how it should be presented
and which parameters a user or UI needs to collect before running it.

## Metadata

Template metadata starts with these fields:

```yaml
metadata_version: 1
id: dbt-runner
kind: flight_template
title: dbt Runner
description: Run a dbt project from a Flight.
features:
  - flights
categories:
  - transformation
tags:
  - dbt
```

Recipe folders use the same `meta.yml` filename with `kind: recipe`.
Use `features` for MotherDuck UI surfaces, `categories` for the fixed cookbook
taxonomy, and `tags` for third-party tools or datasets.
