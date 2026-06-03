---
title: Extract Structured Data From Reviews With dbt And prompt()
id: dbt-ai-prompt
description: >-
  A dbt-duckdb project that calls MotherDuck's prompt() AI function to turn
  unstructured product reviews into typed columns (sentiment, pros, cons,
  features), then aggregates them by product. Use when you want LLM extraction
  to run inside a dbt model as a normal SQL transformation, with no external
  API code.
type: example
features: []
tags: [dbt, ai, prompt, sentiment-analysis, reviews, unstructured-data]
---

# Extract Structured Data From Reviews With dbt And prompt()

This is a dbt-duckdb project that runs MotherDuck's `prompt()` AI function inside a model to extract structured fields from free-text product reviews. The pattern: a single SQL model passes review title and text to `prompt()` with a `struct` schema, MotherDuck returns typed columns (sentiment, pros, cons, product features, customer service signals), and downstream views aggregate those attributes per product. LLM extraction becomes just another dbt transformation, versioned and testable like any other model.

## What you'll adjust

| Setting | Purpose | Options / example |
| --- | --- | --- |
| `source('reviews', 'reviews_raw')` in `models/reviews/reviews_attributes.sql` | The input table of unstructured text to extract from | Point at your own reviews/feedback table |
| `database` / `schema` / `tables` in `models/reviews/_sources.yml` | Where the source data lives | Defaults to database `webshop-dbt-md-ai`, schema `main`, table `reviews_raw` |
| `limit 10` in `reviews_attributes.sql` | Caps how many rows are sent to `prompt()` (keeps cost and runtime small for a demo) | Raise or remove for a full run |
| The `prompt(...)` text in `reviews_attributes.sql` | The extraction instruction sent to the model, built from `title` and `text` columns | Swap in your own column names and task |
| `struct` and `struct_descr` in `reviews_attributes.sql` | The output schema and per-field descriptions that shape the returned JSON | Add/remove fields like `sentiment`, `pros:'VARCHAR[]'`, `mentions_price:'BOOLEAN'` |
| `+database: my_db` under `models.dbt_ai_prompt.reviews` in `dbt_project.yml` | Target database materialized models are written to | Change `my_db` to your MotherDuck database |
| `+materialized: table` (dir default) / per-model `config(materialized=...)` | Materialization: the extraction model is a `table`, aggregates are `view` | `table`, `view`, `incremental` |
| `path: 'md:'` and `target: dev` in `profiles.yml` | Connection target for dbt | `md:` for MotherDuck, or a local `.duckdb` file path |
| `MOTHERDUCK_TOKEN` env var | Read/write auth for MotherDuck | A token from your MotherDuck account |
| `accepted_values` / `not_null` tests in `models/reviews/schema.yml` | Validate the extracted output (e.g. sentiment in positive/neutral/negative) | Adjust to your fields |

## Questions to ask the user

- What is the source table of unstructured text, and which columns hold the text to extract from?
- Which target MotherDuck database and schema should the models write to?
- What structured fields do they want out, and what are the allowed values / types for each?
- Should this run on a sample (the `limit 10` demo) or the full table?
- Do they have a `MOTHERDUCK_TOKEN` with read/write access to the target database?
- Is the source database already created and populated in MotherDuck?

## Run it

Prerequisites: a MotherDuck account and a read/write `MOTHERDUCK_TOKEN` set in your environment, plus access to the source database.

```sh
uv venv --python 3.13
uv pip install dbt-duckdb duckdb==1.4.3
source .venv/bin/activate

export MOTHERDUCK_TOKEN="your_token_here"

dbt run
dbt test
dbt show --select reviews_attributes_by_product
```

`dbt run` builds `reviews_attributes` (the `prompt()` extraction, as a table) plus the two aggregate views; `dbt test` runs the `not_null` and `accepted_values` checks in `schema.yml`.

## How it works / Learn more

- `models/reviews/reviews_attributes.sql`: the core model. Calls `prompt()` with a `struct` (output schema) and `struct_descr` (field descriptions) so MotherDuck returns typed columns instead of raw text. Read this first.
- `models/reviews/reviews_attributes_by_product.sql`: unnests the array fields and aggregates distinct values per `parent_asin`.
- `models/reviews/reviews_attributes_sentiment_by_product.sql`: counts sentiment per product and computes normalized sentiment scores.
- For `prompt()` syntax, the `struct` extraction pattern, and other MotherDuck AI functions, run the `ask_docs_question` MCP tool or see the MotherDuck docs.
