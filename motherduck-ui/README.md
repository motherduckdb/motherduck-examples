---
title: Clean and Analyze a CSV in the MotherDuck UI
id: motherduck-ui
description: >-
  A step-by-step SQL walkthrough that loads a CSV, profiles it with SUMMARIZE,
  cleans messy columns, and answers an analysis question, run one query at a time
  in the MotherDuck web UI. Use when you want a hands-on intro to interactive data
  exploration and ad hoc cleaning in MotherDuck without writing application code.
type: example
features: []
tags: [csv, sql, exploratory-data-analysis, data-cleaning, wine]
---

# Clean and Analyze a CSV in the MotherDuck UI

This example is a guided SQL session you run query-by-query in the MotherDuck web UI. It shows the typical interactive flow: load a CSV into a table, profile it with `SUMMARIZE`, iteratively clean columns (drop bad rows, parse unit sizes into quantity and volume, normalize a dollar-formatted price), then answer an analysis question. It demonstrates the MotherDuck pattern of exploring and reshaping raw data interactively before promoting it to a clean, reusable table.

## What you'll adjust

| Setting | Purpose | Options / example |
| --- | --- | --- |
| `winelist_sample.csv` | Source file loaded by `read_csv_auto([...])` | Swap for your own CSV path; the UI also lets you drag-drop a file or read from S3/HTTPS |
| `winelist` table name | Target table created by the `CREATE OR REPLACE TABLE` step | Rename to your dataset, e.g. `orders`, `sales_raw` |
| Database / schema | Where the table lands; defaults to `my_db.main` in the UI | Pick the database with the UI database selector or qualify as `db.schema.table` |
| `"Unit size"` parsing | `substr` + `instr` logic that splits a value like `6x75cl` into `qty` and `volume_cl` | Adjust the delimiter (`x`) and the trailing-unit length (currently `cl`, 2 chars) for your format |
| `"Offer price"` cleanup | `replace(...)` strips `$` and `,` before casting to `decimal(10,2)` | Change the symbols stripped, the precision/scale, and the source column name |
| `WHERE vintage > 1000` | Filter that removes obvious bad rows | Replace with your own validity filter, or drop it |
| Exercise thresholds | The analysis query filters `Vintage >= 1990` and ranks on `coalesce(coalesce("WA score","Vinous score"),-1)` and `price_per_bottle` | Change the year cutoff, scoring columns, and ranking metric for your question |

## Questions to ask the user

- What CSV (or other source) are you loading, and where does it live (local upload, S3, HTTPS)?
- Which database and schema should the resulting table live in?
- Which columns need cleaning, and what are their real formats (delimiters, currency symbols, units)?
- Do you want to keep the cleaned result as a new table (`CREATE TABLE ... AS`) or just explore?
- What analysis question are you trying to answer, and which columns drive the ranking or aggregation?

## Run it

Prerequisites: a MotherDuck account. Open [app.motherduck.com](https://app.motherduck.com), sign in, and use the SQL editor.

1. Upload `winelist_sample.csv` (or your own CSV) via the UI file picker, or reference it from S3/HTTPS.
2. Open `script.sql` and run it one statement at a time, top to bottom. Read the output of each query before moving on, that is the point of the walkthrough.
3. The last block is the exercise: it computes the price-per-bottle difference between the highest-rated/least-expensive and lowest-rated/most-expensive bottles from vintage 1990 or later. Note that the exercise query reads from a cleaned `winelist_clean` table, so first persist your cleaned `SELECT` as `CREATE OR REPLACE TABLE winelist_clean AS ...` before running it.

You can also run the script from the DuckDB or MotherDuck CLI, but it is written as a UI walkthrough and is best experienced there.

## How it works / Learn more

- `script.sql` holds the full sequence of statements with inline comments explaining each cleaning step.
- The exercise prompt and the `coalesce`/`price_per_bottle` hints are described at the bottom of this README's original spec and inline in `script.sql`.
- For deeper MotherDuck or DuckDB SQL questions (CSV reading options, `SUMMARIZE`, `SELECT * EXCLUDE`, string functions, casting), use the `ask_docs_question` MCP tool or the MotherDuck docs.
