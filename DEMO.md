# Finfry Inbox demo runbook

This runbook produces a short, repeatable walkthrough of three synthetic receipts.
It uses the `workspace.finfry_inbox` schema and the
`/Volumes/workspace/finfry_inbox/raw_receipts` volume selected for this project.

Use only synthetic or fully redacted documents. Replace `<profile>` below with the
Databricks CLI profile you deliberately selected for the demo workspace.

## Expected story

| Fixture | Assigned book | Expected invariant |
|---|---|---|
| `receipt_001.png` | `demo-personal` | Reuses the known food account and produces a balanced proposal |
| `receipt_002.png` | `demo-minimal` | Proposes creating a new `Expenses:...` account because no listed account fits |
| `receipt_003.png` | `demo-personal` | Stops with `arithmetic_mismatch` and never reaches recommendation |

The exact wording of an AI rationale or new subaccount may vary. The validation and
review boundaries above are the stable behavior to demonstrate.

## Prepare

Prerequisites:

- A Databricks workspace with serverless compute and AI Functions available.
- Databricks CLI 1.0 or newer with an authenticated profile.
- `uv` for the local Python checks.

Run the fast local checks:

```sh
uv run pytest
uv run ruff check .
```

Bootstrap the demo schema, managed Volume, two books, and three receipt assignments:

```sh
databricks experimental aitools tools query --file fixtures/seed_reference_data.sql --profile <profile>
```

The file is one SQL Scripting compound statement. It can alternatively be run as
a single cell in the Databricks SQL editor. The DDL uses `IF NOT EXISTS`, and the
seed rows use `MERGE`, so this step is safe to repeat.

Validate and deploy the development bundle. The selected CLI profile supplies the
workspace URL; the repository intentionally contains no personal workspace host:

```sh
databricks bundle validate --strict -t dev --profile <profile>
databricks bundle deploy -t dev --profile <profile>
```

Upload the three PNG fixtures. `dbfs:` is required by the CLI for Unity Catalog
Volume paths:

```sh
databricks fs cp fixtures/receipt_001.png dbfs:/Volumes/workspace/finfry_inbox/raw_receipts/receipt_001.png --overwrite --profile <profile>
databricks fs cp fixtures/receipt_002.png dbfs:/Volumes/workspace/finfry_inbox/raw_receipts/receipt_002.png --overwrite --profile <profile>
databricks fs cp fixtures/receipt_003.png dbfs:/Volumes/workspace/finfry_inbox/raw_receipts/receipt_003.png --overwrite --profile <profile>
```

Run the pipeline and wait for the update to complete:

```sh
databricks bundle run finfry_inbox_etl -t dev --profile <profile>
```

Do not use a full refresh for ordinary demos. The seed operation is idempotent, and
Auto Loader tracks the already-ingested files.

## Present

Open `notebooks/finfry_inbox_demo` from the deployed bundle files and run it from
top to bottom. Databricks omits the local `.sql` suffix when it imports the source
notebook. Free Edition automatically attaches serverless notebook compute when the
first cell runs; in other workspaces, select serverless or another SQL-capable
compute if Databricks prompts for one.

A concise presentation is:

1. Show the pipeline graph and explain Bronze, Silver, and Gold ownership.
2. Show stable content hashes in Bronze.
3. Show AI extraction confidence and citations in Silver.
4. Show the arithmetic mismatch stopped by deterministic Gold logic.
5. Compare the two book-scoped account catalogs and recommendations.
6. Finish on the one-row-per-receipt summary and expand the balanced postings.

The central message is: **AI proposes; deterministic code validates; a person still
decides.**

## Quick verification

The final notebook result should contain all three source files. Before presenting
the demo, confirm that:

- `receipt_001.png` is `ready_for_review` and balanced.
- `receipt_002.png` has `account_action = create`, an `Expenses:` account, and balanced postings.
- `receipt_003.png` has `validation_status = arithmetic_mismatch` and null proposal fields.
