# Finfry Inbox

Finfry Inbox turns receipt documents into reviewed, balanced transaction
proposals for Finfry. Databricks owns document ingestion, extraction,
normalization, and proposal generation. Finfry remains the ledger authority and
posts nothing without explicit approval.

The initial milestones are intentionally narrow:

1. Upload synthetic PNG, JPEG, or PDF receipts to a Unity Catalog volume.
2. Incrementally ingest the file into a Bronze Delta streaming table.
3. Preserve its bytes, source metadata, and a stable SHA-256 receipt ID.
4. Reject empty files and quarantine unsupported extensions through pipeline
   expectations.
5. Parse each Bronze document into versioned structural elements with
   `ai_parse_document`.
6. Extract typed receipt fields with `ai_extract`, retaining confidence scores
   and source citations.
7. Normalize the result into accounting-friendly Gold columns and verify that
   line items equal the subtotal and subtotal plus tax equals the total.
8. Join the receipt to its Finfry book, give the AI only that book's active
   chart of accounts, and persist an existing/new/uncertain recommendation.
9. Verify the recommendation against the book in ordinary code and construct a
   balanced, review-only Finfry transaction proposal.

## Architecture

```text
/Volumes/workspace/finfry_inbox/raw_receipts
                        |
                        v
       Lakeflow Declarative Pipeline
                        |
                        v
 workspace.finfry_inbox.bronze_receipt_files
                        |
                        | ai_parse_document (schema 2.0)
                        v
 workspace.finfry_inbox.silver_parsed_receipts
                        |
                        | ai_extract (schema 2.1)
                        v
 workspace.finfry_inbox.silver_extracted_receipts
                        |
                        | typed normalization + arithmetic checks
                        v
      workspace.finfry_inbox.gold_receipts
                        |
                        | receipt assignment + book-scoped chart
                        v
 workspace.finfry_inbox.silver_account_recommendations
                        |
                        | membership + hierarchy + balance checks
                        v
 workspace.finfry_inbox.gold_transaction_proposals
```

`book_settings`, `book_accounts`, and `receipt_assignments` are synthetic
reference tables for this exercise. Production versions will be synchronized
from Finfry. Finfry account names are identifiers; there is no invented account
ID layer. A proposed new account is explicitly marked `create`, while a known
account is marked `reuse`.

Later milestones add merchant-history rules, deduplication, and a human review
application. A validated proposal is only `ready_for_review`; it is never posted
or added to the chart automatically.

The demo uses three synthetic outcomes: reuse a known account, propose a new
account for a deliberately narrow book, and reject inconsistent receipt
arithmetic. See [DEMO.md](DEMO.md).

## Local development

```sh
uv sync
uv run pytest
uv run ruff check .
```

The domain module is ordinary Python and runs locally. Spark and AI document
processing execute remotely in Databricks.

## Databricks deployment

Choose and authenticate a Databricks CLI profile explicitly, then replace
`<profile>` below with that profile name. The bootstrap is idempotent and creates
the demo schema, managed receipt Volume, reference tables, and seed rows.

```sh
databricks experimental aitools tools query \
  --file fixtures/seed_reference_data.sql \
  --profile <profile>
databricks bundle validate --strict -t dev --profile <profile>
databricks bundle deploy -t dev --profile <profile>
```

Continue with the fixture upload, pipeline run, expected outcomes, and notebook
walkthrough in [DEMO.md](DEMO.md). Upload only synthetic or fully redacted receipts.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).
