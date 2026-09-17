# Finfry Inbox Lakeflow pipeline

`transformations/bronze_receipts.py` declares the streaming ingestion table.
`transformations/parsed_receipts.py` declares the Silver materialized view and
parses each document into structural elements with `ai_parse_document` schema
version 2.0. `transformations/extracted_receipts.py` performs one persisted
`ai_extract` 2.1 call per document and retains its confidence and citation
metadata. `transformations/gold_receipts.py` converts that VARIANT result to
typed columns, reconciles both receipt equations, and assigns a review status.
`transformations/account_recommendations.py` joins each receipt to its book and
persists an AI recommendation made from only that book's active accounts.
`transformations/transaction_proposals.py` independently validates account
membership or a proposed `Expenses:*` hierarchy, selects the configured payment
account, and constructs balanced integer-cent postings for human review.
The pipeline receives the source volume path through Spark configuration so the
same code can be deployed to different environments without editing Python.
