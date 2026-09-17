-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Finfry Inbox: receipt to balanced proposal
-- MAGIC
-- MAGIC This notebook is a read-only walkthrough of the Finfry Inbox Lakeflow pipeline.
-- MAGIC It demonstrates three synthetic outcomes:
-- MAGIC
-- MAGIC | Fixture | Business case | Expected outcome |
-- MAGIC |---|---|---|
-- MAGIC | `receipt_001.png` | Grocery receipt with a matching book account | Reuse `Expenses:Food` |
-- MAGIC | `receipt_002.png` | Developer-tool purchase in a deliberately narrow book | Propose a new `Expenses:...` account |
-- MAGIC | `receipt_003.png` | Printed subtotal, tax, and total do not reconcile | Stop at `arithmetic_mismatch` |
-- MAGIC
-- MAGIC The AI parses and recommends. Ordinary SQL validates arithmetic, account membership,
-- MAGIC hierarchy, confidence, and double-entry balance before anything becomes reviewable.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Follow the records through the medallion layers
-- MAGIC
-- MAGIC Bronze preserves source bytes and metadata. Silver parses documents and extracts typed
-- MAGIC fields. Gold normalizes the receipt and constructs review-only accounting proposals.

-- COMMAND ----------

SELECT 1 AS layer_order, 'Bronze: raw files' AS layer, COUNT(*) AS record_count
FROM workspace.finfry_inbox.bronze_receipt_files
UNION ALL
SELECT 2, 'Silver: parsed documents', COUNT(*)
FROM workspace.finfry_inbox.silver_parsed_receipts
UNION ALL
SELECT 3, 'Silver: extracted fields', COUNT(*)
FROM workspace.finfry_inbox.silver_extracted_receipts
UNION ALL
SELECT 4, 'Gold: validated receipts', COUNT(*)
FROM workspace.finfry_inbox.gold_receipts
UNION ALL
SELECT 5, 'Silver: account recommendations', COUNT(*)
FROM workspace.finfry_inbox.silver_account_recommendations
UNION ALL
SELECT 6, 'Gold: transaction proposals', COUNT(*)
FROM workspace.finfry_inbox.gold_transaction_proposals
ORDER BY layer_order;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Bronze: immutable evidence and stable identity
-- MAGIC
-- MAGIC The SHA-256 hash is the receipt ID. The raw binary remains available for reprocessing,
-- MAGIC while the notebook intentionally avoids displaying the binary column.

-- COMMAND ----------

SELECT
  regexp_extract(source_path, '[^/]+$', 0) AS source_file,
  receipt_id,
  source_size_bytes,
  source_modified_at,
  ingested_at
FROM workspace.finfry_inbox.bronze_receipt_files
ORDER BY source_file;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Silver: AI extraction with confidence and citations
-- MAGIC
-- MAGIC `ai_parse_document` first produces structural elements. `ai_extract` then returns a
-- MAGIC versioned `VARIANT` value; the pipeline retains confidence scores and citation IDs rather
-- MAGIC than flattening away the model evidence.

-- COMMAND ----------

SELECT
  regexp_extract(source_path, '[^/]+$', 0) AS source_file,
  try_variant_get(extracted_receipt, '$.metadata.version', 'STRING') AS schema_version,
  try_variant_get(extracted_receipt, '$.response.merchant.value', 'STRING') AS merchant,
  try_variant_get(extracted_receipt, '$.response.merchant.confidence_score', 'DOUBLE') AS merchant_confidence,
  try_variant_get(extracted_receipt, '$.response.merchant.citation_ids', 'ARRAY<INT>') AS merchant_citations,
  try_variant_get(extracted_receipt, '$.response.total.value', 'DECIMAL(18,2)') AS total,
  try_variant_get(extracted_receipt, '$.response.total.confidence_score', 'DOUBLE') AS total_confidence,
  try_variant_get(extracted_receipt, '$.error_message', 'STRING') AS extraction_error
FROM workspace.finfry_inbox.silver_extracted_receipts
ORDER BY source_file;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Gold receipts: deterministic document checks
-- MAGIC
-- MAGIC The intentionally defective receipt is blocked here. It never reaches account
-- MAGIC recommendation, so an LLM cannot reason its way around broken arithmetic.

-- COMMAND ----------

SELECT
  regexp_extract(source_path, '[^/]+$', 0) AS source_file,
  merchant,
  transaction_date,
  currency,
  subtotal,
  tax,
  total,
  line_items_total,
  totals_reconcile,
  line_items_reconcile,
  minimum_header_confidence,
  validation_status
FROM workspace.finfry_inbox.gold_receipts
ORDER BY source_file;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Book-scoped account recommendation
-- MAGIC
-- MAGIC The recommendation model receives only the active accounts for the assigned Finfry book.
-- MAGIC The narrow `demo-minimal` chart has no software or catch-all expense account, making a new
-- MAGIC account proposal appropriate. A production system would also encode catch-all usage as an
-- MAGIC explicit book policy and enforce it after inference.

-- COMMAND ----------

SELECT
  gr.merchant,
  sar.book_id,
  sar.account_policy,
  size(sar.available_accounts) AS available_account_count,
  try_variant_get(sar.account_recommendation, '$.response.decision.value', 'STRING') AS decision,
  try_variant_get(sar.account_recommendation, '$.response.existing_account.value', 'STRING') AS existing_account,
  try_variant_get(sar.account_recommendation, '$.response.suggested_new_account.value', 'STRING') AS suggested_new_account,
  try_variant_get(sar.account_recommendation, '$.response.rationale.value', 'STRING') AS rationale,
  try_variant_get(sar.account_recommendation, '$.response.decision.confidence_score', 'DOUBLE') AS decision_confidence,
  sar.context_error
FROM workspace.finfry_inbox.silver_account_recommendations AS sar
JOIN workspace.finfry_inbox.gold_receipts AS gr USING (receipt_id)
ORDER BY gr.merchant;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Demo summary: one row per receipt
-- MAGIC
-- MAGIC This left-joined view preserves the failed receipt even though it correctly has no AI
-- MAGIC recommendation or transaction proposal.

-- COMMAND ----------

SELECT
  regexp_extract(gr.source_path, '[^/]+$', 0) AS source_file,
  gr.merchant,
  gr.total,
  gr.validation_status,
  sar.book_id,
  gtp.decision,
  gtp.account_action,
  gtp.categorization_account,
  gtp.recommendation_confidence,
  gtp.proposal_status,
  gtp.proposal_balanced,
  gtp.proposed_postings
FROM workspace.finfry_inbox.gold_receipts AS gr
LEFT JOIN workspace.finfry_inbox.silver_account_recommendations AS sar
  ON gr.receipt_id = sar.receipt_id
LEFT JOIN workspace.finfry_inbox.gold_transaction_proposals AS gtp
  ON gr.receipt_id = gtp.receipt_id
ORDER BY source_file;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. Trust boundary
-- MAGIC
-- MAGIC - Receipt text is untrusted data, never instructions.
-- MAGIC - The model cannot see accounts from another book.
-- MAGIC - Existing accounts must match the book's active chart exactly.
-- MAGIC - New accounts must be new, begin with `Expenses:`, and remain review-only.
-- MAGIC - Both postings must exist and sum to zero.
-- MAGIC - `ready_for_review` is not approval and never posts to Finfry automatically.
