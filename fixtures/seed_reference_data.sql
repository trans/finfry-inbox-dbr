-- Idempotent bootstrap and synthetic reference data for the Finfry Inbox demo.
-- Production versions of these tables will be synchronized from Finfry.

BEGIN

CREATE SCHEMA IF NOT EXISTS workspace.finfry_inbox
COMMENT 'Finfry Inbox demo data and pipeline outputs';

CREATE VOLUME IF NOT EXISTS workspace.finfry_inbox.raw_receipts
COMMENT 'Synthetic or fully redacted receipt files for the Finfry Inbox demo';

CREATE TABLE IF NOT EXISTS workspace.finfry_inbox.book_settings (
  book_id STRING NOT NULL COMMENT 'Stable identifier for one Finfry ledger',
  account_policy STRING NOT NULL COMMENT 'Finfry unknown-account policy: strict, guard, or off',
  default_payment_account STRING NOT NULL COMMENT 'Default counter-account for receipt purchases',
  updated_at TIMESTAMP NOT NULL
)
COMMENT 'Book-level Finfry settings used when preparing transaction proposals';

CREATE TABLE IF NOT EXISTS workspace.finfry_inbox.book_accounts (
  book_id STRING NOT NULL COMMENT 'Finfry ledger that owns this account',
  account_name STRING NOT NULL COMMENT 'Hierarchical Finfry account name and identifier',
  account_type STRING NOT NULL COMMENT 'Top-level account type',
  description STRING COMMENT 'Non-secret context supplied to the recommendation model',
  active BOOLEAN NOT NULL,
  updated_at TIMESTAMP NOT NULL
)
COMMENT 'Book-scoped Finfry chart of accounts; account_name is the identifier';

CREATE TABLE IF NOT EXISTS workspace.finfry_inbox.receipt_assignments (
  receipt_id STRING NOT NULL COMMENT 'SHA-256 receipt identifier',
  book_id STRING NOT NULL COMMENT 'Finfry ledger selected when the receipt was uploaded',
  assigned_at TIMESTAMP NOT NULL
)
COMMENT 'Upload-time mapping that prevents receipts from seeing another book chart';

MERGE INTO workspace.finfry_inbox.book_settings AS target
USING (
  SELECT
    'demo-personal' AS book_id,
    'strict' AS account_policy,
    'Assets:Checking' AS default_payment_account,
    current_timestamp() AS updated_at
) AS source
ON target.book_id = source.book_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- A deliberately narrow chart demonstrates that recommendations are scoped to
-- each book instead of choosing from a universal or catch-all account list.
MERGE INTO workspace.finfry_inbox.book_settings AS target
USING (
  SELECT
    'demo-minimal' AS book_id,
    'guard' AS account_policy,
    'Assets:Checking' AS default_payment_account,
    current_timestamp() AS updated_at
) AS source
ON target.book_id = source.book_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

MERGE INTO workspace.finfry_inbox.book_accounts AS target
USING (
  SELECT *, current_timestamp() AS updated_at
  FROM VALUES
    ('demo-personal', 'Assets:Checking',          'Assets',      'Primary checking account', true),
    ('demo-personal', 'Assets:Cash',              'Assets',      'Physical cash', true),
    ('demo-personal', 'Liabilities:CreditCard',   'Liabilities', 'General credit card', true),
    ('demo-personal', 'Income:Salary',             'Income',      'Employment income', true),
    ('demo-personal', 'Expenses:Food',             'Expenses',    'Groceries and dining', true),
    ('demo-personal', 'Expenses:Housing',          'Expenses',    'Rent and household costs', true),
    ('demo-personal', 'Expenses:Transport',        'Expenses',    'Transit and vehicle costs', true),
    ('demo-personal', 'Expenses:Health',           'Expenses',    'Medical and wellness costs', true),
    ('demo-personal', 'Expenses:Entertainment',    'Expenses',    'Leisure and entertainment', true),
    ('demo-personal', 'Expenses:Misc',             'Expenses',    'Unclassified incidental expenses', true)
    AS accounts(book_id, account_name, account_type, description, active)
) AS source
ON target.book_id = source.book_id AND target.account_name = source.account_name
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

MERGE INTO workspace.finfry_inbox.book_accounts AS target
USING (
  SELECT *, current_timestamp() AS updated_at
  FROM VALUES
    ('demo-minimal', 'Assets:Checking',    'Assets',   'Primary checking account', true),
    ('demo-minimal', 'Expenses:Food',      'Expenses', 'Groceries and dining', true),
    ('demo-minimal', 'Expenses:Housing',   'Expenses', 'Rent and household costs', true),
    ('demo-minimal', 'Expenses:Transport', 'Expenses', 'Transit and vehicle costs', true)
    AS accounts(book_id, account_name, account_type, description, active)
) AS source
ON target.book_id = source.book_id AND target.account_name = source.account_name
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

MERGE INTO workspace.finfry_inbox.receipt_assignments AS target
USING (
  SELECT *, current_timestamp() AS assigned_at
  FROM VALUES
    ('b238c4e95be566c66c452a8c17e5e092e15158471286591cbe87ce6f54135111', 'demo-personal'),
    ('8e5e9a95d8a5deccfc1563c4ad728f91d5739f9bff6e67804d9e63b30f7b3bfe', 'demo-minimal'),
    ('1e06753988c9d114353723d6fa25c94a83c7715be48f5a686adb334ce3235586', 'demo-personal')
    AS assignments(receipt_id, book_id)
) AS source
ON target.receipt_id = source.receipt_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

END;
