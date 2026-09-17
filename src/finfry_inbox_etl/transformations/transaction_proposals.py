from pyspark import pipelines as dp
from pyspark.sql import functions as dbf


@dp.materialized_view(
    name="gold_transaction_proposals",
    comment="Balanced Finfry proposals with deterministic book-account validation.",
)
def gold_transaction_proposals():
    accounts_table = spark.conf.get("finfry_inbox.book_accounts_table")  # noqa: F821

    receipts = spark.read.table("gold_receipts")  # noqa: F821
    recommendations = spark.read.table("silver_account_recommendations").selectExpr(  # noqa: F821
        "receipt_id",
        "book_id",
        "account_policy",
        "default_payment_account",
        "context_error",
        "try_variant_get(account_recommendation, '$.error_message', 'STRING') AS recommendation_error",
        "try_variant_get(account_recommendation, '$.response.decision.value', 'STRING') AS decision",
        "try_variant_get(account_recommendation, '$.response.decision.confidence_score', 'DOUBLE') "
        "AS decision_confidence",
        "try_variant_get(account_recommendation, '$.response.existing_account.value', 'STRING') "
        "AS existing_account",
        "try_variant_get(account_recommendation, '$.response.existing_account.confidence_score', 'DOUBLE') "
        "AS existing_account_confidence",
        "try_variant_get(account_recommendation, '$.response.suggested_new_account.value', 'STRING') "
        "AS suggested_new_account",
        "try_variant_get(account_recommendation, '$.response.suggested_new_account.confidence_score', 'DOUBLE') "
        "AS suggested_new_account_confidence",
        "try_variant_get(account_recommendation, '$.response.rationale.value', 'STRING') AS rationale",
    )
    accounts = spark.read.table(accounts_table).filter("active").select("book_id", "account_name")  # noqa: F821

    joined = receipts.join(recommendations, "receipt_id", "inner")
    validated = (
        joined.alias("proposal")
        .join(
            accounts.alias("existing"),
            (dbf.col("proposal.book_id") == dbf.col("existing.book_id"))
            & (dbf.col("proposal.existing_account") == dbf.col("existing.account_name")),
            "left",
        )
        .join(
            accounts.alias("suggested"),
            (dbf.col("proposal.book_id") == dbf.col("suggested.book_id"))
            & (dbf.col("proposal.suggested_new_account") == dbf.col("suggested.account_name")),
            "left",
        )
        .join(
            accounts.alias("payment"),
            (dbf.col("proposal.book_id") == dbf.col("payment.book_id"))
            & (dbf.col("proposal.default_payment_account") == dbf.col("payment.account_name")),
            "left",
        )
        .select(
            "proposal.*",
            dbf.col("existing.account_name").alias("validated_existing_account"),
            dbf.col("suggested.account_name").alias("duplicate_suggested_account"),
            dbf.col("payment.account_name").alias("validated_payment_account"),
        )
        .withColumn(
            "categorization_account",
            dbf.expr(
                """
                CASE
                  WHEN decision = 'existing_account' AND validated_existing_account IS NOT NULL
                    THEN existing_account
                  WHEN decision = 'new_account'
                    AND suggested_new_account RLIKE '^Expenses(:[^:]+)+$'
                    AND duplicate_suggested_account IS NULL
                    THEN suggested_new_account
                END
                """
            ),
        )
        .withColumn(
            "account_action",
            dbf.expr(
                """
                CASE
                  WHEN decision = 'existing_account' AND categorization_account IS NOT NULL THEN 'reuse'
                  WHEN decision = 'new_account' AND categorization_account IS NOT NULL THEN 'create'
                END
                """
            ),
        )
        .withColumn(
            "recommendation_confidence",
            dbf.expr(
                """
                least(
                  coalesce(decision_confidence, CAST(0 AS DOUBLE)),
                  coalesce(
                    CASE
                      WHEN decision = 'existing_account' THEN existing_account_confidence
                      WHEN decision = 'new_account' THEN suggested_new_account_confidence
                    END,
                    CAST(0 AS DOUBLE)
                  )
                )
                """
            ),
        )
        .withColumn("amount_cents", dbf.expr("CAST(total * 100 AS BIGINT)"))
        .withColumn(
            "proposed_postings",
            dbf.expr(
                """
                CASE WHEN categorization_account IS NOT NULL AND validated_payment_account IS NOT NULL
                  THEN array(
                    named_struct('account', categorization_account, 'amount', amount_cents),
                    named_struct('account', default_payment_account, 'amount', -amount_cents)
                  )
                END
                """
            ),
        )
        .withColumn(
            "proposal_balanced",
            dbf.expr(
                """
                proposed_postings IS NOT NULL
                AND aggregate(proposed_postings, CAST(0 AS BIGINT), (total, posting) -> total + posting.amount) = 0
                """
            ),
        )
    )

    return validated.withColumn(
        "proposal_status",
        dbf.expr(
            """
            CASE
              WHEN context_error IS NOT NULL THEN context_error
              WHEN recommendation_error IS NOT NULL THEN 'recommendation_error'
              WHEN decision = 'uncertain' OR decision IS NULL THEN 'needs_manual_categorization'
              WHEN validated_payment_account IS NULL THEN 'invalid_payment_account'
              WHEN decision = 'existing_account' AND validated_existing_account IS NULL
                THEN 'invalid_existing_account'
              WHEN decision = 'new_account'
                AND (suggested_new_account NOT RLIKE '^Expenses(:[^:]+)+$'
                  OR duplicate_suggested_account IS NOT NULL)
                THEN 'invalid_new_account'
              WHEN recommendation_confidence < 0.80 THEN 'low_confidence'
              WHEN NOT proposal_balanced THEN 'unbalanced_proposal'
              ELSE 'ready_for_review'
            END
            """
        ),
    ).drop(
        "validated_existing_account",
        "duplicate_suggested_account",
        "validated_payment_account",
    )
