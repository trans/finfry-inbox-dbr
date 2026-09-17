from pyspark import pipelines as dp
from pyspark.sql import functions as dbf

LINE_ITEM_EXTRACTION_TYPE = """
ARRAY<STRUCT<
  description:STRUCT<citation_ids:ARRAY<INT>,confidence_score:DOUBLE,value:STRING>,
  line_total:STRUCT<citation_ids:ARRAY<INT>,confidence_score:DOUBLE,value:DECIMAL(18,2)>,
  quantity:STRUCT<citation_ids:ARRAY<INT>,confidence_score:DOUBLE,value:DECIMAL(18,3)>,
  unit_price:STRUCT<citation_ids:ARRAY<INT>,confidence_score:DOUBLE,value:DECIMAL(18,2)>
>>
""".replace("\n", "")


@dp.materialized_view(
    name="gold_receipts",
    comment="Typed receipt facts with deterministic validation and review status.",
)
def gold_receipts():
    extracted = spark.read.table("silver_extracted_receipts").selectExpr(  # noqa: F821 - injected by Lakeflow
        "receipt_id",
        "source_path",
        "source_size_bytes",
        "ingested_at",
        "try_variant_get(extracted_receipt, '$.error_message', 'STRING') AS extraction_error",
        "try_variant_get(extracted_receipt, '$.metadata.version', 'STRING') AS extraction_schema_version",
        "try_variant_get(extracted_receipt, '$.metadata', 'VARIANT') AS extraction_metadata",
        "try_variant_get(extracted_receipt, '$.response.merchant.value', 'STRING') AS merchant",
        "try_variant_get(extracted_receipt, '$.response.merchant.confidence_score', 'DOUBLE') AS merchant_confidence",
        "try_cast(try_variant_get(extracted_receipt, '$.response.transaction_date.value', 'STRING') AS DATE) "
        "AS transaction_date",
        "try_variant_get(extracted_receipt, '$.response.transaction_date.confidence_score', 'DOUBLE') "
        "AS transaction_date_confidence",
        "try_variant_get(extracted_receipt, '$.response.currency.value', 'STRING') AS currency",
        "try_variant_get(extracted_receipt, '$.response.currency.confidence_score', 'DOUBLE') "
        "AS currency_confidence",
        "try_variant_get(extracted_receipt, '$.response.subtotal.value', 'DECIMAL(18,2)') AS subtotal",
        "try_variant_get(extracted_receipt, '$.response.subtotal.confidence_score', 'DOUBLE') "
        "AS subtotal_confidence",
        "try_variant_get(extracted_receipt, '$.response.tax.value', 'DECIMAL(18,2)') AS tax",
        "try_variant_get(extracted_receipt, '$.response.tax.confidence_score', 'DOUBLE') AS tax_confidence",
        "try_variant_get(extracted_receipt, '$.response.total.value', 'DECIMAL(18,2)') AS total",
        "try_variant_get(extracted_receipt, '$.response.total.confidence_score', 'DOUBLE') AS total_confidence",
        "try_variant_get(extracted_receipt, '$.response.payment_method.value', 'STRING') AS payment_method",
        "try_variant_get(extracted_receipt, '$.response.payment_method.confidence_score', 'DOUBLE') "
        "AS payment_method_confidence",
        f"try_variant_get(extracted_receipt, '$.response.line_items', '{LINE_ITEM_EXTRACTION_TYPE}') "
        "AS extracted_line_items",
    )

    normalized = (
        extracted.withColumn(
            "line_items",
            dbf.expr(
                """
                transform(
                  extracted_line_items,
                  item -> named_struct(
                    'description', item.description.value,
                    'quantity', item.quantity.value,
                    'unit_price', item.unit_price.value,
                    'line_total', item.line_total.value,
                    'citation_ids', item.description.citation_ids
                  )
                )
                """
            ),
        )
        .drop("extracted_line_items")
        .withColumn(
            "line_items_total",
            dbf.expr(
                """
                aggregate(
                  line_items,
                  CAST(0 AS DECIMAL(18,2)),
                  (running_total, item) ->
                    CAST(running_total + item.line_total AS DECIMAL(18,2))
                )
                """
            ),
        )
        .withColumn(
            "minimum_header_confidence",
            dbf.expr(
                """
                least(
                  coalesce(merchant_confidence, CAST(0 AS DOUBLE)),
                  coalesce(transaction_date_confidence, CAST(0 AS DOUBLE)),
                  coalesce(currency_confidence, CAST(0 AS DOUBLE)),
                  coalesce(subtotal_confidence, CAST(0 AS DOUBLE)),
                  coalesce(tax_confidence, CAST(0 AS DOUBLE)),
                  coalesce(total_confidence, CAST(0 AS DOUBLE)),
                  coalesce(payment_method_confidence, CAST(0 AS DOUBLE))
                )
                """
            ),
        )
        .withColumn(
            "totals_reconcile",
            dbf.expr("subtotal IS NOT NULL AND tax IS NOT NULL AND total = subtotal + tax"),
        )
        .withColumn(
            "line_items_reconcile",
            dbf.expr("size(line_items) > 0 AND subtotal IS NOT NULL AND line_items_total = subtotal"),
        )
    )

    return normalized.withColumn(
        "validation_status",
        dbf.expr(
            """
            CASE
              WHEN extraction_error IS NOT NULL THEN 'extraction_error'
              WHEN merchant IS NULL
                OR transaction_date IS NULL
                OR currency IS NULL
                OR subtotal IS NULL
                OR tax IS NULL
                OR total IS NULL
                OR payment_method IS NULL
                THEN 'missing_required_field'
              WHEN line_items IS NULL OR size(line_items) = 0 THEN 'missing_line_items'
              WHEN NOT totals_reconcile OR NOT line_items_reconcile THEN 'arithmetic_mismatch'
              WHEN minimum_header_confidence < 0.80 THEN 'low_confidence'
              ELSE 'ready_for_review'
            END
            """
        ),
    )
