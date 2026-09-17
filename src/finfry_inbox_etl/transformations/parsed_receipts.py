from pyspark import pipelines as dp


@dp.materialized_view(
    name="silver_parsed_receipts",
    comment="Receipt documents parsed into versioned structural elements.",
)
def silver_parsed_receipts():
    parsed = spark.read.table("bronze_receipt_files").selectExpr(  # noqa: F821 - injected by Lakeflow
        "receipt_id",
        "source_path",
        "source_size_bytes",
        "ingested_at",
        """
        ai_parse_document(
          content,
          map('version', '2.0', 'descriptionElementTypes', '')
        ) AS parsed_document
        """,
    )

    return parsed.selectExpr(
        "*",
        "parsed_document:error_status::STRING AS parse_error",
    )
