from pyspark import pipelines as dp
from pyspark.sql import functions as dbf

RECEIPT_SCHEMA = {
    "merchant": {
        "type": "string",
        "description": "Merchant or store name printed on the receipt.",
    },
    "transaction_date": {
        "type": "string",
        "description": "Purchase date in YYYY-MM-DD format.",
    },
    "currency": {
        "type": "enum",
        "labels": ["USD", "EUR", "GBP", "CAD", "AUD", "JPY", "OTHER"],
        "description": "ISO 4217 currency code; infer from the symbol and merchant location when unambiguous.",
    },
    "subtotal": {
        "type": "number",
        "description": "Receipt subtotal before tax, with no currency symbol.",
    },
    "tax": {
        "type": "number",
        "description": "Total tax charged, with no currency symbol.",
    },
    "total": {
        "type": "number",
        "description": "Final amount paid, with no currency symbol.",
    },
    "payment_method": {
        "type": "string",
        "description": "Payment method printed on the receipt, such as Debit, Visa, Cash, or Check.",
    },
    "line_items": {
        "type": "array",
        "description": "Every purchased item in receipt order; exclude subtotal, tax, and total rows.",
        "items": {
            "type": "object",
            "properties": {
                "description": {
                    "type": "string",
                    "description": "Item description exactly as printed, preserving useful capitalization.",
                },
                "quantity": {
                    "type": "number",
                    "description": "Quantity purchased; use 1 when the receipt has one unnumbered item.",
                },
                "unit_price": {
                    "type": "number",
                    "description": "Price for one unit, with no currency symbol.",
                },
                "line_total": {
                    "type": "number",
                    "description": "Extended amount for the line, with no currency symbol.",
                },
            },
        },
    },
}


@dp.materialized_view(
    name="silver_extracted_receipts",
    comment="Typed receipt fields with extraction confidence and source citations.",
)
def silver_extracted_receipts():
    parsed = spark.read.table("silver_parsed_receipts")  # noqa: F821 - injected by Lakeflow

    return parsed.select(
        "receipt_id",
        "source_path",
        "source_size_bytes",
        "ingested_at",
        dbf.ai_extract(
            "parsed_document",
            RECEIPT_SCHEMA,
            {
                "version": "2.1",
                "instructions": (
                    "These are retail receipts. Extract only fields supported by the document. "
                    "Do not calculate missing monetary values."
                ),
                "enableCitations": "true",
                "enableConfidenceScores": "true",
            },
        ).alias("extracted_receipt"),
    )
