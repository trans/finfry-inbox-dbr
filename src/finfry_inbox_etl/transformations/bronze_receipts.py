from pyspark import pipelines as dp
from pyspark.sql import functions as F

RAW_RECEIPTS_PATH = spark.conf.get("finfry_inbox.raw_receipts_path")  # noqa: F821 - injected by Lakeflow


@dp.table(
    name="bronze_receipt_files",
    comment="Raw receipt files with source metadata and content hashes.",
)
@dp.expect_or_drop(
    "supported_receipt_type",
    r"lower(source_path) rlike '\\.(png|jpe?g|pdf)$'",
)
@dp.expect_or_fail("non_empty_file", "source_size_bytes > 0")
def bronze_receipt_files():
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821 - injected by Lakeflow
        .option("cloudFiles.format", "binaryFile")
        .load(RAW_RECEIPTS_PATH)
        .select(
            F.sha2("content", 256).alias("receipt_id"),
            F.col("path").alias("source_path"),
            F.col("modificationTime").alias("source_modified_at"),
            F.col("length").alias("source_size_bytes"),
            F.col("content"),
            F.current_timestamp().alias("ingested_at"),
        )
    )
