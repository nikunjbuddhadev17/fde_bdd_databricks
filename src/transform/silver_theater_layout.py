"""Silver: Theater Layout.

Cleanses `bronze.theater_layout` into a validated Silver table. The source
data is generated clean (no seeded DQ issues), so this module mostly guards
against future, real-world ingestion where duplicates or bad data could
appear -- it is NOT a no-op.

Checks applied (first match wins, in this precedence order):
  1. duplicate_screen     -- more than one row for the same screen_id
  2. null_required_field  -- any required column is null
  3. invalid_time_range   -- open_time is not before close_time
  4. invalid_seating      -- row_count <= 0 or seats_per_row <= 0

Note on deduplication tie-breaking: bronze does not persist a row-level
ingest timestamp (Phase 1 intentionally left the ingest tasks unchanged), so
there is no true "latest" to order by. As a proxy for "last row seen in this
bronze read", rows are ordered by `monotonically_increasing_id()` descending
within each `screen_id` partition. In practice the generator never produces
duplicate screen_ids, so this branch is defensive rather than load-bearing.

Outputs:
  silver.stg_theater_layout             -- clean, validated rows
  silver.theater_layout_quality_issues  -- rejected rows + rejection_reason

Run via DABs:
    databricks bundle run -t dev build_medallion_theater_sales
"""

from __future__ import annotations

import argparse
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


logger = logging.getLogger("fde_theater.silver_theater_layout")

REQUIRED_COLUMNS: list[str] = [
    "theater_id", "theater_name", "city", "state", "open_time", "close_time",
    "screen_id", "screen_number", "screen_type", "imax_capable",
    "row_count", "seats_per_row", "total_seats",
]
APPROVED_SCREEN_TYPES: list[str] = ["Standard", "Recliner", "Dolby"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--bronze-schema", required=True)
    parser.add_argument("--silver-schema", required=True)
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def add_quality_flags(df: DataFrame) -> DataFrame:
    """Adds one boolean flag column per DQ check plus a prioritized rejection_reason."""
    dup_window = Window.partitionBy("screen_id").orderBy(F.monotonically_increasing_id().desc())

    df = df.withColumn("_rn", F.row_number().over(dup_window))
    df = df.withColumn("is_duplicate_screen", F.col("_rn") > 1)

    null_condition = F.lit(False)
    for column in REQUIRED_COLUMNS:
        null_condition = null_condition | F.col(column).isNull()
    df = df.withColumn("is_null_required_field", null_condition)

    # close_time == "00:00" represents midnight (end of the operating day),
    # which is always after any same-day open_time -- it is NOT an invalid range.
    df = df.withColumn(
        "is_invalid_time_range",
        F.when(F.col("close_time") == "00:00", F.lit(False))
         .otherwise(~(F.col("open_time") < F.col("close_time"))),
    )

    df = df.withColumn(
        "is_invalid_seating",
        (F.col("row_count") <= 0) | (F.col("seats_per_row") <= 0),
    )

    df = df.withColumn(
        "rejection_reason",
        F.when(F.col("is_duplicate_screen"), F.lit("duplicate_screen"))
         .when(F.col("is_null_required_field"), F.lit("null_required_field"))
         .when(F.col("is_invalid_time_range"), F.lit("invalid_time_range"))
         .when(F.col("is_invalid_seating"), F.lit("invalid_seating"))
         .otherwise(F.lit(None).cast("string")),
    )
    return df.drop("_rn")


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    spark = SparkSession.builder.getOrCreate()

    source_table = f"{args.catalog}.{args.bronze_schema}.theater_layout"
    stg_table = f"{args.catalog}.{args.silver_schema}.stg_theater_layout"
    quality_table = f"{args.catalog}.{args.silver_schema}.theater_layout_quality_issues"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.silver_schema}")

    logger.info("Reading: %s", source_table)
    bronze_df = spark.table(source_table)
    bronze_count = bronze_df.count()

    flagged_df = add_quality_flags(bronze_df)
    flagged_df = flagged_df.withColumn("quality_checked_at", F.current_timestamp())
    # Bronze does not persist a row-level arrival timestamp (Phase 1 left
    # ingest tasks unchanged); this is a proxy for "when Silver observed it".
    flagged_df = flagged_df.withColumn("ingest_timestamp", F.current_timestamp())

    business_columns = REQUIRED_COLUMNS

    accepted_df = (
        flagged_df.filter(F.col("rejection_reason").isNull())
        .select(*business_columns, "quality_checked_at", "ingest_timestamp")
        .withColumn("data_quality_flag", F.lit(False))
        .orderBy("theater_id", "screen_id")
    )
    rejected_df = (
        flagged_df.filter(F.col("rejection_reason").isNotNull())
        .select(
            *business_columns, "rejection_reason",
            "quality_checked_at", "ingest_timestamp",
        )
        .withColumn("data_quality_flag", F.lit(True))
    )

    (accepted_df.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(stg_table))
    (rejected_df.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(quality_table))

    accepted_count = spark.table(stg_table).count()
    rejected_count = spark.table(quality_table).count()
    logger.info("bronze.theater_layout: %s rows", f"{bronze_count:,}")
    logger.info("%s: %s rows accepted", stg_table, f"{accepted_count:,}")
    logger.info("%s: %s rows rejected", quality_table, f"{rejected_count:,}")
    if rejected_count:
        spark.table(quality_table).groupBy("rejection_reason").count().show(truncate=False)


if __name__ == "__main__":
    main()
