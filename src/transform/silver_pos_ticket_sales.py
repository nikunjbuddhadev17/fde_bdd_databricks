"""Silver: POS Ticket Sales.

Cleanses `bronze.pos_ticket_sales` -- the table with seeded, per-POS-system
data-quality issues (see `src/ingest/ingest_pos_ticket_sales.py`) -- into a
validated Silver table.

Processing order:
  1. Parse `purchase_timestamp` (ISO, or legacy "%m/%d/%Y %I:%M %p" used by
     pos_v2_batch) and `showtime` (ISO) into proper TIMESTAMP columns.
  2. Deduplicate on `transaction_id`, keeping the earliest `purchase_timestamp`
     (a replay of an existing transaction should not override the original
     purchase time).
  3. Referential integrity: LEFT JOIN `bronze.theater_layout` on
     `(theater_id, screen_id)` and `bronze.imdb_movie_metadata` on `movie_id`.
     A null match means an orphan reference.
  4. Business rules: `ticket_price` in (0, 100], `seats_sold` > 0, and
     `format = 'IMAX'` only on an `imax_capable` screen.
  5. Final output is ordered by (theater_id, screen_id, showtime,
     purchase_timestamp) for deterministic, idempotent replay.

Each row is assigned at most one `rejection_reason` -- the checks above are
evaluated in this precedence order, first match wins:
  malformed_timestamp > duplicate_transaction > null_theater_id >
  null_screen_id > null_showtime > invalid_ticket_price >
  invalid_seats_sold > orphan_theater > orphan_movie > format_mismatch

Outputs:
  silver.stg_pos_ticket_sales  -- clean, validated, deduplicated rows
  silver.pos_quality_issues    -- rejected rows + rejection_reason

Run via DABs:
    databricks bundle run -t dev build_medallion_theater_sales
"""

from __future__ import annotations

import argparse
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


logger = logging.getLogger("fde_theater.silver_pos_ticket_sales")

RAW_COLUMNS: list[str] = [
    "transaction_id", "theater_id", "screen_id", "movie_id", "format",
    "showtime", "ticket_type", "ticket_price", "seats_sold",
    "purchase_timestamp", "payment_method", "pos_source",
]

ISO_TS_PATTERN = "yyyy-MM-dd'T'HH:mm:ss"
LEGACY_TS_PATTERN = "MM/dd/yyyy hh:mm a"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--bronze-schema", required=True)
    parser.add_argument("--silver-schema", required=True)
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def parse_timestamps(df: DataFrame) -> DataFrame:
    """Parses purchase_timestamp (ISO or legacy) and showtime (ISO) to TIMESTAMP."""
    df = df.withColumn(
        "_parsed_purchase_timestamp",
        F.coalesce(
            F.to_timestamp("purchase_timestamp", ISO_TS_PATTERN),
            F.to_timestamp("purchase_timestamp", LEGACY_TS_PATTERN),
        ),
    )
    df = df.withColumn("_parsed_showtime", F.to_timestamp("showtime", ISO_TS_PATTERN))
    return df


def add_referential_integrity(df: DataFrame, layout_df: DataFrame, movie_df: DataFrame) -> DataFrame:
    """LEFT JOINs to bronze dimension tables; null match == orphan reference."""
    layout_lookup = (
        layout_df.select("theater_id", "screen_id", "imax_capable")
                 .withColumnRenamed("imax_capable", "_screen_imax_capable")
    )
    movie_lookup = (
        movie_df.select("imdb_id").distinct()
                .withColumn("_movie_matched", F.lit(True))
                .withColumnRenamed("imdb_id", "movie_id")
    )
    df = df.join(layout_lookup, on=["theater_id", "screen_id"], how="left")
    df = df.join(movie_lookup, on="movie_id", how="left")
    return df


def add_quality_flags(df: DataFrame) -> DataFrame:
    """Adds one boolean flag column per DQ check plus a prioritized rejection_reason."""
    df = df.withColumn("is_malformed_timestamp", F.col("_parsed_purchase_timestamp").isNull())

    dup_window = Window.partitionBy("transaction_id").orderBy(
        F.col("_parsed_purchase_timestamp").asc_nulls_last(),
        F.monotonically_increasing_id().asc(),
    )
    df = df.withColumn("_rn", F.row_number().over(dup_window))
    df = df.withColumn("is_duplicate_transaction", F.col("_rn") > 1)

    df = df.withColumn("is_null_theater_id", F.col("theater_id").isNull())
    df = df.withColumn("is_null_screen_id", F.col("screen_id").isNull())
    df = df.withColumn("is_null_showtime", F.col("showtime").isNull())

    df = df.withColumn(
        "is_invalid_ticket_price",
        (F.col("ticket_price") <= 0) | (F.col("ticket_price") > 100),
    )
    df = df.withColumn("is_invalid_seats_sold", F.col("seats_sold") <= 0)

    df = df.withColumn("is_orphan_theater", F.col("_screen_imax_capable").isNull())
    df = df.withColumn("is_orphan_movie", F.col("_movie_matched").isNull())

    df = df.withColumn(
        "is_format_mismatch",
        (F.col("format") == "IMAX")
        & (F.col("_screen_imax_capable").isNull() | (F.col("_screen_imax_capable") == False)),  # noqa: E712
    )

    df = df.withColumn(
        "rejection_reason",
        F.when(F.col("is_malformed_timestamp"), F.lit("malformed_timestamp"))
         .when(F.col("is_duplicate_transaction"), F.lit("duplicate_transaction"))
         .when(F.col("is_null_theater_id"), F.lit("null_theater_id"))
         .when(F.col("is_null_screen_id"), F.lit("null_screen_id"))
         .when(F.col("is_null_showtime"), F.lit("null_showtime"))
         .when(F.col("is_invalid_ticket_price"), F.lit("invalid_ticket_price"))
         .when(F.col("is_invalid_seats_sold"), F.lit("invalid_seats_sold"))
         .when(F.col("is_orphan_theater"), F.lit("orphan_theater"))
         .when(F.col("is_orphan_movie"), F.lit("orphan_movie"))
         .when(F.col("is_format_mismatch"), F.lit("format_mismatch"))
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

    pos_table = f"{args.catalog}.{args.bronze_schema}.pos_ticket_sales"
    layout_table = f"{args.catalog}.{args.bronze_schema}.theater_layout"
    movie_table = f"{args.catalog}.{args.bronze_schema}.imdb_movie_metadata"
    stg_table = f"{args.catalog}.{args.silver_schema}.stg_pos_ticket_sales"
    quality_table = f"{args.catalog}.{args.silver_schema}.pos_quality_issues"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.silver_schema}")

    logger.info("Reading: %s, %s, %s", pos_table, layout_table, movie_table)
    pos_df = spark.table(pos_table)
    layout_df = spark.table(layout_table)
    movie_df = spark.table(movie_table)
    pos_count = pos_df.count()

    df = parse_timestamps(pos_df)
    df = add_referential_integrity(df, layout_df, movie_df)
    df = add_quality_flags(df)
    df = df.withColumn("quality_checked_at", F.current_timestamp())
    # Bronze does not persist a row-level arrival timestamp (Phase 1 left
    # ingest tasks unchanged); this is a proxy for "when Silver observed it".
    df = df.withColumn("ingest_timestamp", F.current_timestamp())

    accepted_df = (
        df.filter(F.col("rejection_reason").isNull())
        .select(
            "transaction_id", "theater_id", "screen_id", "movie_id", "format",
            F.col("_parsed_showtime").alias("showtime"),
            "ticket_type", "ticket_price", "seats_sold",
            F.col("_parsed_purchase_timestamp").alias("purchase_timestamp"),
            "payment_method", "pos_source", "quality_checked_at", "ingest_timestamp",
        )
        .withColumn("data_quality_flag", F.lit(False))
        .orderBy("theater_id", "screen_id", "showtime", "purchase_timestamp")
    )
    rejected_df = (
        df.filter(F.col("rejection_reason").isNotNull())
        .select(*RAW_COLUMNS, "rejection_reason", "quality_checked_at", "ingest_timestamp")
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
    logger.info("bronze.pos_ticket_sales: %s rows", f"{pos_count:,}")
    logger.info("%s: %s rows accepted", stg_table, f"{accepted_count:,}")
    logger.info("%s: %s rows rejected", quality_table, f"{rejected_count:,}")
    if rejected_count:
        logger.info("Rejection breakdown by POS system and reason:")
        (spark.table(quality_table)
              .groupBy("pos_source", "rejection_reason")
              .count()
              .orderBy("pos_source", "rejection_reason")
              .show(100, truncate=False))


if __name__ == "__main__":
    main()
