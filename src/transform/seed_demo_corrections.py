"""Seed Demo Corrections.

One-time (per deploy) demonstration of the correction feedback loop: picks
one real, currently-PENDING_REVIEW row per illustrative rejection_reason
from `silver.pos_quality_issues` and inserts a plausible correction into
`silver.pos_corrections`, exactly as a data steward would via the Data
Steward Review tab in the Databricks App.

This is deliberately idempotent-and-done-once: if `pos_corrections` already
has any rows (from this seed, from the App, or from direct SQL), the script
logs and exits without adding more. A real correction workflow never runs on
autopilot forever -- a human decides when a row needs review.

Two-run demonstration cycle (this is expected, not a bug):
  Run 1: silver_pos_ticket_sales quarantines bad rows -> this script reads
         those PENDING_REVIEW rows and seeds a handful of corrections.
  Run 2: silver_pos_ticket_sales re-reads pos_corrections and the seeded
         rows flow through as CORRECTED / APPROVED_AS_IS / REJECTED_PERMANENTLY.

Demo categories (one row each, skipped if none exist):
  orphan_theater        -> CORRECT: the screen_id ("T042-S03") still encodes
                            the real theater_id even though theater_id itself
                            was corrupted; corrected from that prefix.
  invalid_ticket_price   -> CORRECT: the original price is unrecoverable, but
                            the standard price for that ticket_type/format is
                            reconstructed from the same price table the
                            generator uses.
  format_mismatch        -> APPROVE_AS_IS: a steward confirms the screen
                            actually has portable IMAX-certified equipment
                            not reflected in theater_layout -- a false
                            positive, not bad data.
  duplicate_transaction  -> REJECT_PERMANENTLY: confirmed replay, the
                            original is already in Silver.
  orphan_movie           -> REJECT_PERMANENTLY: the movie reference is an
                            unrecoverable placeholder ID, escalated upstream.

Run via DABs:
    databricks bundle run -t dev build_medallion_theater_sales
"""

from __future__ import annotations

import argparse
import logging

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


logger = logging.getLogger("fde_theater.seed_demo_corrections")

REVIEWER = "demo_data_steward"

# Mirrors TICKET_TYPES / IMAX_PREMIUM in ingest_pos_ticket_sales.py -- used
# only to reconstruct a plausible corrected_ticket_price for the demo; the
# original (corrupted) price is unrecoverable, so this is the best available
# estimate a real steward would also reach for.
BASE_TICKET_PRICES: dict[str, float] = {
    "Standard": 14.50, "Senior": 9.50, "Child": 8.00, "Matinee": 10.50, "Premium": 19.00,
}
IMAX_PREMIUM = 5.00

CORRECTIONS_SCHEMA = StructType([
    StructField("transaction_id", StringType(), nullable=False),
    StructField("decision", StringType(), nullable=False),
    StructField("corrected_theater_id", StringType(), nullable=True),
    StructField("corrected_screen_id", StringType(), nullable=True),
    StructField("corrected_movie_id", StringType(), nullable=True),
    StructField("corrected_format", StringType(), nullable=True),
    StructField("corrected_showtime", StringType(), nullable=True),
    StructField("corrected_ticket_price", DoubleType(), nullable=True),
    StructField("corrected_seats_sold", IntegerType(), nullable=True),
    StructField("corrected_purchase_timestamp", StringType(), nullable=True),
    StructField("review_notes", StringType(), nullable=True),
    StructField("reviewed_by", StringType(), nullable=True),
])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--bronze-schema", required=True)
    parser.add_argument("--silver-schema", required=True)
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def one_pending_row(quality_df, rejection_reason: str):
    """Deterministically picks one PENDING_REVIEW row for a reason (smallest
    transaction_id, so repeated runs before the first seed pick the same
    candidate). Returns None if no such row exists."""
    rows = (
        quality_df.filter(F.col("rejection_reason") == rejection_reason)
        .orderBy("transaction_id")
        .limit(1)
        .collect()
    )
    return rows[0] if rows else None


def build_orphan_theater_correction(quality_df, layout_df):
    """screen_id ("T042-S03") still encodes the real theater_id even though
    theater_id was corrupted -- corrected_theater_id is derived from it, then
    verified against theater_layout before being trusted."""
    candidates = (
        quality_df.filter(F.col("rejection_reason") == "orphan_theater")
        .withColumn("_derived_theater_id", F.split(F.col("screen_id"), "-").getItem(0))
        .join(
            layout_df.select("theater_id").distinct(),
            F.col("_derived_theater_id") == layout_df["theater_id"],
            "inner",
        )
        .orderBy("transaction_id")
        .limit(1)
        .collect()
    )
    if not candidates:
        return None
    row = candidates[0]
    notes = (
        f"Screen ID '{row.screen_id}' indicates the true theater is "
        f"{row._derived_theater_id}; theater_id was corrupted upstream. "
        "Corrected per the screen_id-to-theater_id naming convention."
    )
    return Row(
        transaction_id=row.transaction_id, decision="CORRECT",
        corrected_theater_id=row._derived_theater_id,
        corrected_screen_id=None, corrected_movie_id=None, corrected_format=None,
        corrected_showtime=None, corrected_ticket_price=None,
        corrected_seats_sold=None, corrected_purchase_timestamp=None,
        review_notes=notes, reviewed_by=REVIEWER,
    )


def build_invalid_ticket_price_correction(quality_df):
    row = one_pending_row(quality_df, "invalid_ticket_price")
    if row is None:
        return None
    base = BASE_TICKET_PRICES.get(row.ticket_type, 14.50)
    corrected_price = base + (IMAX_PREMIUM if row.format == "IMAX" else 0.0)
    notes = (
        f"Original price ${row.ticket_price:.2f} was clearly erroneous (<= $0); "
        f"reconstructed the standard {row.ticket_type} price"
        f"{' + IMAX premium' if row.format == 'IMAX' else ''} = ${corrected_price:.2f}."
    )
    return Row(
        transaction_id=row.transaction_id, decision="CORRECT",
        corrected_theater_id=None, corrected_screen_id=None, corrected_movie_id=None,
        corrected_format=None, corrected_showtime=None,
        corrected_ticket_price=corrected_price,
        corrected_seats_sold=None, corrected_purchase_timestamp=None,
        review_notes=notes, reviewed_by=REVIEWER,
    )


def build_format_mismatch_correction(quality_df):
    row = one_pending_row(quality_df, "format_mismatch")
    if row is None:
        return None
    notes = (
        f"Confirmed with theater ops: screen {row.screen_id} has portable "
        "IMAX-certified equipment not yet reflected in theater_layout.imax_capable. "
        "Transaction is legitimate; approving as-is rather than editing dimension data."
    )
    return Row(
        transaction_id=row.transaction_id, decision="APPROVE_AS_IS",
        corrected_theater_id=None, corrected_screen_id=None, corrected_movie_id=None,
        corrected_format=None, corrected_showtime=None, corrected_ticket_price=None,
        corrected_seats_sold=None, corrected_purchase_timestamp=None,
        review_notes=notes, reviewed_by=REVIEWER,
    )


def build_duplicate_transaction_correction(quality_df):
    row = one_pending_row(quality_df, "duplicate_transaction")
    if row is None:
        return None
    notes = (
        "Confirmed duplicate replay from a POS batch re-submission; the original "
        "transaction is already recorded in Silver. No correction needed."
    )
    return Row(
        transaction_id=row.transaction_id, decision="REJECT_PERMANENTLY",
        corrected_theater_id=None, corrected_screen_id=None, corrected_movie_id=None,
        corrected_format=None, corrected_showtime=None, corrected_ticket_price=None,
        corrected_seats_sold=None, corrected_purchase_timestamp=None,
        review_notes=notes, reviewed_by=REVIEWER,
    )


def build_orphan_movie_correction(quality_df):
    row = one_pending_row(quality_df, "orphan_movie")
    if row is None:
        return None
    notes = (
        f"Movie reference '{row.movie_id}' is a known placeholder/unrecoverable ID "
        "from the upstream feed; escalated to the POS vendor. Excluding pending a "
        "vendor-side fix."
    )
    return Row(
        transaction_id=row.transaction_id, decision="REJECT_PERMANENTLY",
        corrected_theater_id=None, corrected_screen_id=None, corrected_movie_id=None,
        corrected_format=None, corrected_showtime=None, corrected_ticket_price=None,
        corrected_seats_sold=None, corrected_purchase_timestamp=None,
        review_notes=notes, reviewed_by=REVIEWER,
    )


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    spark = SparkSession.builder.getOrCreate()

    quality_table = f"{args.catalog}.{args.silver_schema}.pos_quality_issues"
    corrections_table = f"{args.catalog}.{args.silver_schema}.pos_corrections"
    layout_table = f"{args.catalog}.{args.bronze_schema}.theater_layout"

    if not (spark.catalog.tableExists(quality_table) and spark.catalog.tableExists(corrections_table)):
        logger.info(
            "%s or %s does not exist yet (silver_pos_ticket_sales hasn't run). Skipping demo seed.",
            quality_table, corrections_table,
        )
        return

    existing_count = spark.table(corrections_table).count()
    if existing_count > 0:
        logger.info(
            "%s already has %s row(s); demo seed only runs once. Skipping.",
            corrections_table, f"{existing_count:,}",
        )
        return

    quality_df = spark.table(quality_table).filter(F.col("resolution_status") == "PENDING_REVIEW")
    layout_df = spark.table(layout_table)

    builders = [
        ("orphan_theater", lambda: build_orphan_theater_correction(quality_df, layout_df)),
        ("invalid_ticket_price", lambda: build_invalid_ticket_price_correction(quality_df)),
        ("format_mismatch", lambda: build_format_mismatch_correction(quality_df)),
        ("duplicate_transaction", lambda: build_duplicate_transaction_correction(quality_df)),
        ("orphan_movie", lambda: build_orphan_movie_correction(quality_df)),
    ]
    demo_rows: list[Row] = []
    for reason, builder in builders:
        row = builder()
        if row is not None:
            demo_rows.append(row)
        else:
            logger.info("No PENDING_REVIEW row found for '%s'; skipping that demo example.", reason)

    if not demo_rows:
        logger.info("No demo examples available yet (no PENDING_REVIEW rows match any category).")
        return

    demo_df = (
        spark.createDataFrame(demo_rows, schema=CORRECTIONS_SCHEMA)
        .withColumn("reviewed_at", F.current_timestamp())
    )
    demo_df.createOrReplaceTempView("_demo_corrections")

    # WHEN NOT MATCHED only: never overwrite a correction that already exists
    # for a transaction_id (defensive -- the existing_count==0 guard above
    # already makes this unreachable in practice, but a MERGE is the correct,
    # safe primitive for "insert these if not already present").
    spark.sql(f"""
        MERGE INTO {corrections_table} AS t
        USING _demo_corrections AS s
        ON t.transaction_id = s.transaction_id
        WHEN NOT MATCHED THEN INSERT *
    """)

    logger.info("Seeded %d demo correction(s) into %s:", len(demo_rows), corrections_table)
    spark.table(corrections_table).select(
        "transaction_id", "decision", "review_notes",
    ).show(truncate=False)
    logger.info(
        "Re-run build_medallion_theater_sales to see these corrections flow "
        "through silver_pos_ticket_sales into stg_pos_ticket_sales / fct_ticket_sales."
    )


if __name__ == "__main__":
    main()
