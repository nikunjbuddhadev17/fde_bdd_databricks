"""Quality Summary.

Final task of the medallion pipeline. Reports how many rows flowed
bronze -> silver -> gold for each source, the overall and per-POS-system
rejection rates, and the top rejection reasons by volume. Logs the report and
persists it to two Gold tables so Phase 3's quality dashboard and alerts can
read it without re-scanning the Silver quality-issue tables on every query:

  gold.dq_summary           -- one row per (dataset, dimension) per run, where
                                 dimension is "overall" or (for pos_ticket_sales)
                                 a POS system name
  gold.dq_rejection_reasons -- one row per (dataset, pos_source, rejection_reason)
                                 per run, with a reason-level rate for the
                                 "orphan rate spike" alert and the rejection
                                 reasons chart

Both tables are APPENDED to (not overwritten) each run: every row carries an
`as_of` run timestamp, so the dashboard can chart rejection-rate trend over
time and alerts can always filter to `as_of = (SELECT MAX(as_of) ...)` for
the latest run.

Run via DABs:
    databricks bundle run -t dev build_medallion_theater_sales
"""

from __future__ import annotations

import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


logger = logging.getLogger("fde_theater.quality_summary")

SUMMARY_SCHEMA = StructType([
    StructField("dataset", StringType(), nullable=False),
    StructField("dimension", StringType(), nullable=False),
    StructField("rows_input", LongType(), nullable=False),
    StructField("rows_accepted", LongType(), nullable=False),
    StructField("rows_rejected", LongType(), nullable=False),
    StructField("rejection_rate", DoubleType(), nullable=False),
])

REASON_SCHEMA = StructType([
    StructField("dataset", StringType(), nullable=False),
    StructField("pos_source", StringType(), nullable=True),
    StructField("rejection_reason", StringType(), nullable=False),
    StructField("count", LongType(), nullable=False),
    StructField("rows_input", LongType(), nullable=False),
    StructField("reason_rate", DoubleType(), nullable=False),
])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--bronze-schema", required=True)
    parser.add_argument("--silver-schema", required=True)
    parser.add_argument("--gold-schema", required=True)
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def overall_summary_row(
    spark: SparkSession,
    dataset: str,
    bronze_table: str,
    stg_table: str,
    quality_table: str,
) -> tuple:
    """Returns a tuple in SUMMARY_SCHEMA column order (plain tuples, not Row(**kwargs),
    so there is no ambiguity about field order when paired with an explicit schema)."""
    rows_input = spark.table(bronze_table).count()
    rows_accepted = spark.table(stg_table).count()
    rows_rejected = spark.table(quality_table).count()
    rejection_rate = (rows_rejected / rows_input) if rows_input else 0.0
    return (dataset, "overall", rows_input, rows_accepted, rows_rejected, round(rejection_rate, 4))


def per_pos_source_rows(
    spark: SparkSession,
    bronze_table: str,
    quality_table: str,
) -> list[tuple]:
    """Breaks down pos_ticket_sales rejection rate by POS system (bronze is the
    source of truth for rows_input since a rejected row still originated there).
    Returns tuples in SUMMARY_SCHEMA column order."""
    bronze_counts = {
        r.pos_source: r["count"]
        for r in spark.table(bronze_table).groupBy("pos_source").count().collect()
    }
    rejected_counts = {
        r.pos_source: r["count"]
        for r in spark.table(quality_table).groupBy("pos_source").count().collect()
    }

    rows = []
    for pos_source, rows_input in sorted(bronze_counts.items()):
        rows_rejected = rejected_counts.get(pos_source, 0)
        rejection_rate = round(rows_rejected / rows_input, 4) if rows_input else 0.0
        rows.append((
            "pos_ticket_sales", pos_source, rows_input,
            rows_input - rows_rejected, rows_rejected, rejection_rate,
        ))
    return rows


def pos_rejection_reason_rows(
    spark: SparkSession,
    bronze_table: str,
    quality_table: str,
) -> list[tuple]:
    """Reason-level breakdown for pos_ticket_sales, broken down by POS system
    too (rates are normalized against that POS system's own row count, not the
    whole dataset) -- this is what the "orphan rate spike" alert reads.
    Returns tuples in REASON_SCHEMA column order."""
    bronze_counts = {
        r.pos_source: r["count"]
        for r in spark.table(bronze_table).groupBy("pos_source").count().collect()
    }
    reason_counts = (
        spark.table(quality_table)
        .groupBy("pos_source", "rejection_reason")
        .count()
        .collect()
    )
    rows = []
    for r in reason_counts:
        rows_input = bronze_counts.get(r.pos_source, 0)
        reason_rate = round(r["count"] / rows_input, 4) if rows_input else 0.0
        rows.append((
            "pos_ticket_sales", r.pos_source, r.rejection_reason,
            r["count"], rows_input, reason_rate,
        ))
    return rows


def dimension_rejection_reason_rows(
    spark: SparkSession,
    dataset: str,
    bronze_table: str,
    quality_table: str,
) -> list[tuple]:
    """Reason-level breakdown for theater_layout / imdb_movie_metadata, which
    have no POS-system dimension (pos_source is always NULL for these).
    Returns tuples in REASON_SCHEMA column order."""
    rows_input = spark.table(bronze_table).count()
    reason_counts = spark.table(quality_table).groupBy("rejection_reason").count().collect()
    rows = []
    for r in reason_counts:
        reason_rate = round(r["count"] / rows_input, 4) if rows_input else 0.0
        rows.append((dataset, None, r.rejection_reason, r["count"], rows_input, reason_rate))
    return rows


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    spark = SparkSession.builder.getOrCreate()

    bronze = f"{args.catalog}.{args.bronze_schema}"
    silver = f"{args.catalog}.{args.silver_schema}"
    gold = f"{args.catalog}.{args.gold_schema}"

    pos_bronze = f"{bronze}.pos_ticket_sales"
    pos_quality = f"{silver}.pos_quality_issues"
    layout_bronze = f"{bronze}.theater_layout"
    layout_quality = f"{silver}.theater_layout_quality_issues"
    imdb_bronze = f"{bronze}.imdb_movie_metadata"
    imdb_quality = f"{silver}.imdb_quality_issues"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold}")

    summary_rows: list[tuple] = [
        overall_summary_row(
            spark, "pos_ticket_sales", pos_bronze,
            f"{silver}.stg_pos_ticket_sales", pos_quality,
        ),
        overall_summary_row(
            spark, "theater_layout", layout_bronze,
            f"{silver}.stg_theater_layout", layout_quality,
        ),
        overall_summary_row(
            spark, "imdb_movie_metadata", imdb_bronze,
            f"{silver}.stg_imdb_movie_metadata", imdb_quality,
        ),
    ]
    summary_rows.extend(per_pos_source_rows(spark, pos_bronze, pos_quality))

    reason_rows: list[tuple] = []
    reason_rows.extend(pos_rejection_reason_rows(spark, pos_bronze, pos_quality))
    reason_rows.extend(dimension_rejection_reason_rows(
        spark, "theater_layout", layout_bronze, layout_quality,
    ))
    reason_rows.extend(dimension_rejection_reason_rows(
        spark, "imdb_movie_metadata", imdb_bronze, imdb_quality,
    ))

    # Each run APPENDS a snapshot (tagged with a single as_of timestamp shared
    # by every row from this run) instead of overwriting, so the dashboard can
    # chart rejection-rate trend over time and alerts can filter to the latest
    # run via `as_of = (SELECT MAX(as_of) ...)`.
    summary_df = (
        spark.createDataFrame(summary_rows, schema=SUMMARY_SCHEMA)
        .withColumn("as_of", F.current_timestamp())
    )
    summary_table = f"{gold}.dq_summary"
    (summary_df.write
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(summary_table))

    reason_df = (
        spark.createDataFrame(reason_rows, schema=REASON_SCHEMA)
        .withColumn("as_of", F.current_timestamp())
    )
    reasons_table = f"{gold}.dq_rejection_reasons"
    (reason_df.write
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(reasons_table))

    logger.info("=" * 72)
    logger.info("MEDALLION PIPELINE QUALITY SUMMARY")
    logger.info("=" * 72)
    for dataset, dimension, rows_input, rows_accepted, rows_rejected, rejection_rate in summary_rows:
        logger.info(
            "%-20s %-20s input=%-8s accepted=%-8s rejected=%-8s rejection_rate=%.2f%%",
            dataset, dimension, f"{rows_input:,}",
            f"{rows_accepted:,}", f"{rows_rejected:,}", rejection_rate * 100,
        )

    gold_fact_table = f"{gold}.fct_ticket_sales"
    gold_count = spark.table(gold_fact_table).count()
    logger.info("-" * 72)
    logger.info("%s: %s rows", gold_fact_table, f"{gold_count:,}")

    logger.info("-" * 72)
    logger.info("Top rejection reasons by volume (this run):")
    for dataset, pos_source, rejection_reason, count, _rows_input, reason_rate in sorted(
        reason_rows, key=lambda r: r[3], reverse=True,
    )[:20]:
        logger.info(
            "%-20s %-18s %-22s count=%-8s reason_rate=%.2f%%",
            dataset, pos_source or "-", rejection_reason, f"{count:,}", reason_rate * 100,
        )

    logger.info("Quality summary written to: %s", summary_table)
    logger.info("Rejection reason breakdown written to: %s", reasons_table)


if __name__ == "__main__":
    main()
