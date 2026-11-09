"""Quality Summary.

Final task of the medallion pipeline. Reports how many rows flowed
bronze -> silver -> gold for each source, the overall and per-POS-system
rejection rates, and the top rejection reasons by volume. Logs the report and
persists it to `gold.dq_summary` so Phase 3's quality dashboard can read it
without re-scanning the Silver quality-issue tables.

Run via DABs:
    databricks bundle run -t dev build_medallion_theater_sales
"""

from __future__ import annotations

import argparse
import logging

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F


logger = logging.getLogger("fde_theater.quality_summary")


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
) -> Row:
    rows_input = spark.table(bronze_table).count()
    rows_accepted = spark.table(stg_table).count()
    rows_rejected = spark.table(quality_table).count()
    rejection_rate = (rows_rejected / rows_input) if rows_input else 0.0
    return Row(
        dataset=dataset,
        dimension="overall",
        rows_input=rows_input,
        rows_accepted=rows_accepted,
        rows_rejected=rows_rejected,
        rejection_rate=round(rejection_rate, 4),
    )


def per_pos_source_rows(
    spark: SparkSession,
    bronze_table: str,
    quality_table: str,
) -> list[Row]:
    """Breaks down pos_ticket_sales rejection rate by POS system (bronze is the
    source of truth for rows_input since a rejected row still originated there)."""
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
        rows.append(Row(
            dataset="pos_ticket_sales",
            dimension=pos_source,
            rows_input=rows_input,
            rows_accepted=rows_input - rows_rejected,
            rows_rejected=rows_rejected,
            rejection_rate=round(rows_rejected / rows_input, 4) if rows_input else 0.0,
        ))
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

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold}")

    summary_rows: list[Row] = [
        overall_summary_row(
            spark, "pos_ticket_sales",
            f"{bronze}.pos_ticket_sales",
            f"{silver}.stg_pos_ticket_sales",
            f"{silver}.pos_quality_issues",
        ),
        overall_summary_row(
            spark, "theater_layout",
            f"{bronze}.theater_layout",
            f"{silver}.stg_theater_layout",
            f"{silver}.theater_layout_quality_issues",
        ),
        overall_summary_row(
            spark, "imdb_movie_metadata",
            f"{bronze}.imdb_movie_metadata",
            f"{silver}.stg_imdb_movie_metadata",
            f"{silver}.imdb_quality_issues",
        ),
    ]
    summary_rows.extend(per_pos_source_rows(
        spark, f"{bronze}.pos_ticket_sales", f"{silver}.pos_quality_issues",
    ))

    summary_df = spark.createDataFrame(summary_rows).withColumn("as_of", F.current_timestamp())
    target_table = f"{gold}.dq_summary"
    (summary_df.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table))

    logger.info("=" * 72)
    logger.info("MEDALLION PIPELINE QUALITY SUMMARY")
    logger.info("=" * 72)
    for row in summary_rows:
        logger.info(
            "%-20s %-20s input=%-8s accepted=%-8s rejected=%-8s rejection_rate=%.2f%%",
            row.dataset, row.dimension, f"{row.rows_input:,}",
            f"{row.rows_accepted:,}", f"{row.rows_rejected:,}", row.rejection_rate * 100,
        )

    gold_fact_table = f"{gold}.fct_ticket_sales"
    gold_count = spark.table(gold_fact_table).count()
    logger.info("-" * 72)
    logger.info("%s: %s rows", gold_fact_table, f"{gold_count:,}")

    logger.info("-" * 72)
    logger.info("Top rejection reasons by volume (pos_quality_issues):")
    (spark.table(f"{silver}.pos_quality_issues")
          .groupBy("rejection_reason")
          .count()
          .orderBy(F.desc("count"))
          .show(20, truncate=False))

    logger.info("Quality summary written to: %s", target_table)


if __name__ == "__main__":
    main()
