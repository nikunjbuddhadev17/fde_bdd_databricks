"""Gold: Fact Ticket Sales.

Builds the business-ready, denormalized fact table from the three Silver
tables. Unlike the as-is `build_denormalized_theater_sales.py` (INNER JOIN on
raw, uncleaned Bronze), this LEFT JOINs the already-validated Silver tables so
that every Silver row is preserved in Gold -- Silver's referential-integrity
checks mean an unmatched dimension here would indicate the dimension changed
between Silver runs, not a dirty source record.

Quality metadata (`data_quality_flag`, `quality_checked_at`,
`ingest_timestamp`) carried over from `silver.stg_pos_ticket_sales` stays
visible on every Gold row per the plan's "quality metadata visible to
downstream" requirement.

Run via DABs:
    databricks bundle run -t dev build_medallion_theater_sales
"""

from __future__ import annotations

import argparse
import logging

from pyspark.sql import SparkSession


logger = logging.getLogger("fde_theater.gold_fct_ticket_sales")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--silver-schema", required=True)
    parser.add_argument("--gold-schema", required=True)
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    spark = SparkSession.builder.getOrCreate()

    silver_pos = f"{args.catalog}.{args.silver_schema}.stg_pos_ticket_sales"
    silver_layout = f"{args.catalog}.{args.silver_schema}.stg_theater_layout"
    silver_movie = f"{args.catalog}.{args.silver_schema}.stg_imdb_movie_metadata"
    target_table = f"{args.catalog}.{args.gold_schema}.fct_ticket_sales"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.gold_schema}")

    logger.info("Reading: %s, %s, %s", silver_pos, silver_layout, silver_movie)
    logger.info("Writing: %s", target_table)

    spark.sql(f"""
CREATE OR REPLACE TABLE {target_table} AS
SELECT
  s.transaction_id,
  s.theater_id,
  l.theater_name,
  l.city,
  l.state,
  l.open_time,
  l.close_time,
  s.screen_id,
  l.screen_type,
  l.imax_capable,
  l.row_count,
  l.seats_per_row,
  l.total_seats,
  s.movie_id,
  m.title              AS movie_title,
  m.genre,
  m.runtime_minutes,
  m.mpaa_rating,
  m.release_date,
  m.imdb_rating,
  s.format,
  s.showtime,
  s.ticket_type,
  s.ticket_price,
  s.seats_sold,
  (s.ticket_price * s.seats_sold) AS gross_revenue,
  s.purchase_timestamp,
  s.payment_method,
  s.pos_source,
  s.data_quality_flag,
  s.quality_checked_at,
  s.ingest_timestamp
FROM {silver_pos} s
LEFT JOIN {silver_layout} l ON s.theater_id = l.theater_id AND s.screen_id = l.screen_id
LEFT JOIN {silver_movie}  m ON s.movie_id  = m.imdb_id
""")

    silver_count = spark.table(silver_pos).count()
    gold_count = spark.table(target_table).count()
    unmatched_dim = spark.table(target_table).filter(
        "theater_name IS NULL OR movie_title IS NULL"
    ).count()

    logger.info("%s: %s rows", silver_pos, f"{silver_count:,}")
    logger.info("%s: %s rows", target_table, f"{gold_count:,}")
    if gold_count != silver_count:
        logger.warning(
            "Row count mismatch: silver=%s gold=%s (LEFT JOIN should preserve all rows)",
            silver_count, gold_count,
        )
    if unmatched_dim:
        logger.warning(
            "%s rows have an unmatched theater/movie dimension despite passing "
            "Silver's referential-integrity checks -- investigate Silver/Gold run ordering.",
            unmatched_dim,
        )


if __name__ == "__main__":
    main()
