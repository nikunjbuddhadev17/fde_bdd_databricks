"""Transform: Denormalized Theater Sales.

Joins the three source tables (POS ticket sales, theater layout, IMDB movie
metadata) into a single wide table that powers the dashboard.

Notes on the as-is transform:
* The join is an INNER JOIN to the layout and metadata tables, which means
  orphan POS rows (bad theater_id, bad movie_id) silently disappear.
* No cleansing of ticket_price <= 0, null screen_id / showtime, malformed
  purchase_timestamp, or duplicate transaction_id is performed.
* Managers see the result of these issues in the dashboard and have no way to
  correct them.

Run via DABs:
    databricks bundle run -t dev build_denormalized_theater_sales
"""

from __future__ import annotations

import argparse
import logging

from pyspark.sql import SparkSession


logger = logging.getLogger("fde_theater.build_denormalized_theater_sales")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    spark = SparkSession.builder.getOrCreate()

    target_table = f"{args.catalog}.{args.schema}.denormalized_theater_sales"
    logger.info("Writing: %s", target_table)

    spark.sql(f"USE CATALOG {args.catalog}")
    spark.sql(f"USE SCHEMA {args.schema}")

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
  s.pos_source
FROM pos_ticket_sales s
INNER JOIN theater_layout       l ON s.theater_id = l.theater_id AND s.screen_id = l.screen_id
INNER JOIN imdb_movie_metadata  m ON s.movie_id  = m.imdb_id
""")

    pos_count = spark.table("pos_ticket_sales").count()
    denorm_count = spark.table("denormalized_theater_sales").count()
    logger.info("Built %s", target_table)
    logger.info("pos_ticket_sales: %s", f"{pos_count:,}")
    logger.info("denormalized_theater_sales: %s", f"{denorm_count:,}")
    logger.info("dropped by inner joins: %s", f"{pos_count - denorm_count:,}")


if __name__ == "__main__":
    main()
