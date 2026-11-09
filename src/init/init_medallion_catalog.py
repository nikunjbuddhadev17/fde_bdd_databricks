"""Initialize Medallion Catalog & Schemas.

Creates the theater_management catalog and three schemas (bronze, silver, gold)
if they don't already exist. This is a one-time setup step before the medallion
pipeline runs.

Run via DABs:
    databricks bundle run -t dev init_medallion_catalog

Or standalone:
    python src/init/init_medallion_catalog.py --catalog theater_management
"""

from __future__ import annotations

import argparse
import logging

from pyspark.sql import SparkSession


logger = logging.getLogger("fde_theater.init_medallion_catalog")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True, default="theater_management")
    parser.add_argument("--bronze-schema", required=True, default="bronze")
    parser.add_argument("--silver-schema", required=True, default="silver")
    parser.add_argument("--gold-schema", required=True, default="gold")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    spark = SparkSession.builder.getOrCreate()

    logger.info("Initializing medallion catalog and schemas")
    logger.info("Catalog: %s", args.catalog)
    logger.info("Schemas: %s, %s, %s", args.bronze_schema, args.silver_schema, args.gold_schema)

    # Create catalog
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {args.catalog}")
    logger.info("✓ Catalog created: %s", args.catalog)

    # Create schemas
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.bronze_schema}")
    logger.info("✓ Schema created: %s.%s", args.catalog, args.bronze_schema)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.silver_schema}")
    logger.info("✓ Schema created: %s.%s", args.catalog, args.silver_schema)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.gold_schema}")
    logger.info("✓ Schema created: %s.%s", args.catalog, args.gold_schema)

    logger.info("Medallion catalog and schemas initialized successfully")


if __name__ == "__main__":
    main()
