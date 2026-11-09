"""Ingest: Theater Layout.

Generates the internally-available theater layout dataset deterministically
(seed=42) and writes it to the theater_layout table. Clean -- no DQ issues
seeded.

Run via DABs:
    databricks bundle run -t dev seed_theater_data
"""

from __future__ import annotations

import argparse
import logging
import random

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


logger = logging.getLogger("fde_theater.ingest_theater_layout")


CITIES = [
    ("New York", "NY"), ("Los Angeles", "CA"), ("Chicago", "IL"), ("Houston", "TX"),
    ("Phoenix", "AZ"), ("Philadelphia", "PA"), ("San Antonio", "TX"), ("San Diego", "CA"),
    ("Dallas", "TX"), ("Austin", "TX"), ("Jacksonville", "FL"), ("Fort Worth", "TX"),
    ("Columbus", "OH"), ("Charlotte", "NC"), ("Indianapolis", "IN"), ("San Francisco", "CA"),
    ("Seattle", "WA"), ("Denver", "CO"), ("Washington", "DC"), ("Boston", "MA"),
    ("El Paso", "TX"), ("Nashville", "TN"), ("Detroit", "MI"), ("Oklahoma City", "OK"),
    ("Portland", "OR"), ("Las Vegas", "NV"), ("Memphis", "TN"), ("Louisville", "KY"),
    ("Baltimore", "MD"), ("Milwaukee", "WI"), ("Albuquerque", "NM"), ("Tucson", "AZ"),
    ("Fresno", "CA"), ("Sacramento", "CA"), ("Kansas City", "MO"), ("Mesa", "AZ"),
    ("Atlanta", "GA"), ("Omaha", "NE"), ("Colorado Springs", "CO"), ("Raleigh", "NC"),
    ("Miami", "FL"), ("Long Beach", "CA"), ("Virginia Beach", "VA"), ("Oakland", "CA"),
    ("Minneapolis", "MN"), ("Tulsa", "OK"), ("Arlington", "TX"), ("Tampa", "FL"),
    ("New Orleans", "LA"), ("Wichita", "KS"), ("Cleveland", "OH"), ("Bakersfield", "CA"),
    ("Aurora", "CO"), ("Anaheim", "CA"), ("Honolulu", "HI"), ("Santa Ana", "CA"),
    ("Riverside", "CA"), ("Corpus Christi", "TX"), ("Lexington", "KY"), ("Stockton", "CA"),
    ("Henderson", "NV"), ("Saint Paul", "MN"), ("St. Louis", "MO"), ("Cincinnati", "OH"),
    ("Pittsburgh", "PA"), ("Greensboro", "NC"), ("Anchorage", "AK"), ("Plano", "TX"),
    ("Lincoln", "NE"), ("Orlando", "FL"), ("Irvine", "CA"), ("Newark", "NJ"),
    ("Toledo", "OH"), ("Durham", "NC"), ("Chula Vista", "CA"), ("Fort Wayne", "IN"),
    ("Jersey City", "NJ"), ("St. Petersburg", "FL"), ("Laredo", "TX"), ("Madison", "WI"),
    ("Chandler", "AZ"), ("Buffalo", "NY"), ("Lubbock", "TX"), ("Scottsdale", "AZ"),
    ("Reno", "NV"), ("Glendale", "AZ"), ("Gilbert", "AZ"), ("Winston-Salem", "NC"),
    ("North Las Vegas", "NV"), ("Norfolk", "VA"), ("Chesapeake", "VA"), ("Garland", "TX"),
    ("Irving", "TX"), ("Hialeah", "FL"), ("Fremont", "CA"), ("Boise", "ID"),
    ("Richmond", "VA"), ("Baton Rouge", "LA"), ("Spokane", "WA"), ("Des Moines", "IA"),
]

BRAND_PREFIXES = [
    "Downtown", "Westside", "Uptown", "Riverside", "Eastpark", "Northgate", "Southpoint",
    "Lakeside", "Hilltop", "Midtown", "Galleria", "Crossroads", "Central", "Plaza",
    "Heritage", "Grand", "Skyline", "Parkview", "Harbor", "Metro",
]

BRAND_SUFFIXES = [
    "Cinema", "Cinemas", "Multiplex", "Theater", "Theatre", "IMAX", "Movie House", "Megaplex",
]

LAYOUT_SCHEMA = StructType([
    StructField("theater_id", StringType(), nullable=False),
    StructField("theater_name", StringType(), nullable=False),
    StructField("city", StringType(), nullable=False),
    StructField("state", StringType(), nullable=False),
    StructField("open_time", StringType(), nullable=False),
    StructField("close_time", StringType(), nullable=False),
    StructField("screen_id", StringType(), nullable=False),
    StructField("screen_number", IntegerType(), nullable=False),
    StructField("screen_type", StringType(), nullable=False),
    StructField("imax_capable", BooleanType(), nullable=False),
    StructField("row_count", IntegerType(), nullable=False),
    StructField("seats_per_row", IntegerType(), nullable=False),
    StructField("total_seats", IntegerType(), nullable=False),
])


def pick_hours(rng: random.Random, n_screens: int) -> tuple[str, str]:
    """Larger theaters open earlier and close later. Times are HH:MM, same every day."""
    if n_screens >= 12:
        open_time = rng.choice(["09:00", "10:00", "10:00", "10:00"])
        close_time = rng.choice(["23:00", "23:30", "00:00", "00:00"])
    elif n_screens >= 8:
        open_time = rng.choice(["10:00", "10:00", "11:00"])
        close_time = rng.choice(["22:30", "23:00", "23:30"])
    else:
        open_time = rng.choice(["10:00", "11:00", "12:00"])
        close_time = rng.choice(["22:00", "22:30", "23:00"])
    return open_time, close_time


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument(
        "--n-theaters",
        type=int,
        default=500,
        help="Number of theaters to generate.",
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def build_layout_rows(rng: random.Random, n_theaters: int) -> list[Row]:
    rows: list[Row] = []
    for i in range(1, n_theaters + 1):
        theater_id = f"T{i:03d}"
        city, state = CITIES[(i - 1) % len(CITIES)]
        prefix = BRAND_PREFIXES[(i * 7) % len(BRAND_PREFIXES)]
        suffix = BRAND_SUFFIXES[(i * 3) % len(BRAND_SUFFIXES)]
        theater_name = f"{prefix} {suffix}"
        n_screens = rng.choices([4, 6, 8, 10, 12, 14, 16], weights=[5, 15, 25, 25, 15, 10, 5])[0]
        open_time, close_time = pick_hours(rng, n_screens)
        for screen_num in range(1, n_screens + 1):
            screen_id = f"{theater_id}-S{screen_num:02d}"
            n_rows = rng.choice([8, 10, 12, 14])
            seats_per_row = rng.choice([10, 12, 14, 16])
            screen_type = rng.choices(
                ["Standard", "Recliner", "Dolby"],
                weights=[70, 20, 10],
            )[0]
            imax_capable = rng.random() < 0.12
            rows.append(Row(
                theater_id=theater_id,
                theater_name=theater_name,
                city=city,
                state=state,
                open_time=open_time,
                close_time=close_time,
                screen_id=screen_id,
                screen_number=screen_num,
                screen_type=screen_type,
                imax_capable=imax_capable,
                row_count=n_rows,
                seats_per_row=seats_per_row,
                total_seats=n_rows * seats_per_row,
            ))
    return rows


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    spark = SparkSession.builder.getOrCreate()
    rng = random.Random(args.seed)

    target_table = f"{args.catalog}.{args.schema}.theater_layout"
    logger.info("Generating theater layout (n_theaters=%d, seed=%d)", args.n_theaters, args.seed)
    logger.info("Writing: %s", target_table)

    spark.sql(f"CREATE CATALOG IF NOT EXISTS {args.catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.schema}")

    rows = build_layout_rows(rng, args.n_theaters)
    df = spark.createDataFrame(rows, schema=LAYOUT_SCHEMA)

    (df.write
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(target_table))

    row_count = spark.table(target_table).count()
    logger.info("Loaded %s rows into %s", f"{row_count:,}", target_table)


if __name__ == "__main__":
    main()
