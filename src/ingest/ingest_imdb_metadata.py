"""Ingest: IMDB Movie Metadata.

Daily batch full-refresh of movie metadata from the IMDB API. Clean -- no DQ
issues seeded. The scaffold synthesizes the API response deterministically
(seed=42) instead of calling the live IMDB API.

Run via DABs:
    databricks bundle run -t dev seed_theater_data
"""

from __future__ import annotations

import argparse
import logging
import random

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


logger = logging.getLogger("fde_theater.ingest_imdb_metadata")


# 100 movies spanning 2023-2026, mixed genres / ratings / runtimes.
MOVIES: list[tuple[str, str, str, int, str, str]] = [
    # 2023
    ("tt2023001", "The Glass Cathedral",        "Drama",       124, "R",     "2023-01-13"),
    ("tt2023002", "Crimson Sky",                "Action",      138, "PG-13", "2023-01-27"),
    ("tt2023003", "Wholesome Heists",           "Comedy",      102, "PG-13", "2023-02-10"),
    ("tt2023004", "Algorithm of the Heart",     "Romance",     108, "PG",    "2023-02-14"),
    ("tt2023005", "Last Stop Before Mars",      "Sci-Fi",      145, "PG-13", "2023-03-03"),
    ("tt2023006", "Lighthouse Keepers",         "Drama",       119, "R",     "2023-03-17"),
    ("tt2023007", "Silent Engine",              "Thriller",    131, "R",     "2023-04-07"),
    ("tt2023008", "Paw Patrol Returns",         "Animation",    92, "G",     "2023-04-21"),
    ("tt2023009", "The Final Algorithm",        "Sci-Fi",      142, "PG-13", "2023-05-05"),
    ("tt2023010", "Summer in Vermillion",       "Drama",       110, "PG-13", "2023-05-26"),
    ("tt2023011", "Vault of Echoes",            "Mystery",     124, "PG-13", "2023-06-09"),
    ("tt2023012", "Beyond the Horizon",         "Adventure",   156, "PG",    "2023-06-23"),
    ("tt2023013", "Karma Roadshow",             "Comedy",       98, "R",     "2023-07-07"),
    ("tt2023014", "Iron Phoenix",               "Action",      144, "PG-13", "2023-07-21"),
    ("tt2023015", "The Unraveling",             "Thriller",    128, "R",     "2023-08-04"),
    ("tt2023016", "Whisperwood",                "Horror",      102, "R",     "2023-08-18"),
    ("tt2023017", "Songs of the Pacific",       "Documentary",  96, "PG",    "2023-09-01"),
    ("tt2023018", "The Long Goodbye",           "Drama",       134, "R",     "2023-09-15"),
    ("tt2023019", "Spire",                      "Sci-Fi",      138, "PG-13", "2023-09-29"),
    ("tt2023020", "Murder at Maple Hall",       "Mystery",     116, "PG-13", "2023-10-13"),
    # 2024
    ("tt2024001", "Above Water",                "Thriller",    128, "R",     "2024-01-12"),
    ("tt2024002", "The Velvet Cell",            "Drama",       134, "R",     "2024-01-26"),
    ("tt2024003", "Sunset Spies",               "Action",      142, "PG-13", "2024-02-09"),
    ("tt2024004", "Starcrossed",                "Romance",     109, "PG-13", "2024-02-14"),
    ("tt2024005", "Quantum Reach",              "Sci-Fi",      148, "PG-13", "2024-03-01"),
    ("tt2024006", "Backwoods",                  "Horror",       96, "R",     "2024-03-15"),
    ("tt2024007", "The Long Road West",         "Drama",       156, "R",     "2024-04-05"),
    ("tt2024008", "Talking Critters",           "Animation",    92, "G",     "2024-04-19"),
    ("tt2024009", "Iron Phoenix 2",             "Action",      152, "PG-13", "2024-05-03"),
    ("tt2024010", "Last Light",                 "Drama",       124, "PG-13", "2024-05-24"),
    ("tt2024011", "Detective Romeo",            "Comedy",      105, "PG-13", "2024-06-07"),
    ("tt2024012", "Echoes of the Deep",         "Adventure",   138, "PG",    "2024-06-21"),
    ("tt2024013", "Heat Wave",                  "Action",      122, "R",     "2024-07-05"),
    ("tt2024014", "The Reckoning",              "Western",     142, "R",     "2024-07-19"),
    ("tt2024015", "Glass Houses",               "Mystery",     118, "PG-13", "2024-08-02"),
    ("tt2024016", "Ravenwood",                  "Horror",      104, "R",     "2024-08-16"),
    ("tt2024017", "After the Storm",            "Documentary",  98, "PG",    "2024-08-30"),
    ("tt2024018", "The Long Goodbye, Again",    "Drama",       132, "R",     "2024-09-13"),
    ("tt2024019", "Spire: The Awakening",       "Sci-Fi",      144, "PG-13", "2024-09-27"),
    ("tt2024020", "Maple Hall: The Reunion",    "Mystery",     122, "PG-13", "2024-10-11"),
    ("tt2024021", "Nocturne 2: Resurrection",   "Horror",      112, "R",     "2024-10-25"),
    ("tt2024022", "Tinsel Town Holidays",       "Comedy",      102, "PG",    "2024-11-08"),
    ("tt2024023", "Ark of the Stars",           "Adventure",   156, "PG-13", "2024-11-22"),
    ("tt2024024", "Whiteout",                   "Drama",       128, "R",     "2024-12-06"),
    ("tt2024025", "Star Bound: Frontiers",      "Sci-Fi",      152, "PG-13", "2024-12-13"),
    ("tt2024026", "Pawty in the City",          "Animation",    88, "G",     "2024-12-20"),
    ("tt2024027", "Heist Royale",               "Action",      124, "PG-13", "2024-04-26"),
    ("tt2024028", "The Listener",               "Thriller",    118, "R",     "2024-07-26"),
    ("tt2024029", "Moonlight Mariposa",         "Romance",     105, "PG-13", "2024-09-06"),
    ("tt2024030", "Edge of the World",          "Documentary", 102, "PG",    "2024-11-01"),
    # 2025
    ("tt2025001", "Galactic Drift",             "Sci-Fi",      128, "PG-13", "2025-06-13"),
    ("tt2025002", "The Last Lighthouse",        "Drama",       112, "PG",    "2025-07-04"),
    ("tt2025003", "Midnight Heist",             "Thriller",    119, "R",     "2025-07-18"),
    ("tt2025004", "Paws of Fury 2",             "Animation",    95, "PG",    "2025-08-01"),
    ("tt2025005", "Quantum Detective",          "Sci-Fi",      134, "PG-13", "2025-08-15"),
    ("tt2025006", "Echoes of Tomorrow",         "Drama",       108, "PG-13", "2025-09-05"),
    ("tt2025007", "Velocity",                   "Action",      121, "PG-13", "2025-09-19"),
    ("tt2025008", "The Quiet Room",             "Horror",       97, "R",     "2025-10-03"),
    ("tt2025009", "Sunlit",                     "Romance",     104, "PG",    "2025-10-17"),
    ("tt2025010", "Ironclad Legacy",            "Action",      140, "PG-13", "2025-10-31"),
    ("tt2025011", "Recipe for Disaster",        "Comedy",       99, "PG-13", "2025-11-14"),
    ("tt2025012", "Northern Lights",            "Documentary",  88, "G",     "2025-11-28"),
    ("tt2025013", "The Code Breaker",           "Thriller",    132, "PG-13", "2025-01-17"),
    ("tt2025014", "Cinder & Steel",             "Action",      138, "PG-13", "2025-01-31"),
    ("tt2025015", "Roadtrip Royale",            "Comedy",      102, "R",     "2025-02-14"),
    ("tt2025016", "Saltwater",                  "Drama",       122, "R",     "2025-02-28"),
    ("tt2025017", "Galaxy's Edge",              "Sci-Fi",      144, "PG-13", "2025-03-14"),
    ("tt2025018", "Hollow Hills",               "Horror",       98, "R",     "2025-03-28"),
    ("tt2025019", "Quartz",                     "Adventure",   134, "PG-13", "2025-04-11"),
    ("tt2025020", "The Long Drive",             "Drama",       116, "R",     "2025-04-25"),
    ("tt2025021", "Pixel Pals",                 "Animation",    92, "G",     "2025-05-09"),
    ("tt2025022", "Heartbeats",                 "Romance",     105, "PG-13", "2025-05-23"),
    ("tt2025023", "Stormrunner",                "Action",      132, "PG-13", "2025-06-06"),
    ("tt2025024", "City of Glass",              "Mystery",     124, "PG-13", "2025-06-20"),
    ("tt2025025", "Echo Bay",                   "Thriller",    116, "R",     "2025-07-11"),
    ("tt2025026", "The Cartographer",           "Drama",       138, "PG-13", "2025-07-25"),
    ("tt2025027", "Mountain's Shadow",          "Western",     142, "R",     "2025-08-08"),
    ("tt2025028", "Wavebreaker",                "Adventure",   128, "PG",    "2025-08-22"),
    ("tt2025029", "The Cipher",                 "Thriller",    118, "R",     "2025-08-29"),
    ("tt2025030", "October Country",            "Horror",      102, "R",     "2025-09-12"),
    ("tt2025031", "Tinsel Town Christmas",      "Comedy",      105, "PG",    "2025-12-05"),
    ("tt2025032", "Snowpoint",                  "Drama",       122, "R",     "2025-12-12"),
    ("tt2025033", "Star Bound: Origins",        "Sci-Fi",      148, "PG-13", "2025-12-19"),
    ("tt2025034", "Holiday Pawty",              "Animation",    88, "G",     "2025-12-26"),
    ("tt2025035", "The Dispatcher",             "Thriller",    124, "R",     "2025-10-24"),
    # 2026
    ("tt2026001", "After the Eclipse",          "Drama",       124, "R",     "2026-01-09"),
    ("tt2026002", "Rebel Angel",                "Action",      138, "PG-13", "2026-01-23"),
    ("tt2026003", "Stardust Express",           "Sci-Fi",      142, "PG-13", "2026-02-06"),
    ("tt2026004", "Wedding Crashers Reunion",   "Comedy",      105, "R",     "2026-02-13"),
    ("tt2026005", "The Last Page",              "Drama",       122, "PG-13", "2026-02-27"),
    ("tt2026006", "Snowmelt",                   "Drama",       118, "R",     "2026-03-13"),
    ("tt2026007", "Beyond Pluto",               "Sci-Fi",      148, "PG-13", "2026-03-27"),
    ("tt2026008", "Whirlwind",                  "Animation",    95, "PG",    "2026-04-10"),
    ("tt2026009", "Phoenix Rising",             "Action",      134, "PG-13", "2026-04-17"),
    ("tt2026010", "Quiet Storm",                "Thriller",    128, "R",     "2026-04-24"),
    ("tt2026011", "Carolina Heat",              "Romance",     102, "PG-13", "2026-04-03"),
    ("tt2026012", "The Glassblower",            "Drama",       128, "PG-13", "2026-02-20"),
    ("tt2026013", "Ravenwood: Returns",         "Horror",      108, "R",     "2026-03-06"),
    ("tt2026014", "Mile-High Mayhem 2",         "Action",      119, "PG-13", "2026-03-20"),
    ("tt2026015", "The Cartographer's Atlas",   "Drama",       136, "PG-13", "2026-04-24"),
]

assert len(MOVIES) == 100, f"expected 100 movies, got {len(MOVIES)}"

MOVIE_SCHEMA = StructType([
    StructField("imdb_id", StringType(), nullable=False),
    StructField("title", StringType(), nullable=False),
    StructField("genre", StringType(), nullable=False),
    StructField("runtime_minutes", IntegerType(), nullable=False),
    StructField("mpaa_rating", StringType(), nullable=False),
    StructField("release_date", StringType(), nullable=False),
    StructField("imdb_rating", DoubleType(), nullable=False),
    StructField("metascore", IntegerType(), nullable=False),
])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def build_movie_rows(rng: random.Random) -> list[Row]:
    rows: list[Row] = []
    for imdb_id, title, genre, runtime, mpaa, release in MOVIES:
        rows.append(Row(
            imdb_id=imdb_id,
            title=title,
            genre=genre,
            runtime_minutes=runtime,
            mpaa_rating=mpaa,
            release_date=release,
            imdb_rating=round(rng.uniform(5.5, 9.0), 1),
            metascore=rng.randint(40, 95),
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

    target_table = f"{args.catalog}.{args.schema}.imdb_movie_metadata"
    logger.info("Generating IMDB movie metadata (%d movies, seed=%d)", len(MOVIES), args.seed)
    logger.info("Writing: %s", target_table)

    spark.sql(f"CREATE CATALOG IF NOT EXISTS {args.catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.schema}")

    rows = build_movie_rows(rng)
    df = spark.createDataFrame(rows, schema=MOVIE_SCHEMA)

    (df.write
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(target_table))

    row_count = spark.table(target_table).count()
    logger.info("Loaded %s rows into %s", f"{row_count:,}", target_table)


if __name__ == "__main__":
    main()
