"""Ingest: POS Ticket Sales.

Generates POS ticket sales deterministically (seed=42) and writes them to the
pos_ticket_sales table. Reads theater_layout and imdb_movie_metadata from the
configured catalog/schema for FK consistency.

Mixed POS sources -- pos_v3_streaming, pos_v2_batch, kiosk_v1_batch -- each with
its own data-quality profile. Each theater is assigned a primary POS system, so
issues cluster by location rather than spreading evenly. The transform layer
does NOT clean these issues; they surface in the dashboard. That is the point.

Run via DABs (depends on theater_layout and imdb_movie_metadata):
    databricks bundle run -t dev seed_theater_data
"""

from __future__ import annotations

import argparse
import logging
import random
from datetime import datetime, timedelta

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


logger = logging.getLogger("fde_theater.ingest_pos_ticket_sales")


POS_SYSTEMS: list[str] = ["pos_v3_streaming", "pos_v2_batch", "kiosk_v1_batch"]
POS_SYSTEM_WEIGHTS: list[int] = [55, 30, 15]

# Data-quality profiles per POS system. Rates are fractions of that system's rows.
# pos_v3_streaming is the modern path (clean). kiosk_v1_batch is the legacy
# kiosk pipeline (worst). pos_v2_batch sits in between -- it ships timestamps as
# strings in the legacy "%m/%d/%Y %I:%M %p" format and has occasional batch dupes.
DQ_PROFILES: dict[str, dict[str, float]] = {
    "pos_v3_streaming": {
        "duplicate_txn_rate":  0.005,
        "null_screen_rate":    0.0,
        "null_showtime_rate":  0.0,
        "bad_price_rate":      0.0,
        "bad_timestamp_rate":  0.0,
        "orphan_theater_rate": 0.0,
        "orphan_movie_rate":   0.001,
        "format_mismatch_rate": 0.001,
    },
    "pos_v2_batch": {
        "duplicate_txn_rate":  0.012,
        "null_screen_rate":    0.005,
        "null_showtime_rate":  0.020,
        "bad_price_rate":      0.005,
        "bad_timestamp_rate":  0.030,
        "orphan_theater_rate": 0.0,
        "orphan_movie_rate":   0.005,
        "format_mismatch_rate": 0.006,
    },
    "kiosk_v1_batch": {
        "duplicate_txn_rate":  0.045,
        "null_screen_rate":    0.040,
        "null_showtime_rate":  0.030,
        "bad_price_rate":      0.040,
        "bad_timestamp_rate":  0.025,
        "orphan_theater_rate": 0.060,
        "orphan_movie_rate":   0.040,
        "format_mismatch_rate": 0.030,
    },
}

# Genres that get an IMAX release in this catalog. Pricing for IMAX adds a
# premium on top of the ticket_type base price.
IMAX_ELIGIBLE_GENRES: set[str] = {"Action", "Sci-Fi", "Adventure"}
IMAX_PREMIUM: float = 5.00

# Probability that an IMAX-eligible movie on an IMAX-capable screen is sold as
# the IMAX format (vs. a Standard show on the same screen).
IMAX_TAKEUP_RATE: float = 0.55

TICKET_TYPES: list[tuple[str, float]] = [
    ("Standard", 14.50),
    ("Senior",    9.50),
    ("Child",     8.00),
    ("Matinee",  10.50),
    ("Premium",  19.00),
]

PAYMENT_METHODS: list[str] = [
    "credit_card", "debit_card", "mobile_pay", "cash", "gift_card",
]

ORPHAN_THEATER_IDS: list[str] = ["T099", "T999", "T-OLD"]
ORPHAN_MOVIE_IDS: list[str] = ["tt9999999", "tt8888888", "UNKNOWN"]

POS_SCHEMA = StructType([
    StructField("transaction_id",     StringType(),  nullable=False),
    StructField("theater_id",         StringType(),  nullable=False),
    StructField("screen_id",          StringType(),  nullable=True),
    StructField("movie_id",           StringType(),  nullable=False),
    StructField("format",             StringType(),  nullable=False),
    StructField("showtime",           StringType(),  nullable=True),
    StructField("ticket_type",        StringType(),  nullable=False),
    StructField("ticket_price",       DoubleType(),  nullable=False),
    StructField("seats_sold",         IntegerType(), nullable=False),
    StructField("purchase_timestamp", StringType(),  nullable=False),
    StructField("payment_method",     StringType(),  nullable=False),
    StructField("pos_source",         StringType(),  nullable=False),
])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument(
        "--start-date",
        default="2026-03-01",
        help="ISO date for the first day of generated sales (inclusive).",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days of sales to generate.",
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def load_theater_screens(
    spark: SparkSession,
    catalog: str,
    schema: str,
) -> dict[str, list[tuple[str, bool]]]:
    """Returns {theater_id: [(screen_id, imax_capable), ...]} sorted for determinism."""
    df = (
        spark.table(f"{catalog}.{schema}.theater_layout")
             .select("theater_id", "screen_id", "imax_capable")
             .orderBy("theater_id", "screen_id")
    )
    out: dict[str, list[tuple[str, bool]]] = {}
    for r in df.collect():
        out.setdefault(r.theater_id, []).append((r.screen_id, bool(r.imax_capable)))
    return out


def load_movie_catalog(
    spark: SparkSession,
    catalog: str,
    schema: str,
) -> list[tuple[str, str]]:
    """Returns [(imdb_id, genre), ...] sorted for determinism."""
    df = (
        spark.table(f"{catalog}.{schema}.imdb_movie_metadata")
             .select("imdb_id", "genre")
             .orderBy("imdb_id")
    )
    return [(r.imdb_id, r.genre) for r in df.collect()]


def assign_pos_systems(
    rng: random.Random,
    theater_ids: list[str],
) -> dict[str, str]:
    """Each theater gets one primary POS system, so DQ issues cluster by location."""
    return {
        tid: rng.choices(POS_SYSTEMS, weights=POS_SYSTEM_WEIGHTS)[0]
        for tid in sorted(theater_ids)
    }


def build_transactions(
    rng: random.Random,
    theater_screens: dict[str, list[tuple[str, bool]]],
    movies: list[tuple[str, str]],
    theater_pos_system: dict[str, str],
    start: datetime,
    days: int,
) -> list[dict]:
    """Generate clean, FK-consistent transactions before DQ injection."""
    movie_ids = [m for m, _ in movies]
    imax_eligible_movies = [m for m, g in movies if g in IMAX_ELIGIBLE_GENRES]

    rows: list[dict] = []
    txn_counter = 1_000_000

    for theater_id in sorted(theater_screens.keys()):
        screens = theater_screens[theater_id]
        screen_count = len(screens)
        pos_source = theater_pos_system[theater_id]

        for day_offset in range(days):
            day = start + timedelta(days=day_offset)
            day_factor = 1.6 if day.weekday() >= 5 else 1.0
            base = max(2, screen_count // 2)
            n_txns = max(1, int(rng.randint(base, base * 3) * day_factor))

            for _ in range(n_txns):
                txn_counter += 1
                transaction_id = f"TXN{txn_counter}"
                screen_id, imax_capable = rng.choice(screens)

                # Decide format: IMAX only when both movie and screen support it.
                if imax_capable and rng.random() < IMAX_TAKEUP_RATE and imax_eligible_movies:
                    movie_id = rng.choice(imax_eligible_movies)
                    fmt = "IMAX"
                else:
                    movie_id = rng.choice(movie_ids)
                    fmt = "Standard"

                showtime_hour = rng.choice([10, 13, 16, 19, 21, 22])
                showtime = day.replace(
                    hour=showtime_hour,
                    minute=rng.choice([0, 15, 30, 45]),
                )
                ticket_type, base_price = rng.choice(TICKET_TYPES)
                price = base_price + (IMAX_PREMIUM if fmt == "IMAX" else 0.0)
                seats_sold = rng.choices(
                    [1, 2, 3, 4, 5, 6],
                    weights=[15, 40, 15, 15, 8, 7],
                )[0]
                purchase_ts = showtime - timedelta(minutes=rng.randint(5, 240))
                payment = rng.choice(PAYMENT_METHODS)

                rows.append({
                    "transaction_id":     transaction_id,
                    "theater_id":         theater_id,
                    "screen_id":          screen_id,
                    "movie_id":           movie_id,
                    "format":             fmt,
                    "showtime":           showtime.isoformat(),
                    "ticket_type":        ticket_type,
                    "ticket_price":       price,
                    "seats_sold":         seats_sold,
                    "purchase_timestamp": purchase_ts.isoformat(),
                    "payment_method":     payment,
                    "pos_source":         pos_source,
                })

    return rows


def inject_dq_issues(
    rng: random.Random,
    rows: list[dict],
    theater_screens: dict[str, list[tuple[str, bool]]],
) -> None:
    """Mutate rows in place with DQ issues per POS-source profile."""
    by_source: dict[str, list[int]] = {s: [] for s in POS_SYSTEMS}
    for idx, row in enumerate(rows):
        by_source[row["pos_source"]].append(idx)

    non_imax_screens_by_theater = {
        tid: [s for s, capable in screens if not capable]
        for tid, screens in theater_screens.items()
    }

    for source, indices in by_source.items():
        if not indices:
            continue
        profile = DQ_PROFILES[source]
        n_total = len(indices)

        def sample(rate: float) -> list[int]:
            n = int(n_total * rate)
            return rng.sample(indices, n) if n > 0 else []

        for i in sample(profile["orphan_theater_rate"]):
            rows[i]["theater_id"] = rng.choice(ORPHAN_THEATER_IDS)

        for i in sample(profile["duplicate_txn_rate"]):
            dup_src = rng.choice(rows)
            rows[i]["transaction_id"] = dup_src["transaction_id"]

        for i in sample(profile["bad_price_rate"]):
            rows[i]["ticket_price"] = rng.choice([0.0, -5.00, -1.00])

        for i in sample(profile["null_screen_rate"]):
            rows[i]["screen_id"] = None

        for i in sample(profile["null_showtime_rate"]):
            rows[i]["showtime"] = None

        for i in sample(profile["orphan_movie_rate"]):
            rows[i]["movie_id"] = rng.choice(ORPHAN_MOVIE_IDS)

        for i in sample(profile["bad_timestamp_rate"]):
            ts = rows[i]["purchase_timestamp"]
            try:
                parsed = datetime.fromisoformat(ts)
            except (TypeError, ValueError):
                continue
            rows[i]["purchase_timestamp"] = parsed.strftime("%m/%d/%Y %I:%M %p")

        # Format mismatch: claim IMAX on a non-IMAX-capable screen at the same
        # theater. Only applied where there is at least one non-IMAX screen.
        for i in sample(profile["format_mismatch_rate"]):
            tid = rows[i]["theater_id"]
            non_imax = non_imax_screens_by_theater.get(tid, [])
            if not non_imax:
                continue
            rows[i]["screen_id"] = rng.choice(non_imax)
            rows[i]["format"] = "IMAX"
            # Price keeps the IMAX premium -- the bug is the screen, not the price.


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    spark = SparkSession.builder.getOrCreate()
    rng = random.Random(args.seed)

    target_table = f"{args.catalog}.{args.schema}.pos_ticket_sales"
    logger.info("Reading theater_layout and imdb_movie_metadata for FK lookups")
    theater_screens = load_theater_screens(spark, args.catalog, args.schema)
    movies = load_movie_catalog(spark, args.catalog, args.schema)
    logger.info(
        "Loaded %d theaters (%d screens) and %d movies",
        len(theater_screens),
        sum(len(v) for v in theater_screens.values()),
        len(movies),
    )

    theater_pos_system = assign_pos_systems(rng, list(theater_screens.keys()))
    pos_rollup: dict[str, int] = {s: 0 for s in POS_SYSTEMS}
    for src in theater_pos_system.values():
        pos_rollup[src] += 1
    logger.info("Theaters per POS system: %s", pos_rollup)

    start = datetime.fromisoformat(args.start_date)
    logger.info(
        "Generating transactions: start=%s, days=%d, seed=%d",
        args.start_date, args.days, args.seed,
    )
    rows = build_transactions(
        rng=rng,
        theater_screens=theater_screens,
        movies=movies,
        theater_pos_system=theater_pos_system,
        start=start,
        days=args.days,
    )
    logger.info("Built %s clean transactions", f"{len(rows):,}")

    inject_dq_issues(rng, rows, theater_screens)
    logger.info("Injected per-source DQ issues")

    df = spark.createDataFrame(rows, schema=POS_SCHEMA)

    (df.write
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(target_table))

    row_count = spark.table(target_table).count()
    logger.info("Loaded %s rows into %s", f"{row_count:,}", target_table)


if __name__ == "__main__":
    main()
