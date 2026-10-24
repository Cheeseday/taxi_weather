from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as f

DATA_DIR = Path(__file__).resolve().parent / "data"


def first_day_of_next_month(day):
    """2024-03-01 -> 2024-04-01, and 2024-12-01 -> 2025-01-01."""
    return date(day.year + day.month // 12, day.month % 12 + 1, 1)


def discover_months(data_dir):
    """Monthly trip files in data_dir, oldest first, as (path, first day of month).

    The pattern only matches finished downloads: 00_download.py writes to a
    ".part" file and renames it when complete, so a half-written file never
    ends in ".parquet". The ????-?? shape also guarantees zero-padded months,
    which is what makes sorting the names sort them by date.
    """
    found = []
    for path in sorted(data_dir.glob("yellow_tripdata_????-??.parquet")):
        year, month = path.stem.removeprefix("yellow_tripdata_").split("-")
        found.append((path, date(int(year), int(month), 1)))
    if not found:
        raise SystemExit(
            f"No yellow_tripdata_YYYY-MM.parquet files in {data_dir} - run 00_download.py first."
        )
    return found


def analysis_period(month_starts):
    """Half-open [start, end) covering every loaded month.

    The bounds come from the file names, never from the data: the data's own
    earliest and latest pickups are exactly the outliers this period exists to
    reject. A gap between months is refused rather than bridged, because the
    7-day rolling average in 05 counts rows, not calendar days, and would
    silently average across the hole.
    """
    for previous, current in zip(month_starts, month_starts[1:]):
        expected = first_day_of_next_month(previous)
        if current != expected:
            raise SystemExit(
                f"Monthly files are not consecutive: {expected:%Y-%m} is missing "
                f"before {current:%Y-%m}."
            )
    return month_starts[0], first_day_of_next_month(month_starts[-1])


# Work out what to clean before starting Spark, so a missing download fails in
# a second rather than after the JVM has spun up.
raw_months = discover_months(DATA_DIR)
period_start, period_end = analysis_period([month for _, month in raw_months])
print(
    f"Cleaning {len(raw_months)} monthly file(s): pickups from {period_start} "
    f"up to, but not including, {period_end}"
)

# Trip timestamps are naive NYC wall-clock values. Pin the session zone so
# Spark does no DST arithmetic on them, and so every script in the pipeline
# reads back exactly what the previous one wrote. See 03 for the full reason.
spark = (
    SparkSession.builder.appName("taxi_weather")
    .master("local[*]")
    .config("spark.sql.session.timeZone", "UTC")
    # Set spark heap 4 GB  
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

df = spark.read.parquet(*(str(path) for path, _ in raw_months))

duration_sec = f.unix_timestamp("tpep_dropoff_datetime") - f.unix_timestamp("tpep_pickup_datetime")

# Drop lines with missing value for essential columns. It solves potential problem 
# when comparisons across columns stop being apples-to-apples (for grouping functions).
df_cleaned = df.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "fare_amount", "total_amount"])

# Rules for filtering data 
rules = {
    # Trips must fall inside the loaded months - every other trip is excluded.
    # The upper bound is exclusive (the first day after the last month), so no
    # "23:59:59.999999" end point is needed.
    "pickup_outside_period":        (f.col("tpep_pickup_datetime") < period_start.isoformat()) |
                                        (f.col("tpep_pickup_datetime") >= period_end.isoformat()),
    # It doesn't make sense if a dropoff take place before a pickup
    "dropoff_not_after_pickup":     f.col("tpep_pickup_datetime") >= f.col("tpep_dropoff_datetime"),
    # Trips that lasts more than 6 hours usually ends beyond NYC borders
    "duration_over_6h":             duration_sec > 6 * 3600,
    # Trips with non-positive distance are broken
    "bad_trip_distance":            (f.col("trip_distance") <= 0) | (f.col("trip_distance") >= 100),
    "non_positive_fare":            f.col("fare_amount") <= 0, # For broken values and for zero-division error guard
    "non_positive_total":           f.col("total_amount") <= 0, # For broken values and for zero-division error guard
}

reject = f.lit(False)
for cond in rules.values():
    reject = reject | cond

# Report how many rows each rule removes. rows_kept is folded in here on purpose:
# counting the cleaned frame separately would mean a second full pass over the
# raw Parquet for a number this same pass can already produce.
report = [f.count("*").alias("rows_total")]
report += [f.sum(f.when(c, 1).otherwise(0)).alias(r) for r, c in rules.items()]
report.append(f.sum(f.when(reject, 0).otherwise(1)).alias("rows_kept"))

stats = df_cleaned.select(report)

df_cleaned = df_cleaned.filter(~reject)

# Adding new columns for analysis part
df_cleaned_extended = df_cleaned.withColumns({
    "duration_min": f.round(duration_sec / 60 , 2),
    "tip_pct":      f.round(f.col("tip_amount") / f.col("fare_amount") * 100, 2),
    "pickup_date":  f.to_date("tpep_pickup_datetime"),
    "pickup_hour":  f.date_trunc("hour", "tpep_pickup_datetime"),
    "speed_mph":    f.round(f.col("trip_distance") / (duration_sec / 3600), 2)
})

# Leave only useful columns, but raw Parquet still has the rest ones if needed
KEPT_COLUMNS = [
    "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "passenger_count", "trip_distance",
    "PULocationID", "DOLocationID", "payment_type",
    "fare_amount", "tip_amount", "total_amount", "congestion_surcharge",
    "duration_min", "tip_pct", "pickup_date", "pickup_hour", "speed_mph",
]

df_output = df_cleaned_extended.select(*KEPT_COLUMNS)

# Print the results. One .first() materialises the whole report, so the counts
# below cost a single pass rather than one per line.
print("Schema of the cleaned output:")
df_output.printSchema()

summary = stats.first()
print(f"\nRows before cleaning: {summary['rows_total']:,}")
print("Rows matching each rule (a row can match several, so these overlap):")
for rule in rules:
    print(f"    {rule:28} {summary[rule]:>9,}")
print(f"Total row count after cleaning: {summary['rows_kept']:,}")

# repartition("pickup_date") costs one shuffle but lands each date in a single
# file. Writing without it leaves about twice as many files (176 instead of 91
# for Q1), which every downstream read then pays for. mode("overwrite") replaces
# the whole directory (Spark's default static partition overwrite), so dates
# left over from an earlier, longer period don't survive a run over fewer months.
df_output.repartition("pickup_date").write \
    .partitionBy("pickup_date") \
    .mode("overwrite") \
    .parquet(str(DATA_DIR / "cleaned_trips"))

spark.stop()
