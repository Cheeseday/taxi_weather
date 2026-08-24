from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("taxi_weather").master("local[*]").getOrCreate()

df = spark.read.parquet(
            "/home/ales/Documents/taxi_weather/data/yellow_tripdata_2024-01.parquet",
            "/home/ales/Documents/taxi_weather/data/yellow_tripdata_2024-02.parquet",
            "/home/ales/Documents/taxi_weather/data/yellow_tripdata_2024-03.parquet"
)

duration_sec = f.unix_timestamp("tpep_dropoff_datetime") - f.unix_timestamp("tpep_pickup_datetime")

# Drop lines with missing value for essential columns. It solves potential problem 
# when comparisons across columns stop being apples-to-apples (for grouping functions).
df_cleaned = df.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "fare_amount", "total_amount"])

# Rules for filtering data 
rules = {
    # Only three months was chosed for analysis - every other trip should be excluded
    "pickup_outside_q1":            (f.col("tpep_pickup_datetime").cast("timestamp") < "2024-01-01 00:00:00") | 
                                        (f.col("tpep_pickup_datetime") >= "2024-04-01"),
    # It doesn't make sense if a dropoff take place before a pickup
    "dropoff_not_after_pickup":     f.col("tpep_pickup_datetime") >= f.col("tpep_dropoff_datetime"),
    # Trips that lasts more than 6 hours usually ends beyond NYC borders
    "duration_over_6h":             duration_sec > 6 * 3600,
    # Trips with non-positive distance are broken
    "bad_trip_distance":            (f.col("trip_distance") <= 0) | (f.col("trip_distance") >= 100),
    "non_positive_fare":            f.col("fare_amount") <= 0, # For broken values and for zero-division error guard
    "non_positive_total":           f.col("total_amount") <= 0, # For broken values and for zero-division error guard
}

# Create a report for how many rows every rule removes
report = [f.count("*").alias("rows_total")]
report += [f.sum(f.when(c, 1).otherwise(0)).alias(r) for r, c in rules.items()]

reject = f.lit(False)
for cond in rules.values():
    reject = reject | cond

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

# Print the results
print("DataFrame schema:")
df_cleaned_extended.printSchema()
stats.show(vertical=True)
print(f"Total row count after cleaning: {df_cleaned_extended.count()}")

# Save partitioned data to parquet
df_cleaned_extended.repartition("pickup_date").write \
    .partitionBy("pickup_date") \
    .mode("overwrite") \
    .parquet("data/cleaned_trips")

spark.stop()
