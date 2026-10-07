from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window

# Trip timestamps are naive NYC wall-clock values. Pin the session zone so
# Spark does no DST arithmetic on them, and so every script in the pipeline
# reads back exactly what the previous one wrote. See 03 for the full reason.
spark = (
    SparkSession.builder.appName("window_functions")
    .master("local[*]")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

df = spark.read.parquet("data/cleaned_trips")

daily = df.groupBy("pickup_date").agg(
    f.count("*").alias("trips_amount"),
)   

# 1. Day-over-day percentage change in daily trip count (lag)
window = Window.orderBy("pickup_date")

trips_lag = daily.withColumn("prev_day", f.lag("trips_amount").over(window))
trips_lag = trips_lag.withColumn(
    "change_in_trips_amount(%)",
    f.round((f.col("trips_amount") / f.col("prev_day") - 1) * 100, 2),
)

# 2. 7-day rolling average of daily trips (rowsBetween)
# Trailing window - today plus the six days before it
windowWeek = Window.orderBy("pickup_date").rowsBetween(-6, 0)

trips_week_avg = daily.withColumn("avg_trips_weekly", f.round(f.avg("trips_amount").over(windowWeek)))

# 3. Top 3 busiest hours within each day (dense_rank).
hourly = df.groupBy("pickup_date", f.hour("tpep_pickup_datetime").alias("pickup_hour")).agg(
    f.count("*").alias("trips_per_hour")
)
w = Window.partitionBy("pickup_date").orderBy(f.col("trips_per_hour").desc())
busiest_hours = hourly.withColumn("d_rank", f.dense_rank().over(w)).filter(f.col("d_rank") < 4).sort("pickup_date")

#Print the results
print("Day-over-day percentage change in daily trip count:")
trips_lag.show()
print("7-day rolling average of daily trips:")
trips_week_avg.show()
print("Top 3 busiest hours within each day:")
busiest_hours.show(n=21)
