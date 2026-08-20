from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("taxi_weather").master("local[*]").getOrCreate()

df = spark.read.parquet("data/cleaned_trips")

# Trips and total revenue per day, per hour of day, and per day of week
df_per_day = df.groupBy("pickup_date")
trips_per_day = df_per_day.agg(f.count("tpep_pickup_datetime").alias("trips_per_day")) \
                            .sort("pickup_date")
revenue_per_day = df_per_day.agg(f.round(f.sum("total_amount")).alias("revenue_per_day")) \
                            .sort("pickup_date")

df_per_hour = df.groupBy(f.hour("tpep_pickup_datetime").alias("pickup_per_hour_of_day"))
trips_per_hour = df_per_hour.agg(f.count("tpep_pickup_datetime").alias("trips_per_hour")) \
                             .sort("pickup_per_hour_of_day")
revenue_per_hour = df_per_hour.agg(f.round(f.sum("total_amount")).alias("revenue_per_hour")) \
                               .sort("pickup_per_hour_of_day")

df_per_day_of_week = df.groupBy(f.date_format("tpep_pickup_datetime", "EEEE").alias("day_of_week"))
trips_pey_day_of_week = df_per_day_of_week.agg(f.count("tpep_pickup_datetime").alias("trips_per_day_of_week")) \
                                            .sort("trips_per_day_of_week", ascending=False)
revenue_per_day_of_week = df_per_day_of_week.agg(f.round(f.sum("total_amount")).alias("revenue_per_day_of_week")) \
                                            .sort("revenue_per_day_of_week", ascending=False)


print("Trips per day:")
trips_per_day.show(n=12)
print("Revenue per day:")
revenue_per_day.show(n=12)
print("Trips per hour of a day:")
trips_per_hour.show(n=24)
print("Revenue per hour of a day:")
revenue_per_hour.show(n=24)
print("Trips per day of week:")
trips_pey_day_of_week.show()
print("Revenue per day of week:")
revenue_per_day_of_week.show()

# Average fare, distance, duration, and speed per day
fare_avg = df_per_day.agg(f.round(f.avg("fare_amount"), 1).alias("avg_fare_amount")) \
                    .sort("pickup_date")
distance_avg = df_per_day.agg(f.round(f.avg("trip_distance"), 1).alias("avg_trip_distance")) \
                    .sort("pickup_date")
duration_avg = df_per_day.agg(f.round(f.avg("duration_min"), 1).alias("avg_duration_in_min")) \
                    .sort("pickup_date")
speed_avg = df_per_day.agg(f.round(f.avg("speed_mph"), 1).alias("avg_speed")).sort("pickup_date")

print("Average fare amount per day:")
fare_avg.show(n=12)
print("Average distance per day:")
distance_avg.show(n=12)
print("Average trip duration per day (in minutes):")
duration_avg.show(n=12)
print("Average speed per day (mph):")
speed_avg.show(n=12)

