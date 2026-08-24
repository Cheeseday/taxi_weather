from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("taxi_weather").master("local[*]").getOrCreate()

df = spark.read.parquet("data/cleaned_trips")

# Trips and total revenue per day, per hour of day, and per day of week
daily = df.groupBy("pickup_date").agg(
    f.count("*").alias("trips_per_day"),
    f.round(f.sum("total_amount")).alias("revenue_per_day")
).sort("pickup_date")

hourly = df.groupBy(f.hour("tpep_pickup_datetime").alias("pickup_hour")).agg(
    f.count("*").alias("trips_per_hour"),
    f.round(f.sum("total_amount")).alias("revenue_per_hour")
).sort("pickup_hour")

by_day_of_week = df.groupBy(f.date_format("tpep_pickup_datetime", "EEEE").alias("day_of_week")).agg(
    f.count("*").alias("trips_per_day_of_week"),
    f.round(f.sum("total_amount")).alias("revenue_per_day_of_week")
).sort("revenue_per_day_of_week", ascending=False)

print("Trips and revenue per day:")
daily.show()
print("Trips and revenue per hour of a day:")
hourly.show(n=24)
print("Trips and revenue per day of week:")
by_day_of_week.show()

# Average fare, distance, duration, and speed per day
averages = df.groupBy("pickup_date").agg(
    f.round(f.avg("fare_amount"), 1).alias("avg_fare_amount"),
    f.round(f.avg("trip_distance"), 1).alias("avg_trip_distance"),
    f.round(f.avg("duration_min"), 1).alias("avg_duration_in_min"),
    f.round(f.avg("speed_mph"), 1).alias("avg_speed"),
).sort("pickup_date")

print("Average fare amount, distance, duration in minutes and speed (mph) per day:")
averages.show()


# Chart for revenue and trip volume by the hour of day
data = hourly.toPandas()

fig, ax = plt.subplots()
fig.suptitle("Average revenue and trip volume by the hour of day")
twin1 = ax.twinx()

x = data["pickup_hour"]

p1, = ax.plot(x, data["revenue_per_hour"], "8--m", label="Revenue", linewidth=2, ms=8)
p2 = twin1.bar(x, data["trips_per_hour"], label="Trips")

ax.set(ylim=(0, 2e7), xlabel="Hour of day", ylabel="Average revenue (1e7)")
twin1.set(ylim=(0, 1e6), ylabel="Average number of trips (1e6)")
ax.set_xticks(x)

ax.legend(handles=[p1, p2])

plt.show()
