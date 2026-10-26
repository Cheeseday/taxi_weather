from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

from pathlib import Path


FIGURES = Path(__file__).resolve().parent / "figures"


def save_figure(name):
    """Write the current figure to figures/<name>.png, then show it.

    savefig has to run before show(): several matplotlib backends clear the
    figure when it is shown, which would leave an empty file behind.
    """
    FIGURES.mkdir(exist_ok=True)
    plt.savefig(FIGURES / f"{name}.png", dpi=120, bbox_inches="tight")
    plt.show()


spark = (
    SparkSession.builder.appName("taxi_weather")
    .master("local[*]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

df = spark.read.parquet("data/cleaned_trips")

# Trips and total revenue aggregated per day, per hour of day, and per day of week.
#
# Everything keyed on pickup_date is computed in ONE aggregation - counts, revenue
# and the daily averages below. The frame is cached because it feeds three tables.
by_day = df.groupBy("pickup_date").agg(
    f.count("*").alias("trips_per_day"),
    f.sum("total_amount").alias("revenue_sum"),  # unrounded: by_day_of_week re-adds it
    f.round(f.avg("fare_amount"), 1).alias("avg_fare_amount"),
    f.round(f.avg("trip_distance"), 1).alias("avg_trip_distance"),
    f.round(f.avg("duration_min"), 1).alias("avg_duration_in_min"),
    f.round(f.avg("speed_mph"), 1).alias("avg_speed"),
).sort("pickup_date").cache()

print("By day:")
by_day.show()

daily = by_day.select("pickup_date", "trips_per_day", f.round("revenue_sum").alias("revenue_per_day"))

# Cached as well: this frame is shown below and then fed to the chart. Without it
# toPandas() would recompute the whole aggregation from the Parquet files.
#
# hour_of_day (0-23) is read off the stored pickup_hour column
hourly = df.groupBy(f.hour("pickup_hour").alias("hour_of_day")).agg(
    f.count("*").alias("trips_per_hour"),
    f.round(f.sum("total_amount")).alias("revenue_per_hour")
).sort("hour_of_day").cache()

# Built from the daily rows above instead of the trips: counts and revenue sums
# add up across days, so summing each weekday's dates gives the same totals with
# no second pass over the data. Revenue is rounded once, after this final sum -
# adding up already-rounded daily figures would carry each day's rounding error.
by_day_of_week = by_day.groupBy(f.date_format("pickup_date", "EEEE").alias("day_of_week")).agg(
    f.sum("trips_per_day").alias("trips_per_day_of_week"),
    f.round(f.sum("revenue_sum")).alias("revenue_per_day_of_week")
).sort("revenue_per_day_of_week", ascending=False)

print("Trips and revenue per day:")
daily.show()
print("Trips and revenue per hour of a day:")
hourly.show(n=24)
print("Trips and revenue per day of week:")
by_day_of_week.show()

# Average fare, distance, duration, and speed per day - already computed above
averages = by_day.select(
    "pickup_date", "avg_fare_amount", "avg_trip_distance", "avg_duration_in_min", "avg_speed"
)

print("Average fare amount, distance, duration in minutes and speed (mph) per day:")
averages.show()


# Chart for revenue and trip volume by the hour of day
data = hourly.toPandas()

fig, ax = plt.subplots(figsize=(9, 5))   # wide enough for 24 two-digit hour labels
fig.suptitle("Total revenue and trip volume by the hour of day")
twin1 = ax.twinx()

x = data["hour_of_day"]

p1, = ax.plot(x, data["revenue_per_hour"], "8--m", label="Revenue", linewidth=2, ms=8)
p2 = twin1.bar(x, data["trips_per_hour"], label="Trips")

# Both axes start at zero, with 15% headroom on top for the legend.
in_millions = FuncFormatter(lambda value, _: f"{value / 1e6:g}")
ax.set(ylim=(0, data["revenue_per_hour"].max() * 1.15),
       xlabel="Hour of day", ylabel="Total revenue (million $)")
twin1.set(ylim=(0, data["trips_per_hour"].max() * 1.15), ylabel="Number of trips (millions)")
ax.yaxis.set_major_formatter(in_millions)
twin1.yaxis.set_major_formatter(in_millions)
ax.set_xticks(x)

# The twin axis is drawn last, so its bars would cover the revenue line.
# Raise the line's axis above it and make its background transparent.
ax.set_zorder(twin1.get_zorder() + 1)
ax.patch.set_visible(False)

ax.legend(handles=[p1, p2], loc="upper left")

save_figure("revenue_and_trips_by_hour")

by_day.unpersist()
hourly.unpersist()
spark.stop()
