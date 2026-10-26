from pyspark.sql import SparkSession
from pyspark.sql import functions as f

import matplotlib.pyplot as plt

from pathlib import Path


FIGURES = Path(__file__).resolve().parent / "figures"
FINDINGS = Path(__file__).resolve().parent / "findings"


def save_figure(name):
    """Write the current figure to figures/<name>.png, then show it.

    savefig has to run before show(): several matplotlib backends clear the
    figure when it is shown, which would leave an empty file behind.
    """
    FIGURES.mkdir(exist_ok=True)
    plt.savefig(FIGURES / f"{name}.png", dpi=120, bbox_inches="tight")
    plt.show()

# Both sides of the weather join are naive NYC wall-clock labels, so Spark must
# do no timezone arithmetic on them. UTC is the only zone with no DST
# transitions, which makes to_timestamp() parse every label literally.
# Under a zone that does shift, the hour lost to spring-forward collapses onto
# the next one and joins twice: Europe/Warsaw loses 2024-03-31 02:00, and
# America/New_York - despite being the data's own zone - loses 2024-03-10 02:00.
spark = (
    SparkSession.builder.appName("weather_enrichment")
    .master("local[*]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

card_payment = 1
tip_pct_threshold = 20000 # bigger tips only appears when the distance is equal 0.01 miles
speed_threshold = 55 # a speed limit for highways in New York
rainy_hour_threshold = 0.5 
    # According to the UK Met Office, the boundary between "light" rain and "moderate" rain

df = spark.read.parquet("data/cleaned_trips")

df = df.filter((f.col("speed_mph") < speed_threshold))

# Tip percentage is only meaningful for card payments - cash tips are not
# recorded, so a cash row carries a structural 0 rather than a real 0.
card_tip = f.when(
    (f.col("payment_type") == card_payment) & (f.col("tip_pct") < tip_pct_threshold),
    f.col("tip_pct"),
)

# Group by pickup hour, carrying sums and counts rather than averages
hourly = df.groupBy("pickup_hour").agg(
    f.count("*").alias("trips_amount_hourly"),
    f.sum("total_amount").alias("revenue_sum"),
    f.sum(card_tip).alias("tip_pct_sum"),
    f.count(card_tip).alias("tip_trips"),
    f.sum("speed_mph").alias("speed_sum"),
    f.count("speed_mph").alias("speed_trips"),
    f.sum("trip_distance").alias("distance_sum"),
)

# Per-hour averages for the output table, derived from those sums.
hourly = hourly.withColumns({
    "revenue_hourly": f.round(f.col("revenue_sum")),
    "avg_tip_pct": f.round(f.col("tip_pct_sum") / f.col("tip_trips"), 2),
    "avg_speed_mph": f.round(f.col("speed_sum") / f.col("speed_trips"), 2),
    "avg_trip_distance": f.round(f.col("distance_sum") / f.col("trips_amount_hourly"), 2),
})


def weighted_stats():
    """Trip-weighted aggregates over a set of hourly rows.

    Pooling the sums and dividing once gives the average over trips. Averaging
    the per-hour averages instead would give every hour the same weight.
    """
    return [
        f.round(f.sum("tip_pct_sum") / f.sum("tip_trips"), 2).alias("avg_tip_pct"),
        f.round(f.sum("speed_sum") / f.sum("speed_trips"), 2).alias("avg_speed_mph"),
        f.round(f.avg("trips_amount_hourly")).alias("trips_amount"),
        f.count("*").alias("hours"),
    ]

# Create weatherDF from json.
raw_hourly = spark.read.json("data/archive").first()["hourly"]
weather_df = spark.createDataFrame(
    zip(raw_hourly["time"], raw_hourly["temperature_2m"], raw_hourly["precipitation"]),
    schema=["pickup_hour", "temperature", "precipitation"],
)

# The API returns "2024-01-01T00:00" as a string. Parse it explicitly instead of
# leaving Spark to cast string-to-timestamp inside the join: a format change
# would otherwise turn every key null and silently empty the join.
weather_df = weather_df.withColumn(
    "pickup_hour", f.to_timestamp("pickup_hour", "yyyy-MM-dd'T'HH:mm")
)

# Join weather data with trips data
extended_df = hourly.join(weather_df, "pickup_hour").sort("pickup_hour").cache()
extended_df.show(n=10)

# An inner join on a mis-parsed key fails silently by returning fewer rows, so
# check both sides line up. 2024-03-10 02:00 is expected to be unmatched: the
# API emits a flat 24-hour grid, but that local hour does not exist on the day
# the clocks go forward, so no trip can carry it.
trip_hours, weather_hours, joined = hourly.count(), weather_df.count(), extended_df.count()
print(f"trip hours: {trip_hours}, weather hours: {weather_hours}, joined: {joined}")
unmatched = weather_df.join(hourly, "pickup_hour", "left_anti")
if unmatched.count():
    print("weather hours with no trips:")
    unmatched.select("pickup_hour").show(5, False)

# Result tables kept in the repository. findings/ is tracked by git (data/ is
# not), so they are written as plain CSV files with fixed names, which GitHub can
# display. Both are aggregates - 8,783 hourly and 366 daily rows for 2024 - small
# enough to collect with toPandas().
#
# The daily table is rolled up from the hourly one using the same sums and counts,
# so tips and speed stay trip-weighted and the trips are not read a second time.
# Like everything in this script, it excludes trips at speed_threshold or above.
daily_summary = (
    extended_df.groupBy(f.to_date("pickup_hour").alias("date"))
    .agg(
        f.sum("trips_amount_hourly").alias("trips"),
        f.round(f.sum("revenue_sum"), 2).alias("revenue"),
        f.round(f.sum("tip_pct_sum") / f.sum("tip_trips"), 2).alias("avg_tip_pct"),
        f.round(f.sum("speed_sum") / f.sum("speed_trips"), 2).alias("avg_speed_mph"),
        f.round(f.sum("precipitation"), 1).alias("precipitation_mm"),
        f.sum(f.when(f.col("precipitation") >= rainy_hour_threshold, 1).otherwise(0)).alias("rainy_hours"),
        f.round(f.avg("temperature"), 1).alias("temp_mean_c"),
        f.min("temperature").alias("temp_min_c"),
        f.max("temperature").alias("temp_max_c"),
    )
    .sort("date")
)

FINDINGS.mkdir(exist_ok=True)
extended_df.toPandas().round(2).to_csv(FINDINGS / "hourly_summary.csv", index=False)
daily_summary.toPandas().to_csv(FINDINGS / "daily_summary.csv", index=False)

# Finding relation between weather and speed, tips amount, trip distance
rainy_df = extended_df.filter(
    f.col("precipitation") >= rainy_hour_threshold
).agg(*weighted_stats())

normal_weather_df = extended_df.filter(
    f.col("precipitation") < rainy_hour_threshold
).agg(*weighted_stats())

print("Tips amount, number of trips and average speed at rainy hours:")
rainy_df.show()
print("Tips amount, number of trips and average speed at NON-rainy hours:")
normal_weather_df.show()

r_tips, r_speed, r_trips_volume, r_hours = rainy_df.first()
n_tips, n_speed, n_trips_volume, n_hours = normal_weather_df.first()

# Sample size matters for how much weight these comparisons carry.
print(f"Rainy hours (>= {rainy_hour_threshold} mm): {r_hours} of {r_hours + n_hours}")

print(f"During rainy hours: \n \
        {r_tips}% vs {n_tips}% - there is no meaningful tip effect for these two categories;\n \
        the trip volume is {round((r_trips_volume / n_trips_volume - 1) * 100, 1)}% higher than when it's not raining;\n \
        the speed (in mph) is {round((1 - r_speed / n_speed) * 100, 1)}% lower than when it's not raining."
)

# Finding the relation between temperature and indicators like tip percentage, trip speed, trip volume 
# Label each hour with the lower bound of its 5-degree bucket, then group once.
# floor() puts any temperature into a bucket, so the range follows the data: a
# fixed list of ranges would silently drop every hour outside it (summer hours
# above 25 °C, for example). The isNotNull filter only drops hours with no reading.
BUCKET_WIDTH = 5
bucket = (f.floor(f.col("temperature") / BUCKET_WIDTH) * BUCKET_WIDTH).cast("int")

by_temp = (
    extended_df.withColumn("bucket_lower", bucket)
    .filter(f.col("bucket_lower").isNotNull())
    .groupBy("bucket_lower")
    .agg(*weighted_stats())
    .sort("bucket_lower")
)

stat = [
    (
        f"{row['bucket_lower']} to {row['bucket_lower'] + BUCKET_WIDTH}",
        row["avg_tip_pct"],
        row["avg_speed_mph"],
        row["trips_amount"],
        row["hours"],
    )
    for row in by_temp.collect()
]


results = spark.createDataFrame(stat, ["Temperature interval (°C)", "Average tip percentage", "Average speed (mph)", "Average number of trips", "Hours"])
print("\nAverage values by temperature intervals:")
results.show()
# Signed changes rather than fixed words like "drop" or "increase": the direction
# depends on which months are loaded. The hour counts show how thin the extreme
# buckets are, so read these two rows with care.
coldest, warmest = stat[0], stat[-1]
change = lambda i: (float(warmest[i]) / float(coldest[i]) - 1) * 100
print(f"From the coldest interval ({coldest[0]} °C, {coldest[4]} hours) to the warmest "
      f"({warmest[0]} °C, {warmest[4]} hours): tips {change(1):+.2f}%, "
      f"speed {change(2):+.2f}%, number of trips {change(3):+.2f}%")


# Bar chart for tips in rainy hours (>= 0.5 mm per hour) in comparison with unrainy hours (the rest)
fig, ax = plt.subplots()
bar_colors = ['tab:blue', 'tab:orange']

x = ["Rainy hours", "Non-rainy hours"]
y = [r_tips, n_tips]
bars = ax.bar(x, y, color=bar_colors)

ax.bar_label(bars, padding=1)
ax.set_ylabel("Tip percentage")
ax.set_title("Average tip percentage in rainy hours and in non-rainy hours")
save_figure("tip_pct_in_rain")


# Trips becomes slower in the rainy hours 
fig, ax = plt.subplots()
bar_colors = ['tab:green', 'tab:purple']

x = ["Rainy hours", "Non-rainy hours"]
y = [r_speed, n_speed]
bars = ax.bar(x, y, color=bar_colors)

ax.bar_label(bars, padding=1)
ax.set_ylabel("Speed (mph)")
ax.set_title("Average speed in rainy hours and in non-rainy hours")
save_figure("speed_in_rain")


# Hourly trip volume changes with rain (it's growing significantly)
fig, ax = plt.subplots()
bar_colors = ['tab:olive', 'tab:cyan']

x = ["Rainy hours", "Non-rainy hours"]
y = [r_trips_volume, n_trips_volume]
bars = ax.bar(x, y, color=bar_colors)

ax.bar_label(bars, padding=1)
ax.set_ylabel("Number of trips")
ax.set_title("Average number of trips in rainy hours and in non-rainy hours")
save_figure("trips_in_rain")


# How temperature relates to tips, speed and number of trips
tips = [row[1] for row in stat]
speed = [row[2] for row in stat]
trips = [row[3] for row in stat]
temperature_intervals = [row[0] for row in stat]

fig, ax = plt.subplots(figsize=(10, 5.5))
fig.subplots_adjust(right=0.75, bottom=0.22, top=0.82)
fig.suptitle("How temperature relates to tips, speed and number of trips")
twin1 = ax.twinx()
twin2 = ax.twinx()

# Offset the right spine of twin2
twin2.spines.right.set_position(("axes", 1.2))

x = range(len(temperature_intervals))

p1, = ax.plot(x, tips, "C0", label="Average tips (%)", linewidth=2)
p2, = twin1.plot(x, speed, "C1", label="Average speed (mph)", linewidth=2)
p3, = twin2.plot(x, trips, "C2", label="Average number of trips", linewidth=2)

# No fixed limits: each line's range depends on the months loaded.
ax.set(xlabel="Temperature interval (°C)", ylabel="Tips (%)")
twin1.set(ylabel="Speed (mph)")
twin2.set(ylabel="Number of trips")
for axis in (ax, twin1, twin2):
    axis.margins(y=0.08)

ax.yaxis.label.set_color(p1.get_color())
twin1.yaxis.label.set_color(p2.get_color())
twin2.yaxis.label.set_color(p3.get_color())

ax.tick_params(axis='y', colors=p1.get_color())
twin1.tick_params(axis='y', colors=p2.get_color())
twin2.tick_params(axis='y', colors=p3.get_color())

ax.set_xticks(list(x))
ax.set_xticklabels(temperature_intervals, rotation=45, ha="right")

# Above the plot area, so it can never cover a line whatever the data does.
ax.legend(handles=[p1, p2, p3], loc="lower center", bbox_to_anchor=(0.5, 1.02), ncol=3, frameon=False)
save_figure("temperature_intervals")
