from pyspark.sql import SparkSession
from pyspark.sql import functions as f

import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("weather_enrichment").master("local[*]").getOrCreate()

tip_pct_threshold = 20000 # bigger tips only appears when the distance is equal 0.01 miles
card_payment = 1 # verbal explanation what the threshold means
speed_threshold = 55 # that threshold was chosed because it's a speed limit for highways in New York
rainy_hour_threshold = 2.5 # The Bureau of Meteorology consider it as a lower bound for moderate rain intensity

df = spark.read.parquet("data/cleaned_trips")

df = df.filter((f.col("speed_mph") < speed_threshold))

# Group the df by pickup hour and create aggregations for further analysis
hourly = df.groupBy("pickup_hour").agg(
    f.count("*").alias("trips_amount_hourly"),
    f.round(f.sum("total_amount"))
        .alias("revenue_hourly"),
    f.round(f.avg(f.when(
        (f.col("payment_type") == card_payment) & 
        (f.col("tip_pct") < tip_pct_threshold), f.col("tip_pct"))), 2)
        .alias("avg_tip_pct"),
    f.round(f.avg(f.col("trip_distance")), 2)
        .alias("avg_trip_distance"),
    f.round(f.avg(f.col("speed_mph")), 2)
        .alias("avg_speed_mph") 
)

# Create weatherDF from json 
w_df = spark.read.json("data/archive")
weather_df = spark.createDataFrame(
    zip(w_df.first()["hourly"][0], w_df.first()["hourly"][1], w_df.first()["hourly"][2]), 
    schema=["precipitation", "temperature", "pickup_hour"]
)

# Join weather data with trips data
extended_df = hourly.join(weather_df, "pickup_hour").sort("pickup_hour").cache()
extended_df.show(n=10)

extended_df.coalesce(1).repartition(1).write.mode("overwrite").parquet("data/findings")

# Finding relation between weather and speed, tips amount, trip distance
rainy_df = extended_df.filter(f.col("precipitation") >= rainy_hour_threshold).agg(
    f.round(f.avg(f.col("avg_tip_pct")), 2).alias("avg_rainy_tip_pct"),
    f.round(f.avg(f.col("avg_speed_mph")), 2).alias("avg_rainy_speed_mph"),
    f.round(f.avg(f.col("trips_amount_hourly"))).alias("rainy_trips_amount"),
)

normal_weather_df = extended_df.filter(f.col("precipitation") < rainy_hour_threshold).agg(
    f.round(f.avg(f.col("avg_tip_pct")), 2).alias("avg_normal_tip_pct"),
    f.round(f.avg(f.col("avg_speed_mph")), 2).alias("avg_normal_speed_mph"),
    f.round(f.avg(f.col("trips_amount_hourly"))).alias("normal_trips_amount"),
)

print("Tips amount, number of trips and average speed at rainy hours:")
rainy_df.show()
print("Tips amount, number of trips and average speed at NON-rainy hours:")
normal_weather_df.show()

r_tips, r_speed, r_trips_volume = rainy_df.first()
n_tips, n_speed, n_trips_volume = normal_weather_df.first()

print(f"During rainy hours: \n \
        the tips amount is {round((r_tips / n_tips - 1) * 100, 1)} percent higher than when it's not raining;\n \
        the trip volume is {round((r_trips_volume / n_trips_volume - 1) * 100, 1)} percent higher than when it's not raining;\n \
        the speed (in mph) is {round((1 - r_speed / n_speed) * 100, 1)} percent lower than when it's not raining."
)

# Finding the relation between temperature and indicators like tip percentage, trip speed, trip volume 
temp_buckets = ((-10, -5), (-5, 0), (0, 5), (5, 10), (10, 15), (15, 20), (20, 25))
stat = []
for lower, upper in temp_buckets:
    df = extended_df.filter((f.col("temperature") >= lower) & (f.col("temperature") < upper)).agg(
        f.round(f.avg("avg_tip_pct"), 2),
        f.round(f.avg("avg_speed_mph"), 2),
        f.round(f.avg("trips_amount_hourly")),
    )
    temp = f"{lower} to {upper}"
    tip, speed, volume = df.first()

    stat.append((temp, tip, speed, volume))


results = spark.createDataFrame(stat, ["Temperature interval (°C)", "Average tip percentage", "Average speed (mph)", "Average number of trips"])
print("\nAverage values by temperature intervals:")
results.show()
print(f"The number of trips is hugely increase and speed significantly decrease with temperature growth, tips percentage stays generally the same:\n\
    For interval {stat[0][0]}°C: tips - {stat[0][1]} pct, speed - {stat[0][2]} mph, number of trips - {stat[0][3]}\n\
    For interval {stat[6][0]}°C: tips - {stat[6][1]} pct, speed - {stat[6][2]} mph, number of trips - {stat[6][3]}\n\
    Difference: tips - {round((1 - float(stat[6][1]) / float(stat[0][1])) * 100, 2)}% drop, speed: {round(((1 - float(stat[6][2]) / float(stat[0][2])) * 100), 2)}% drop, number of trips: {round(((float(stat[6][3]) / float(stat[0][3]) - 1) * 100), 2)}% increase"
)


# Bar chart for tips in rainy hours (>= 2.5 mm per hour) in comparison with unrainy hours (the rest)
fig, ax = plt.subplots()
bar_colors = ['tab:blue', 'tab:orange']

x = ["Rainy hours", "Non-rainy hours"]
y = [r_tips, n_tips]
bars = ax.bar(x, y, color=bar_colors)

ax.bar_label(bars, padding=1)
ax.set_ylabel("Tip percentage")
ax.set_title("Average tip percentage in rainy hours and in non-rainy hours")
plt.show()


# Trips becomes slower in the rainy hours 
fig, ax = plt.subplots()
bar_colors = ['tab:green', 'tab:purple']

x = ["Rainy hours", "Non-rainy hours"]
y = [r_speed, n_speed]
bars = ax.bar(x, y, color=bar_colors)

ax.bar_label(bars, padding=1)
ax.set_ylabel("Speed (mph)")
ax.set_title("Average speed in rainy hours and in non-rainy hours")
plt.show()


# Hourly trip volume changes with rain (it's growing significantly)
fig, ax = plt.subplots()
bar_colors = ['tab:olive', 'tab:cyan']

x = ["Rainy hours", "Non-rainy hours"]
y = [r_trips_volume, n_trips_volume]
bars = ax.bar(x, y, color=bar_colors)

ax.bar_label(bars, padding=1)
ax.set_ylabel("Number of trips")
ax.set_title("Average number of trips in rainy hours and in non-rainy hours")
plt.show()


# How temperature relates to tips, speed and number of trips
tips = [row[1] for row in stat]
speed = [row[2] for row in stat]
trips = [row[3] for row in stat]
temperature_intervals = ["[-10..-5)", "[-5..-0)", "[0..+5)", "[+5..+10)", "[+10..+15)", "[+15..+20)", "[+20..+25)"]

fig, ax = plt.subplots()
fig.subplots_adjust(right=0.75, bottom=0.22)
fig.suptitle("How temperature relates to tips, speed and number of trips")
twin1 = ax.twinx()
twin2 = ax.twinx()

# Offset the right spine of twin2
twin2.spines.right.set_position(("axes", 1.2))

x = range(len(temperature_intervals))

p1, = ax.plot(x, tips, "C0", label="Average tips (%)", linewidth=2)
p2, = twin1.plot(x, speed, "C1", label="Average speed (mph)", linewidth=2)
p3, = twin2.plot(x, trips, "C2", label="Average number of trips", linewidth=2)

ax.set(ylim=(22, 27), xlabel="Temperature interval (°C)", ylabel="Tips (%)")
twin1.set(ylim=(8, 20), ylabel="Speed (mph)")
twin2.set(ylim=(2200, 9000), ylabel="Number of trips")

ax.yaxis.label.set_color(p1.get_color())
twin1.yaxis.label.set_color(p2.get_color())
twin2.yaxis.label.set_color(p3.get_color())

ax.tick_params(axis='y', colors=p1.get_color())
twin1.tick_params(axis='y', colors=p2.get_color())
twin2.tick_params(axis='y', colors=p3.get_color())

ax.set_xticks(list(x))
ax.set_xticklabels(temperature_intervals, rotation=45, ha="right")

ax.legend(handles=[p1, p2, p3], loc=1)
plt.show()
