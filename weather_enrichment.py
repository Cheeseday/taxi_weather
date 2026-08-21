from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("weather_enrichment").master("local[*]").getOrCreate()

tip_pct_threshold = 20000 # bigger tips only appears when the distance is equal 0.01 miles
card_payment = 1 # verbal explanation what the threshold means
speed_threshold = 55 # that threshold was chosed because it's a speed limit for highways in New York
rainy_hour_threshold = 2.5 # The Bureau of Meteorology consider it as a lower bound for moderate rain intensity

df = spark.read.parquet("data/cleaned_trips")
df = df.filter((f.col("speed_mph") < speed_threshold))

hourly = df.groupBy("pickup_hour").agg(
    f.count("*").alias("trips_amount_hourly"),
    f.round(f.sum("total_amount"))
        .alias("revenue_hourly"),
    f.round(f.sum(f.when(
        (f.col("payment_type") == card_payment) & 
        (f.col("tip_pct") < tip_pct_threshold), f.col("tip_amount"))), 2)
        .alias("tip_amount_hourly"),
    f.round(f.avg(f.when(
        (f.col("payment_type") == card_payment) & 
        (f.col("tip_pct") < tip_pct_threshold), f.col("tip_pct"))), 2)
        .alias("avg_tip_pct"),
    f.round(f.avg(f.col("trip_distance")), 2)
        .alias("avg_trip_distance"),
    f.round(f.avg(f.col("speed_mph")), 2)
        .alias("avg_speed_mph") 
).sort("pickup_hour")

w_df = spark.read.json("data/archive")
weather_df = spark.createDataFrame(
    zip(w_df.first()["hourly"][0], w_df.first()["hourly"][1], w_df.first()["hourly"][2]), 
    schema=["precipitation", "temperature", "pickup_hour"]
)

extended_df = hourly.join(weather_df, "pickup_hour").sort("pickup_hour").cache()
extended_df.show(n=10)

# Finding relation between weather and speed, tips amount, trip distance
rainy_df = extended_df.filter(f.col("precipitation") >= rainy_hour_threshold).agg(
    f.round(f.avg(f.col("avg_tip_pct")), 2).alias("avg_rainy_tip_pct"),
    f.round(f.avg(f.col("avg_speed_mph")), 2).alias("avg_rainy_speed_mph"),
    f.round(f.avg(f.col("avg_trip_distance")), 2).alias("avg_rainy_trip_distance"),
)

normal_weather_df = extended_df.filter(f.col("precipitation") < rainy_hour_threshold).agg(
    f.round(f.avg(f.col("avg_tip_pct")), 2).alias("avg_normal_tip_pct"),
    f.round(f.avg(f.col("avg_speed_mph")), 2).alias("avg_normal_speed_mph"),
    f.round(f.avg(f.col("avg_trip_distance")), 2).alias("avg_normal_trip_distance"),
)

print("Tips amount and average speed at rainy hours:")
rainy_df.show()
print("Tips amount and average speed at NOT rainy hours:")
normal_weather_df.show()

r_tips, r_speed, r_distance = rainy_df.first()
n_tips, n_speed, n_distance = normal_weather_df.first()

print(f"During rainy hours: \n \
        the tips amount is {round((r_tips / n_tips - 1) * 100, 1)} percent higher than when it's not raining;\n \
        the trip distance is {round((1 - r_distance / n_distance) * 100, 1)} percent lower than when it's not raining;\n \
        the speed (in mph) is {round((1 - r_speed / n_speed) * 100, 1)} percent lower than when it's not raining."
)

temp_buckets = ((-10, -5), (-5, 0), (0, 5), (5, 10), (10, 15), (15, 20), (20, 25))
stat = []
for lower, upper in temp_buckets:
    df = extended_df.filter((f.col("temperature") >= lower) & (f.col("temperature") < upper)).agg(
        f.round(f.avg("avg_tip_pct"), 2).alias("tip_pct"),
        f.round(f.avg("avg_speed_mph"), 2).alias("speed_mph"),
        f.round(f.avg("avg_trip_distance"), 2).alias("trip_distance"),
    )
    temp = f"{lower} to {upper}"
    tip, speed, distance= df.first()
    stat.append((temp, tip, speed, distance))

results = spark.createDataFrame(stat, ["Temperature interval (°C)", "Average tip percentage", "Average speed (mph)", "Average trip distance (miles)"])
print("\nAverage values by temperature intervals:")
results.show()
print(f"The trip distance, as well as speed, significantly decrease with temperature growth, tips percentage stays generally the same:\n\
    For interval {stat[0][0]}°C: tips - {stat[0][1]} pct, speed - {stat[0][2]} mph, distance - {stat[0][3]} miles\n\
    For interval {stat[6][0]}°C: tips - {stat[6][1]} pct, speed - {stat[6][2]} mph, distance - {stat[6][3]} miles\n\
    Difference: tips - {round((float(stat[0][1]) / float(stat[6][1]) - 1) * 100, 2)}% drop, speed: {round(((float(stat[0][2]) / float(stat[6][2]) - 1) * 100), 2)}% drop, distance: {round(((float(stat[0][3]) / float(stat[6][3]) - 1) * 100), 2)}% drop"
)
