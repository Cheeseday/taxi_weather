from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("weather_enrichment").master("local[*]").getOrCreate()

tip_pct_threshold = 20000 # bigger tips only appears when the distance is equal 0.01 miles
card_payment = 1 # verbal explanation what the threshold means
speed_threshold = 55 # that threshold was chosed because it's a speed limit for highways in New York

df = spark.read.parquet("data/cleaned_trips")
df = df.filter((f.col("speed_mph") < speed_threshold))

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
).sort("pickup_hour")

w_df = spark.read.json("data/archive")
weather_df = spark.createDataFrame(
    zip(w_df.first()["hourly"][0], w_df.first()["hourly"][1], w_df.first()["hourly"][2]), 
    schema=["precipitation", "temperature", "pickup_hour"]
)

extended_df = hourly.join(weather_df, "pickup_hour").sort("pickup_hour").cache()
extended_df.show(n=10)

# Finding relation between weather and speed, tips amount, trip distance
rainy_hour_threshold = 2.5 # The Bureau of Meteorology consider it as a lower bound for moderate rain intensity

rainy_df = extended_df.filter(f.col("precipitation") >= rainy_hour_threshold).agg(
    f.round(f.avg(f.col("avg_tip_pct")), 2).alias("avg_rainy_tip_pct"),
    f.round(f.avg(f.col("avg_speed_mph")), 2).alias("avg_rainy_speed_mph"),
    f.round(f.avg(f.col("avg_trip_distance")), 2).alias("avg_rainy_trip_distance")
)

normal_weather_df = extended_df.filter(f.col("precipitation") < rainy_hour_threshold).agg(
    f.round(f.avg(f.col("avg_tip_pct")), 2).alias("avg_normal_tip_pct"),
    f.round(f.avg(f.col("avg_speed_mph")), 2).alias("avg_normal_speed_mph"),
    f.round(f.avg(f.col("avg_trip_distance")), 2).alias("avg_normal_trip_distance")
)

print("Tips amount and average speed at rainy hours:")
rainy_df.show()
print("Tips amount and average speed at NOT rainy hours:")
normal_weather_df.show()

rainy_tips = rainy_df.first()["avg_rainy_tip_pct"]
normal_tips = normal_weather_df.first()["avg_normal_tip_pct"]
rainy_speed = rainy_df.first()["avg_rainy_speed_mph"]
normal_speed = normal_weather_df.first()["avg_normal_speed_mph"]
rainy_distance = rainy_df.first()["avg_rainy_trip_distance"]
normal_distance = normal_weather_df.first()["avg_normal_trip_distance"]

print(f"During rainy hours: \n \
        the tips amount is {round((rainy_tips / normal_tips - 1) * 100, 1)} percent higher than when it's not raining;\n \
        the trip distance is {round((1 - rainy_distance / normal_distance) * 100, 1)} percent lower than when it's not raining;\n \
        the speed (in mph) is {round((1 - rainy_speed / normal_speed) * 100, 1)} percent lower than when it's not raining."
)
