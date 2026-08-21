from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("weather_enrichment").master("local[*]").getOrCreate()

tip_pct_threshold = 20000 # bigger tips only appears when the distance is equal 0.01 miles
card_payment = 1 # verbal explanation what the threshold means

df = spark.read.parquet("data/cleaned_trips")

# Broadcast
zone_df = spark.read.option("header", True).csv("data/taxi_zone_lookup.csv")
df_b = f.broadcast(zone_df)

# Pickup
df_pickup = df.join(df_b, df["PULocationID"] == df_b["LocationID"])
print("Top 10 pickup zones:")
df_pickup.groupBy("Zone").agg(
    f.count("*").alias("Number of pickups")
).sort("Number of pickups", ascending=False).show(n=10, truncate=False)

# Dropoff
df_dropoff = df.join(df_b, df["DOLocationID"] == df_b["LocationID"])
print("Top 10 dropoff zones:")
df_dropoff.groupBy("Zone").agg(
    f.count("*").alias("Number of dropoffs")
).sort("Number of dropoffs", ascending=False).show(n=10, truncate=False)

# Tip percentage across boroughs
df_pickup = df_pickup.filter(
    (f.col("payment_type") == card_payment) & 
    (f.col("tip_pct") < tip_pct_threshold) & 
    ~f.col("Borough").isin("EWR", "Unknown", "N/A")
)
print("Average tip percentage across boroughs:")
df_pickup.groupBy("Borough").agg(
        f.round(f.avg(f.col("tip_pct")), 2).alias("Average tip percentage"),
        f.count("*").alias("trip_amount"),
        f.round(f.avg(f.when(f.col("tip_amount") == 0, 1).otherwise(0)), 2).alias("Zero tip share"),
).sort("Average tip percentage", ascending=False).show()

