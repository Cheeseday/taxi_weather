from pyspark.sql import SparkSession
from pyspark.sql import functions as f


spark = (
    SparkSession.builder.appName("zone_enrichment")
    .master("local[*]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", 32)
    .getOrCreate()
)

tip_pct_threshold = 20000  # above this measure, tip_pct comes from a 0.01-mile fare
card_payment = 1           # credit card only

df = spark.read.parquet("data/cleaned_trips")

# # Use a broadcast join to enrich df with NYC zone data.
zone_df = spark.read.option("header", True).option("inferSchema", True).csv(
    "data/taxi_zone_lookup.csv"
)
zones = f.broadcast(zone_df)

card_tip = f.when(
    (f.col("payment_type") == card_payment) & (f.col("tip_pct") < tip_pct_threshold),
    f.col("tip_pct"),
)

# Aggregate before joining. 
# Borough is a function of LocationID, so it can be attached after aggregating
pickup_stats = df.groupBy("PULocationID").agg(
    f.count("*").alias("trips"),
    f.sum(card_tip).alias("tip_pct_sum"),
    f.count(card_tip).alias("tip_trips"),
    f.sum(
        f.when(
            (f.col("payment_type") == card_payment)
            & (f.col("tip_pct") < tip_pct_threshold)
            & (f.col("tip_amount") == 0),
            1,
        ).otherwise(0)
    ).alias("zero_tip_trips"),
).cache()

dropoff_stats = df.groupBy("DOLocationID").agg(f.count("*").alias("trips"))


def zone_totals(stats, id_column, label):
    """Attach zone names to per-LocationID counts and rank them.
    """
    return (
        stats.join(zones, f.col(id_column) == zones["LocationID"])
        .groupBy("Zone")
        .agg(f.sum("trips").alias(label))
        .sort(label, ascending=False)
    )


print("Top 10 pickup zones:")
zone_totals(pickup_stats, "PULocationID", "Number of pickups").show(n=10, truncate=False)

print("Top 10 dropoff zones:")
zone_totals(dropoff_stats, "DOLocationID", "Number of dropoffs").show(n=10, truncate=False)

print("Average tip percentage across boroughs:")
pickup_stats.join(zones, f.col("PULocationID") == zones["LocationID"]).filter(
    ~f.col("Borough").isin("EWR", "Unknown", "N/A")
).groupBy("Borough").agg(
    f.round(f.sum("tip_pct_sum") / f.sum("tip_trips"), 2).alias("Average tip percentage"),
    f.sum("tip_trips").alias("trip_amount"),
    f.round(f.sum("zero_tip_trips") / f.sum("tip_trips"), 2).alias("Zero tip share"),
).sort("Average tip percentage", ascending=False).show()

pickup_stats.unpersist()
spark.stop()
