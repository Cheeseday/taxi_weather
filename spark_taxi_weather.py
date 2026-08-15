from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("taxi_weather").master("local[*]").getOrCreate()

df = spark.read.parquet(
            "/home/ales/Documents/taxi_weather/data/yellow_tripdata_2024-01.parquet",
            "/home/ales/Documents/taxi_weather/data/yellow_tripdata_2024-02.parquet",
            "/home/ales/Documents/taxi_weather/data/yellow_tripdata_2024-03.parquet"
)

df_cleaned = df.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "fare_amount", "total_amount", "passenger_count", "tip_amount"]) \
                .filter((df.tpep_pickup_datetime >= "2024-01-01 00:00:00") & 
                       (df.tpep_pickup_datetime <= "2024-03-31 23:59:59") & 
                       (df.tpep_pickup_datetime <= df.tpep_dropoff_datetime) & 
                       (df.tpep_dropoff_datetime - df.tpep_pickup_datetime < f.expr("INTERVAL 6 HOURS")) & 
                       ((df.trip_distance >= 0) | (df.trip_distance < 100)) & 
                       (df.fare_amount > 0) & 
                       (df.total_amount >= 0) &
                       (df.passenger_count >= 0)
)

df_cleaned_extended = df_cleaned.withColumns({
    "duration_min": f.expr("timestampdiff(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime)"),
    "tip_pct":      f.round(f.col("tip_amount") / f.col("fare_amount") * 100, 2),
    "pickup_date":  f.date_trunc("day", "tpep_pickup_datetime"),
    "pickup_hour":  f.date_trunc("hour", "tpep_pickup_datetime"),
    "speed_mph":    f.round(f.col("trip_distance") / ((f.col("tpep_dropoff_datetime") - f.col("tpep_pickup_datetime")).cast("long") / 3600), 2)
})

print("DataFrame schema:")
df_cleaned_extended.printSchema()
print(f"Total row count before cleaning: {df.count()}")
print(f"Total row count after cleaning: {df_cleaned.count()}")

# df_cleaned_extended.show()


spark.stop()
