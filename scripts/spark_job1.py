from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, max, min, datediff, to_date

# Initialize Spark session
spark = SparkSession.builder.appName("VehicleLocationMetrics").getOrCreate()

# Read data from S3
vehicles = spark.read.csv(
    "s3://lab4-car-rental-data/raw/vehicles/", header=True, inferSchema=True
)
locations = spark.read.csv(
    "s3://lab4-car-rental-data/raw/locations/", header=True, inferSchema=True
)
transactions = spark.read.csv(
    "s3://lab4-car-rental-data/raw/rental_transactions/", header=True, inferSchema=True
)

# Join datasets
trans_loc = transactions.join(
    locations, transactions.pickup_location == locations.location_id, "inner"
)
trans_loc_veh = trans_loc.join(
    vehicles, transactions.vehicle_id == vehicles.vehicle_id, "inner"
)

# Calculate KPIs
location_metrics = trans_loc.groupBy("pickup_location", "location_name").agg(
    sum("total_amount").alias("total_revenue"),
    count("rental_id").alias("total_transactions"),
    avg("total_amount").alias("avg_transaction_amount"),
    max("total_amount").alias("max_transaction_amount"),
    min("total_amount").alias("min_transaction_amount"),
    count("vehicle_id").alias("unique_vehicles"),
)

vehicle_metrics = trans_loc_veh.groupBy("vehicle_type").agg(
    sum("total_amount").alias("total_revenue"),
    avg(
        datediff(to_date(col("rental_end_time")), to_date(col("rental_start_time")))
    ).alias("avg_rental_duration_days"),
)

# Write results to S3 in Parquet format
location_metrics.write.mode("overwrite").parquet(
    "s3://lab4-car-rental-data/processed/location_metrics/"
)
vehicle_metrics.write.mode("overwrite").parquet(
    "s3://lab4-car-rental-data/processed/vehicle_metrics/"
)

spark.stop()
