from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)
from pyspark.sql.functions import col, count, sum, avg, max, min, datediff, to_date
import logging
from datetime import datetime
import boto3
import sys
import os

# ==== Setup logging ====
log_filename = f"spark_job1_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
local_log_path = f"/tmp/{log_filename}"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(local_log_path), logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("VehicleLocationMetrics")


# ==== Initialize Spark session ====
spark = SparkSession.builder.appName("VehicleLocationMetrics").getOrCreate()

# ==== Define Schemas ====
vehicles_schema = StructType(
    [
        StructField("active", StringType(), True),
        StructField("vehicle_license_number", StringType(), True),
        StructField("registration_name", StringType(), True),
        StructField("license_type", StringType(), True),
        StructField("expiration_date", StringType(), True),
        StructField("permit_license_number", StringType(), True),
        StructField("certification_date", StringType(), True),
        StructField("vehicle_year", IntegerType(), True),
        StructField("base_telephone_number", StringType(), True),
        StructField("base_address", StringType(), True),
        StructField("vehicle_id", StringType(), False),
        StructField("last_update_timestamp", TimestampType(), True),
        StructField("brand", StringType(), True),
        StructField("vehicle_type", StringType(), True),
    ]
)

locations_schema = StructType(
    [
        StructField("location_id", StringType(), False),
        StructField("location_name", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zip_code", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
    ]
)

transactions_schema = StructType(
    [
        StructField("rental_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("vehicle_id", StringType(), False),
        StructField("rental_start_time", TimestampType(), True),
        StructField("rental_end_time", TimestampType(), True),
        StructField("pickup_location", StringType(), True),
        StructField("dropoff_location", StringType(), True),
        StructField("total_amount", DoubleType(), True),
    ]
)

# ==== Main Processing ====
try:
    logger.info("Starting spark_job1: Vehicle and Location Metrics")

    # Read Data
    try:
        vehicles = spark.read.schema(vehicles_schema).csv(
            "s3://lab4-car-rental-data/raw/vehicles/", header=True
        )
        locations = spark.read.schema(locations_schema).csv(
            "s3://lab4-car-rental-data/raw/locations/", header=True
        )
        transactions = spark.read.schema(transactions_schema).csv(
            "s3://lab4-car-rental-data/raw/rental_transactions/", header=True
        )
        logger.info("Successfully read input data from S3")
    except Exception as e:
        logger.error(f"Error reading input data from S3: {e}")
        raise e

    # Validate non-empty datasets
    if vehicles.count() == 0 or locations.count() == 0 or transactions.count() == 0:
        raise ValueError("One or more input datasets are empty")

    logger.info("Input data validation passed")

    # Join Datasets
    try:
        trans_loc = transactions.join(
            locations, transactions.pickup_location == locations.location_id, "inner"
        )

        trans_loc_veh = trans_loc.join(
            vehicles, transactions.vehicle_id == vehicles.vehicle_id, "inner"
        )
        logger.info("Successfully joined datasets")
    except Exception as e:
        logger.error(f"Error during join operations: {e}")
        raise e

    # Calculate KPIs
    try:
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
                datediff(
                    to_date(col("rental_end_time")), to_date(col("rental_start_time"))
                )
            ).alias("avg_rental_duration_days"),
        )
        logger.info("Successfully calculated KPIs")
    except Exception as e:
        logger.error(f"Error calculating KPIs: {e}")
        raise e

    # Write Results to S3
    try:
        location_metrics.write.mode("overwrite").parquet(
            "s3://lab4-car-rental-data/processed/location_metrics/"
        )
        vehicle_metrics.write.mode("overwrite").parquet(
            "s3://lab4-car-rental-data/processed/vehicle_metrics/"
        )
        logger.info("Successfully wrote output metrics to S3")
    except Exception as e:
        logger.error(f"Error writing output to S3: {e}")
        raise e

except Exception as e:
    logger.error(f"Job failed with error: {e}")
    raise e

finally:
    try:
        # Upload full log file to S3
        s3_client = boto3.client("s3")
        s3_key = f"logs/{log_filename}"
        with open(local_log_path, "rb") as f:
            s3_client.upload_fileobj(f, "lab4-car-rental-data", s3_key)
        logger.info(
            f"Uploaded full log file to S3 at: s3://lab4-car-rental-data/{s3_key}"
        )
    except Exception as upload_error:
        logger.error(f"Failed to upload log file to S3: {upload_error}")

    spark.stop()
    logger.info("Spark session stopped")
