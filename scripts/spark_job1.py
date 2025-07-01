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

# Initialize Spark session
spark = SparkSession.builder.appName("VehicleLocationMetrics").getOrCreate()

# Configure logging to S3
log_file = f"s3://lab4-car-rental-data/logs/spark_job1_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("VehicleLocationMetrics")
s3_handler = logging.StreamHandler(sys.stdout)  # Fallback to stdout
logger.addHandler(s3_handler)

# Define schemas
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


# Function to write logs to S3
def write_log_to_s3(message):
    try:
        s3_client = boto3.client("s3")
        log_message = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}\n"
        s3_client.put_object(
            Bucket="lab4-car-rental-data",
            Key=f"logs/spark_job1_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
            Body=log_message,
        )
    except Exception as e:
        logger.error(f"Failed to write log to S3: {str(e)}")


# Main processing with error handling
try:
    logger.info("Starting spark_job1: Vehicle and Location Metrics")

    # Read data from S3
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
        error_msg = f"Error reading input data from S3: {str(e)}"
        logger.error(error_msg)
        write_log_to_s3(error_msg)
        raise e

    # Validate data
    if vehicles.count() == 0 or locations.count() == 0 or transactions.count() == 0:
        error_msg = "One or more input datasets are empty"
        logger.error(error_msg)
        write_log_to_s3(error_msg)
        raise ValueError(error_msg)

    # Join datasets
    try:
        trans_loc = transactions.join(
            locations, transactions.pickup_location == locations.location_id, "inner"
        )
        trans_loc_veh = trans_loc.join(
            vehicles, transactions.vehicle_id == vehicles.vehicle_id, "inner"
        )
        logger.info("Successfully joined datasets")
    except Exception as e:
        error_msg = f"Error joining datasets: {str(e)}"
        logger.error(error_msg)
        write_log_to_s3(error_msg)
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
        error_msg = f"Error calculating KPIs: {str(e)}"
        logger.error(error_msg)
        write_log_to_s3(error_msg)
        raise e

    # Write results to S3
    try:
        location_metrics.write.mode("overwrite").parquet(
            "s3://lab4-car-rental-data/processed/location_metrics/"
        )
        vehicle_metrics.write.mode("overwrite").parquet(
            "s3://lab4-car-rental-data/processed/vehicle_metrics/"
        )
        logger.info("Successfully wrote results to S3")
    except Exception as e:
        error_msg = f"Error writing results to S3: {str(e)}"
        logger.error(error_msg)
        write_log_to_s3(error_msg)
        raise e

except Exception as e:
    error_msg = f"Job failed: {str(e)}"
    logger.error(error_msg)
    write_log_to_s3(error_msg)
    raise e
finally:
    spark.stop()
    logger.info("Spark session stopped")
