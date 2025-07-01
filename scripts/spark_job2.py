from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)
from pyspark.sql.functions import col, count, sum, avg, max, min, to_date, datediff
import logging
from datetime import datetime
import boto3
import sys
import os

# ==== Setup logging ====
log_filename = f"spark_job2_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
local_log_path = f"/tmp/{log_filename}"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(local_log_path), logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("UserTransactionAnalysis")

# ==== Initialize Spark session ====
spark = SparkSession.builder.appName("UserTransactionAnalysis").getOrCreate()

# ==== Define schemas ====
users_schema = StructType(
    [
        StructField("user_id", StringType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("driver_license_number", StringType(), True),
        StructField("driver_license_expiry", StringType(), True),
        StructField("creation_date", TimestampType(), True),
        StructField("is_active", StringType(), True),
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
    logger.info("Starting spark_job2: User and Transaction Analysis")

    # Read data from S3
    try:
        users = spark.read.schema(users_schema).csv(
            "s3://lab4-car-rental-data/raw/users/", header=True
        )
        transactions = spark.read.schema(transactions_schema).csv(
            "s3://lab4-car-rental-data/raw/rental_transactions/", header=True
        )
        logger.info("Successfully read input data from S3")
    except Exception as e:
        logger.error(f"Error reading input data from S3: {e}")
        raise e

    # Validate non-empty datasets
    if users.count() == 0 or transactions.count() == 0:
        raise ValueError("One or more input datasets are empty")

    logger.info("Input data validation passed")

    # Join Datasets
    try:
        trans_user = transactions.join(
            users, transactions.user_id == users.user_id, "inner"
        )
        logger.info("Successfully joined datasets")
    except Exception as e:
        logger.error(f"Error during join operations: {e}")
        raise e

    # Calculate KPIs
    try:
        daily_metrics = trans_user.groupBy(
            to_date(col("rental_start_time")).alias("rental_date")
        ).agg(
            count("rental_id").alias("total_transactions"),
            sum("total_amount").alias("total_revenue"),
        )

        user_metrics = trans_user.groupBy(users.user_id, "first_name", "last_name").agg(
            count("rental_id").alias("total_transactions"),
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_transaction_amount"),
            max("total_amount").alias("max_transaction_amount"),
            min("total_amount").alias("min_transaction_amount"),
            sum(
                datediff(
                    to_date(col("rental_end_time")), to_date(col("rental_start_time"))
                )
            ).alias("total_rental_days"),
        )
        logger.info("Successfully calculated KPIs")
    except Exception as e:
        logger.error(f"Error calculating KPIs: {e}")
        raise e

    # Write Results to S3
    try:
        daily_metrics.write.mode("overwrite").parquet(
            "s3://lab4-car-rental-data/processed/daily_metrics/"
        )
        user_metrics.write.mode("overwrite").parquet(
            "s3://lab4-car-rental-data/processed/user_metrics/"
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
