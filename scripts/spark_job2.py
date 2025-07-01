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

# Initialize Spark session
spark = SparkSession.builder.appName("UserTransactionAnalysis").getOrCreate()

# Configure logging to S3
log_file = f"s3://lab4-car-rental-data/logs/spark_job2_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("UserTransactionAnalysis")
s3_handler = logging.StreamHandler(sys.stdout)  # Fallback to stdout
logger.addHandler(s3_handler)

# Define schemas
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


# Function to write logs to S3
def write_log_to_s3(message):
    try:
        s3_client = boto3.client("s3")
        log_message = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}\n"
        s3_client.put_object(
            Bucket="lab4-car-rental-data",
            Key=f"logs/spark_job2_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
            Body=log_message,
        )
    except Exception as e:
        logger.error(f"Failed to write log to S3: {str(e)}")


# Main processing with error handling
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
        error_msg = f"Error reading input data from S3: {str(e)}"
        logger.error(error_msg)
        write_log_to_s3(error_msg)
        raise e

    # Validate data
    if users.count() == 0 or transactions.count() == 0:
        error_msg = "One or more input datasets are empty"
        logger.error(error_msg)
        write_log_to_s3(error_msg)
        raise ValueError(error_msg)

    # Join datasets
    try:
        trans_user = transactions.join(
            users, transactions.user_id == users.user_id, "inner"
        )
        logger.info("Successfully joined datasets")
    except Exception as e:
        error_msg = f"Error joining datasets: {str(e)}"
        logger.error(error_msg)
        write_log_to_s3(error_msg)
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
        error_msg = f"Error calculating KPIs: {str(e)}"
        logger.error(error_msg)
        write_log_to_s3(error_msg)
        raise e

    # Write results to S3
    try:
        daily_metrics.write.mode("overwrite").parquet(
            "s3://lab4-car-rental-data/processed/daily_metrics/"
        )
        user_metrics.write.mode("overwrite").parquet(
            "s3://lab4-car-rental-data/processed/user_metrics/"
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
