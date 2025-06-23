from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, max, min, to_date, datediff

# Initialize Spark session
spark = SparkSession.builder.appName("UserTransactionAnalysis").getOrCreate()

# Read data from S3
users = spark.read.csv(
    "s3://lab4-car-rental-data/raw/users/", header=True, inferSchema=True
)
transactions = spark.read.csv(
    "s3://lab4-car-rental-data/raw/rental_transactions/", header=True, inferSchema=True
)

# Join datasets
trans_user = transactions.join(users, transactions.user_id == users.user_id, "inner")

# Calculate KPIs
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
        datediff(to_date(col("rental_end_time")), to_date(col("rental_start_time")))
    ).alias("total_rental_days"),
)

# Write results to S3 in Parquet format
daily_metrics.write.mode("overwrite").parquet(
    "s3://lab4-car-rental-data/processed/daily_metrics/"
)
user_metrics.write.mode("overwrite").parquet(
    "s3://lab4-car-rental-data/processed/user_metrics/"
)

spark.stop()
