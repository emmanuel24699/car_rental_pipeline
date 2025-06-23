# Car Rental Data Pipeline with EMR and AWS Step Functions

This project implements a serverless big data processing pipeline for a car rental marketplace using AWS services: EMR for Spark processing, AWS Glue for schema inference, Athena for querying, and Step Functions for orchestration. The pipeline processes raw datasets stored in Amazon S3, transforms them into meaningful KPIs, and automates the workflow.

## Overview

The pipeline processes four datasets from a car rental marketplace:

- **Vehicles**: Details of rental vehicles (e.g., `vehicle_id`, `brand`, `vehicle_type`).
- **Users**: User sign-up information (e.g., `user_id`, `first_name`, `last_name`).
- **Rental Transactions**: Rental records (e.g., `rental_id`, `user_id`, `vehicle_id`, `total_amount`).
- **Locations**: Rental location data (e.g., `location_id`, `location_name`).

The pipeline:

1. Runs two Spark jobs in parallel using EMR Serverless to compute KPIs.
2. Uses AWS Glue crawlers to infer schemas and create a Glue Data Catalog.
3. Queries processed data with Athena to extract business insights.
4. Automates the workflow with AWS Step Functions.

## Architecture

- **Input Data**: Raw CSV files in `s3://<bucket_name>/raw/`.
- **Processing**: EMR Serverless runs PySpark scripts (`spark_job1.py`, `spark_job2.py`) to transform data and write Parquet outputs to `s3://<bucket_name>/processed/`.
- **Schema Inference**: Glue crawlers create tables in the `car_rental_db` database.
- **Querying**: Athena queries the processed data, storing results in `s3://<bucket_name>/athena-results/`.
- **Orchestration**: Step Functions coordinates the pipeline, running Spark jobs, crawlers, and Athena queries in parallel where efficient.

## Setup

### 1. S3 Bucket Configuration

- **Bucket**: `lab4-car-rental-data`.
- **Folders**:
  - `raw/vehicles/`: Upload `vehicles.csv`.
  - `raw/users/`: Upload `users.csv`.
  - `raw/rental_transactions/`: Upload `rental_transaction.csv`.
  - `raw/locations/`: Upload `locations.csv`.
  - `scripts/`: Upload `spark_job1.py`, `spark_job2.py`.
  - `processed/`: For Parquet outputs.
  - `logs/`: For EMR logs.
  - `athena-results/`: For Athena query results (subfolders: `top-location/`, `top-vehicle/`, `top-users/`, `daily-metrics/`).
- **Console Steps**:
  - Navigate to **S3 Console** (`https://s3.console.aws.amazon.com/s3/home`).
  - Create bucket `<bucket_name>`.
  - Create folders and upload files as listed.

### 2. IAM Roles

- **Lab4EMRServerlessRole**:
  ```json
  Add AmazonEMRServerlessFullAccess and AmazonS3FullAccess
  ```
  - Trust relationship: `emr-serverless.amazonaws.com`.
- **AWSGlueServiceRole**:
  - Policies: `AWSGlueServiceRole`, `AmazonS3FullAccess`.
  - Trust relationship: `glue.amazonaws.com`.
- **StepFunctionsRole**:
  ```json
  Add AmazonEMRServerlessFullAccess, AmazonS3FullAccess, AWSGlueServiceRole, AmazonAthenaFullAccess, CloudWatchFullAccess
  ```
  - Trust relationship: `states.amazonaws.com`.
- **Console Steps**:
  - In **IAM Console**, create/update roles with the above policies.

### 3. EMR Serverless Application

- **Configuration**:
  - Name: `CarRentalEMRServerless`.
  - Type: Spark.
  - Release: `emr-7.2.0` (or latest).
  - Runtime role: `Lab4EMRServerlessRole`.
- **Console Steps**:
  - Navigate to **EMR Serverless** (`https://console.aws.amazon.com/emr/home#/serverless`).
  - Create application if not exists.
  - Note our application ID.

### 4. Spark Scripts

- **spark_job1.py**: Computes vehicle and location metrics (e.g., revenue per location, rental duration by vehicle type).
- **spark_job2.py**: Computes user and transaction metrics (e.g., daily transactions, user spending).
- **Location**: `s3://<bucket_name>/scripts/`.

### 5. Glue Crawlers

- **Database**: `car_rental_db`.
- **Crawlers**:
  - `LocationMetricsCrawler`: `s3://<bucket_name>/processed/location_metrics/`.
  - `VehicleMetricsCrawler`: `s3://<bucket_name>/processed/vehicle_metrics/`.
  - `DailyMetricsCrawler`: `s3://<bucket_name>/processed/daily_metrics/`.
  - `UserMetricsCrawler`: `s3://<bucket_name>/processed/user_metrics/`.
- **Console Steps**:
  - In **Glue Console**, create database `car_rental_db`.
  - Create crawlers with the above S3 paths, role `AWSGlueServiceRole`, and output to `car_rental_db`.

### 6. Athena Setup

- **Query Location**: `s3://<bucket_name>/athena-results/`.
- **Database**: `car_rental_db`.
- **Console Steps**:
  - In **Athena Console** (`https://console.aws.amazon.com/athena/`), set query result location.
  - Select `car_rental_db` as the database.

### 7. Step Functions State Machine

- **Name**: Car Rental Serverless Data Pipeline.
- **ASL File**: `state_machine.json`.
- **Features**:
  - Parallel Spark jobs (`spark_job1.py`, `spark_job2.py`).
  - Parallel Glue crawlers.
  - Parallel Athena queries for KPIs.
- **Console Steps**:
  - In **Step Functions Console**, create/edit state machine.
  - Paste ASL from `state_machine.json`.
  - Set IAM role to `StepFunctionsRole`.
  - Save.

## Execution

1. **Upload Data**:

   - Ensure raw CSV files are in `s3://<bucket_name>/raw/` folders.

2. **Test Spark Jobs** (optional):

   - In **EMR Serverless Console**, submit jobs:
     - Job 1: Script=`s3://<bucket_name>/scripts/spark_job1.py`, Name=`VehicleLocationMetrics`.
     - Job 2: Script=`s3://<bucket_name>/scripts/spark_job2.py`, Name=`UserTransactionAnalysis`.
   - Check outputs in `s3://<bucket_name>/processed/`.

3. **Run Crawlers** (optional):

   - In **Glue Console**, run crawlers manually.
   - Verify tables in `car_rental_db`.

4. **Test Athena Queries** (optional):

   - In **Athena Console**, run:
     ```sql
     SELECT location_name, total_revenue FROM car_rental_db.location_metrics ORDER BY total_revenue DESC LIMIT 1;
     SELECT vehicle_type, total_revenue FROM car_rental_db.vehicle_metrics ORDER BY total_revenue DESC LIMIT 1;
     SELECT first_name, last_name, total_revenue FROM car_rental_db.user_metrics ORDER BY total_revenue DESC LIMIT 5;
     SELECT rental_date, total_transactions, total_revenue FROM car_rental_db.daily_metrics ORDER BY total_revenue DESC LIMIT 10;
     ```

5. **Run Pipeline**:
   - In **Step Functions Console**, start execution.
   - Monitor the graph for parallel Spark jobs, crawlers, and Athena queries.
   - Check outputs in `s3://<bucket_name>/processed/` and `athena-results/`.

## Outputs

- **Processed Data** (Parquet):
  - `s3://<bucket_name>/processed/location_metrics/`: Revenue, transactions, etc., per location.
  - `s3://<bucket_name>/processed/vehicle_metrics/`: Revenue, rental duration by vehicle type.
  - `s3://<bucket_name>/processed/daily_metrics/`: Daily transactions and revenue.
  - `s3://<bucket_name>/processed/user_metrics/`: User spending, rental days.
- **Athena Results** (CSV):
  - `s3://<bucket_name>/athena-results/top-location/`: Top revenue location.
  - `s3://<bucket_name>/athena-results/top-vehicle/`: Top vehicle type by revenue.
  - `s3://<bucket_name>/athena-results/top-users/`: Top 5 spending users.
  - `s3://<bucket_name>/athena-results/daily-metrics/`: Top 10 daily metrics.

## KPIs

- **Location and Vehicle Performance**:
  - Total revenue, transactions, avg/max/min transaction amounts per location.
  - Unique vehicles per location.
  - Rental duration by vehicle type.
- **User and Transaction Metrics**:
  - Daily transactions and revenue.
  - User-specific spending, rental days, avg/max/min transaction amounts.

## Troubleshooting

1. **Crawler Failures**:

   - **Issue**: No tables in `car_rental_db`.
     - **Fix**: Ensure S3 paths contain Parquet files. Check crawler logs in Glue Console.

2. **Athena Query Failures**:

   - **Issue**: Tables missing.
     - **Fix**: Rerun crawlers and verify `car_rental_db` tables.

3. **Permission Errors**:
   - Check CloudWatch logs (`/aws/states/<state-machine-name>`).
   - Ensure IAM roles have required permissions for S3, EMR Serverless, Glue, and Athena.

##
