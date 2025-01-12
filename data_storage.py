# data_storage.py
# This script handles storing the processed data into AWS S3, ensuring that data is partitioned for efficient querying.
# AWS S3 is used as a cost-effective, scalable storage solution.

import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Function to initialize Spark session
def init_spark_session():
    """
    Initializes and returns a Spark session for data processing.
    """
    try:
        spark = SparkSession.builder \
            .appName("Autonomous Vehicle Data Storage") \
            .getOrCreate()
        print("Spark session initialized successfully.")
        return spark
    except Exception as e:
        print(f"Error initializing Spark session: {e}")
        return None

# Function to save data to AWS S3
def save_to_s3(df, s3_bucket, s3_prefix):
    """
    Saves the processed data to AWS S3 with appropriate partitioning for efficient querying.
    
    Args:
        df (DataFrame): The data to be saved to S3.
        s3_bucket (str): The S3 bucket name.
        s3_prefix (str): The prefix (directory path) within the bucket where data will be stored.
    """
    try:
        # Write the data to S3 with partitioning by vehicle_id and timestamp for optimized querying
        df.write \
            .partitionBy("vehicle_id", "timestamp") \
            .mode("overwrite") \
            .parquet(f"s3://{s3_bucket}/{s3_prefix}")
        
        print(f"Data successfully saved to S3: s3://{s3_bucket}/{s3_prefix}")
    except Exception as e:
        print(f"Error saving data to S3: {e}")

# Function to initialize AWS S3 client (for potential other storage operations)
def init_s3_client():
    """
    Initializes the AWS S3 client for direct interactions with S3 (e.g., listing or deleting files).
    
    Returns:
        S3 client object
    """
    try:
        s3_client = boto3.client('s3')
        print("AWS S3 client initialized successfully.")
        return s3_client
    except Exception as e:
        print(f"Error initializing S3 client: {e}")
        return None

# Main function to store processed data
def main():
    """
    Main function to process and store data in AWS S3.
    """
    # Initialize Spark session
    spark = init_spark_session()

    if spark:
        # Example: Load the feature-engineered data (adjust file path as necessary)
        feature_data_path = "/path/to/processed/feature_data.parquet"  # Example path, replace with actual path
        feature_data = spark.read.parquet(feature_data_path)

        # AWS S3 configuration
        s3_bucket = "your-s3-bucket-name"
        s3_prefix = "processed-data/vehicle-logs"

        # Save the data to S3
        save_to_s3(feature_data, s3_bucket, s3_prefix)

if __name__ == "__main__":
    main()
