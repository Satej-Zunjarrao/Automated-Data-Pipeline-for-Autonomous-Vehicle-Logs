# data_preprocessing.py
# This script handles the preprocessing and transformation of raw sensor logs.
# It uses PySpark for distributed computing to efficiently process large datasets.

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp
from datetime import datetime
import pandas as pd

# Initialize the Spark session
def init_spark_session():
    """
    Initializes and returns a Spark session for distributed data processing.
    """
    try:
        spark = SparkSession.builder \
            .appName("Autonomous Vehicle Data Preprocessing") \
            .getOrCreate()
        print("Spark session initialized successfully.")
        return spark
    except Exception as e:
        print(f"Error initializing Spark session: {e}")
        return None

# Function to load raw data into a Spark DataFrame
def load_raw_data(file_path, spark):
    """
    Loads raw sensor log data from a specified file path into a Spark DataFrame.
    
    Args:
        file_path (str): The file path where the raw sensor logs are stored.
        spark (SparkSession): The active Spark session.

    Returns:
        DataFrame: The loaded data as a Spark DataFrame.
    """
    try:
        # Load data into a Spark DataFrame (assuming CSV format for simplicity)
        df = spark.read.option("header", "true").csv(file_path)
        print(f"Raw data loaded from {file_path}")
        return df
    except Exception as e:
        print(f"Error loading data: {e}")
        return None

# Function to clean and synchronize sensor logs based on timestamp
def preprocess_data(df):
    """
    Preprocesses the raw data by cleaning and synchronizing sensor logs based on timestamps.
    
    Args:
        df (DataFrame): The raw sensor log data as a Spark DataFrame.
    
    Returns:
        DataFrame: The cleaned and synchronized data.
    """
    try:
        # Convert timestamp from string to datetime
        df = df.withColumn("timestamp", unix_timestamp("timestamp").cast("timestamp"))
        
        # Drop rows with missing timestamps or invalid data
        df = df.filter(df.timestamp.isNotNull())
        
        # Synchronize data streams by joining on timestamp (if multiple sensor logs)
        df = df.orderBy("timestamp")
        
        print("Data preprocessing and synchronization complete.")
        return df
    except Exception as e:
        print(f"Error preprocessing data: {e}")
        return None

# Function to perform feature engineering (e.g., calculating speed, proximity to obstacles)
def feature_engineering(df):
    """
    Performs feature engineering to derive key metrics like speed, proximity to obstacles, etc.
    
    Args:
        df (DataFrame): The cleaned and synchronized data as a Spark DataFrame.
    
    Returns:
        DataFrame: The data with additional engineered features.
    """
    try:
        # Example: Calculate speed (based on GPS and time data)
        df = df.withColumn("speed", col("gps").cast("double") / 10.0)  # Example speed calculation
        
        # Example: Proximity to obstacles (based on LiDAR data)
        df = df.withColumn("proximity", col("lidar").cast("double") / 5.0)  # Example proximity calculation
        
        print("Feature engineering complete.")
        return df
    except Exception as e:
        print(f"Error during feature engineering: {e}")
        return None

# Main function to load, preprocess, and transform raw data
def main():
    """
    Main function to load raw data, preprocess it, and perform feature engineering.
    """
    # Initialize Spark session
    spark = init_spark_session()

    if spark:
        # Load raw sensor data (use the actual path for your data files)
        file_path = "/path/to/raw/sensor_data.csv"  # Example path, replace with actual location
        raw_data = load_raw_data(file_path, spark)

        if raw_data:
            # Preprocess and clean data
            processed_data = preprocess_data(raw_data)

            # Perform feature engineering
            feature_data = feature_engineering(processed_data)

            # Optionally, show the transformed data
            feature_data.show()

            # You can save the processed data back to S3 or another storage solution
            # feature_data.write.parquet("/path/to/output/processed_data.parquet")
        
if __name__ == "__main__":
    main()
