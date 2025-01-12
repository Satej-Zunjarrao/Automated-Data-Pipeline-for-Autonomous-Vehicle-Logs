# feature_engineering.py
# This script is responsible for deriving additional features from raw sensor data.
# These features may include speed, proximity to obstacles, and event detection based on sensor inputs.

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import DoubleType

# Function to initialize the Spark session
def init_spark_session():
    """
    Initializes and returns a Spark session to handle large-scale data processing.
    """
    try:
        spark = SparkSession.builder \
            .appName("Autonomous Vehicle Feature Engineering") \
            .getOrCreate()
        print("Spark session initialized successfully.")
        return spark
    except Exception as e:
        print(f"Error initializing Spark session: {e}")
        return None

# Function to load preprocessed sensor data from a Parquet or CSV file
def load_data(file_path, spark):
    """
    Loads preprocessed sensor data into a Spark DataFrame.
    
    Args:
        file_path (str): Path to the preprocessed sensor data file (CSV/Parquet).
        spark (SparkSession): The active Spark session.

    Returns:
        DataFrame: The loaded sensor data as a Spark DataFrame.
    """
    try:
        # Load data (can be CSV or Parquet based on the format used)
        df = spark.read.option("header", "true").csv(file_path)  # Change to .parquet for Parquet files
        print(f"Data loaded from {file_path}")
        return df
    except Exception as e:
        print(f"Error loading data: {e}")
        return None

# Function to perform feature engineering on the sensor data
def feature_engineering(df):
    """
    Performs feature engineering on the raw sensor data, including calculations like speed, proximity to obstacles, etc.
    
    Args:
        df (DataFrame): The preprocessed sensor data as a Spark DataFrame.
    
    Returns:
        DataFrame: The data with additional features (e.g., speed, proximity).
    """
    try:
        # Calculate speed (simplified as distance/time)
        df = df.withColumn("speed", (col("gps").cast("double") / 10.0).cast(DoubleType()))  # Example speed calc
        
        # Calculate proximity to obstacles (simplified for illustration)
        df = df.withColumn("proximity", (col("lidar").cast("double") / 5.0).cast(DoubleType()))  # Example proximity calc
        
        # Detect braking events (example: if speed decreases by more than a threshold within a time window)
        df = df.withColumn("braking_event", when(col("speed") < 5, lit(1)).otherwise(lit(0)))
        
        # Add a column for whether the vehicle is near an obstacle (threshold can be adjusted)
        df = df.withColumn("near_obstacle", when(col("proximity") < 2, lit(1)).otherwise(lit(0)))

        print("Feature engineering complete.")
        return df
    except Exception as e:
        print(f"Error during feature engineering: {e}")
        return None

# Function to save the engineered features to a Parquet file or any other format
def save_data(df, output_path):
    """
    Saves the feature-engineered data to a specified output path.
    
    Args:
        df (DataFrame): The feature-engineered sensor data.
        output_path (str): The path to save the processed data (e.g., S3 or local file system).
    """
    try:
        # Save the data to Parquet (could also be CSV or any other format)
        df.write.parquet(output_path)
        print(f"Feature-engineered data saved to {output_path}")
    except Exception as e:
        print(f"Error saving data: {e}")

# Main function to load, perform feature engineering, and save the results
def main():
    """
    Main function to load the preprocessed data, apply feature engineering, and save the results.
    """
    # Initialize Spark session
    spark = init_spark_session()

    if spark:
        # Load the preprocessed sensor data (use the correct path)
        file_path = "/path/to/preprocessed/sensor_data.csv"  # Example path, replace with actual location
        data = load_data(file_path, spark)

        if data:
            # Perform feature engineering
            feature_data = feature_engineering(data)

            # Save the processed data to output location (e.g., AWS S3 or local file system)
            output_path = "/path/to/output/feature_data.parquet"  # Example path, replace with actual location
            save_data(feature_data, output_path)

if __name__ == "__main__":
    main()
