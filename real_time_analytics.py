# real_time_analytics.py
# This script performs real-time analytics on sensor data for vehicle diagnostics and performance monitoring.
# It queries the processed data stored in S3 and performs SQL-based analysis for anomaly detection and reporting.

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, when
from pyspark.sql.types import DoubleType

# Function to initialize the Spark session
def init_spark_session():
    """
    Initializes and returns a Spark session for distributed data processing.
    """
    try:
        spark = SparkSession.builder \
            .appName("Real-Time Vehicle Analytics") \
            .getOrCreate()
        print("Spark session initialized successfully.")
        return spark
    except Exception as e:
        print(f"Error initializing Spark session: {e}")
        return None

# Function to query the processed data and perform real-time analytics
def perform_real_time_analytics(df):
    """
    Perform real-time analytics on the vehicle sensor data, such as anomaly detection and performance monitoring.
    
    Args:
        df (DataFrame): The processed sensor data to analyze.
    
    Returns:
        DataFrame: The data with computed metrics for diagnostics.
    """
    try:
        # Example: Calculate the average speed for each vehicle
        avg_speed_df = df.groupBy("vehicle_id").agg(avg("speed").alias("average_speed"))

        # Example: Detect anomalies (e.g., speed exceeding a threshold)
        anomaly_df = df.withColumn(
            "speed_anomaly", 
            when(col("speed") > 100, 1).otherwise(0)  # Flag speeds over 100 as anomalies
        )
        
        # Example: Calculate proximity to obstacles and flag extreme proximity
        anomaly_df = anomaly_df.withColumn(
            "obstacle_anomaly", 
            when(col("proximity") < 1, 1).otherwise(0)  # Flag proximity less than 1 as an anomaly
        )

        print("Real-time analytics performed successfully.")
        return anomaly_df
    except Exception as e:
        print(f"Error performing real-time analytics: {e}")
        return None

# Function to save the analysis results for further reporting or visualization
def save_analytics_results(df, output_path):
    """
    Saves the real-time analytics results to a specified output path (e.g., S3 or local).
    
    Args:
        df (DataFrame): The DataFrame containing the analytics results.
        output_path (str): The output path to save the results.
    """
    try:
        # Save the results in Parquet format for efficient querying
        df.write.mode("overwrite").parquet(output_path)
        print(f"Real-time analytics results saved to {output_path}")
    except Exception as e:
        print(f"Error saving analytics results: {e}")

# Main function to run real-time analytics on vehicle sensor data
def main():
    """
    Main function to perform real-time analytics on the vehicle data.
    """
    # Initialize Spark session
    spark = init_spark_session()

    if spark:
        # Load the processed sensor data from a file (could be S3 or local)
        file_path = "/path/to/processed/vehicle_data.parquet"  # Example path
        df = spark.read.parquet(file_path)

        # Perform real-time analytics
        analytics_results = perform_real_time_analytics(df)

        # Save the analytics results (e.g., to S3)
        output_path = "/path/to/output/real_time_analytics_results.parquet"  # Example path
        save_analytics_results(analytics_results, output_path)

if __name__ == "__main__":
    main()
