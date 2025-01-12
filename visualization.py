# visualization.py
# This script handles the visualization of vehicle sensor data and analytics results.
# It generates dashboards for vehicle performance, anomalies, and other metrics using libraries like Matplotlib.

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Function to load analytics results from Parquet or CSV file
def load_data(file_path):
    """
    Loads the analytics results from a specified file path.
    
    Args:
        file_path (str): The path to the file containing the analytics data (Parquet or CSV).
    
    Returns:
        DataFrame: The loaded data.
    """
    try:
        # Load the analytics data (can be CSV or Parquet based on the format used)
        df = pd.read_parquet(file_path)  # Change to .csv for CSV files
        print(f"Data loaded from {file_path}")
        return df
    except Exception as e:
        print(f"Error loading data: {e}")
        return None

# Function to visualize real-time vehicle performance
def visualize_performance(df):
    """
    Visualizes the vehicle performance based on key metrics (e.g., speed, proximity, anomalies).
    
    Args:
        df (DataFrame): The analytics data to visualize.
    """
    try:
        # Plot the distribution of speeds across all vehicles
        plt.figure(figsize=(10, 6))
        sns.histplot(df['speed'], kde=True, bins=30, color='blue')
        plt.title('Speed Distribution of Vehicles')
        plt.xlabel('Speed (km/h)')
        plt.ylabel('Frequency')
        plt.show()

        # Plot anomalies (e.g., speed anomalies)
        plt.figure(figsize=(10, 6))
        sns.countplot(x='speed_anomaly', data=df, palette='coolwarm')
        plt.title('Speed Anomalies in Vehicle Data')
        plt.xlabel('Anomaly Flag (0 = No, 1 = Yes)')
        plt.ylabel('Count')
        plt.show()

        # Plot proximity anomalies
        plt.figure(figsize=(10, 6))
        sns.countplot(x='obstacle_anomaly', data=df, palette='coolwarm')
        plt.title('Obstacle Proximity Anomalies')
        plt.xlabel('Anomaly Flag (0 = No, 1 = Yes)')
        plt.ylabel('Count')
        plt.show()

        print("Visualization complete.")
    except Exception as e:
        print(f"Error during visualization: {e}")

# Main function to load data and generate visualizations
def main():
    """
    Main function to generate visualizations from the analytics data.
    """
    # Load the analytics data (adjust file path as necessary)
    file_path = "/path/to/output/real_time_analytics_results.parquet"  # Example path
    data = load_data(file_path)

    if data is not None:
        # Generate visualizations
        visualize_performance(data)

if __name__ == "__main__":
    main()
