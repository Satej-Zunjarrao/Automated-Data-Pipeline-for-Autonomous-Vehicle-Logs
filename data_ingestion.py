# data_ingestion.py
# This script handles the ingestion of raw sensor data from autonomous vehicles
# It uses Apache Kafka for real-time streaming and integrates multiple data sources (GPS, LiDAR, radar, camera feeds)

import kafka
from kafka import KafkaProducer
import json
import time

# Constants for Kafka configuration
KAFKA_BROKER = "your-kafka-broker-url"
KAFKA_TOPIC = "vehicle-sensor-data"

# Function to initialize Kafka producer
def init_kafka_producer():
    """
    Initializes and returns a Kafka producer to send data to the specified topic.
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Kafka Producer initialized successfully.")
        return producer
    except Exception as e:
        print(f"Error initializing Kafka producer: {e}")
        return None

# Function to simulate vehicle sensor data and send it to Kafka
def simulate_sensor_data(vehicle_id, sensor_data):
    """
    Simulates the generation of sensor data for a specific vehicle and sends it to Kafka.
    
    Args:
        vehicle_id (str): The unique ID of the vehicle generating the data.
        sensor_data (dict): The raw sensor data, such as GPS, LiDAR, radar, camera feeds, etc.
    """
    try:
        # Construct the payload that will be sent to Kafka
        payload = {
            "vehicle_id": vehicle_id,
            "timestamp": int(time.time()),  # Unix timestamp
            "sensor_data": sensor_data  # This contains raw sensor values
        }

        # Send the payload to the Kafka topic
        producer.send(KAFKA_TOPIC, value=payload)
        print(f"Data from vehicle {vehicle_id} sent to Kafka topic {KAFKA_TOPIC}")
    except Exception as e:
        print(f"Error sending data to Kafka: {e}")

# Example sensor data for testing (replace with actual sensor data collection logic)
example_sensor_data = {
    "gps": {"lat": 37.7749, "long": -122.4194},  # GPS coordinates (example)
    "lidar": [1.0, 2.0, 3.0, 4.0],  # LiDAR data points (example)
    "radar": [0.5, 1.5, 2.5],  # Radar data points (example)
    "camera": ["frame1.jpg", "frame2.jpg"]  # Camera feed frames (example)
}

# Main function to run the data ingestion process
def main():
    """
    Main function to initialize the Kafka producer and simulate sensor data ingestion.
    """
    vehicle_id = "vehicle-001"  # Example vehicle ID
    producer = init_kafka_producer()

    if producer:
        while True:
            # Simulate sending data every 5 seconds (can be adjusted as per real-time requirements)
            simulate_sensor_data(vehicle_id, example_sensor_data)
            time.sleep(5)  # Simulate data collection at regular intervals (e.g., every 5 seconds)

if __name__ == "__main__":
    main()
