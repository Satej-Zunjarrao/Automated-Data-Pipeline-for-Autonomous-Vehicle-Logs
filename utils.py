# utils.py
# This script contains utility functions used across multiple modules, including logging, error handling, and data validation.

import logging
import os

# Set up logging configuration
def setup_logging(log_level=logging.INFO):
    """
    Sets up logging configuration for the project.
    
    Args:
        log_level (int): The logging level to set (e.g., logging.INFO, logging.DEBUG).
    """
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=log_level
    )
    logging.info("Logging setup complete.")

# Function to validate if a given file exists
def validate_file_exists(file_path):
    """
    Validates whether the specified file exists at the given path.
    
    Args:
        file_path (str): The path to the file to check.
    
    Returns:
        bool: True if the file exists, False otherwise.
    """
    if os.path.exists(file_path):
        logging.info(f"File found: {file_path}")
        return True
    else:
        logging.error(f"File not found: {file_path}")
        return False

# Function to check if a directory exists
def validate_directory_exists(directory_path):
    """
    Validates whether the specified directory exists.
    
    Args:
        directory_path (str): The directory path to check.
    
    Returns:
        bool: True if the directory exists, False otherwise.
    """
    if os.path.isdir(directory_path):
        logging.info(f"Directory exists: {directory_path}")
        return True
    else:
        logging.error(f"Directory not found: {directory_path}")
        return False

# Function to load JSON data from a file
def load_json_data(file_path):
    """
    Loads JSON data from the specified file.
    
    Args:
        file_path (str): The path to the JSON file to load.
    
    Returns:
        dict: The parsed JSON data as a Python dictionary.
    """
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
        logging.info(f"JSON data loaded successfully from {file_path}")
        return data
    except Exception as e:
        logging.error(f"Error loading JSON data from {file_path}: {e}")
        return None

# Function to save JSON data to a file
def save_json_data(data, file_path):
    """
    Saves the provided data to a JSON file.
    
    Args:
        data (dict): The data to save in JSON format.
        file_path (str): The file path where to save the data.
    """
    try:
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)
        logging.info(f"JSON data saved successfully to {file_path}")
    except Exception as e:
        logging.error(f"Error saving JSON data to {file_path}: {e}")

# Function to handle and log exceptions
def handle_exception(exception):
    """
    Handles and logs exceptions with detailed information.
    
    Args:
        exception (Exception): The exception to handle.
    """
    logging.error(f"Exception occurred: {str(exception)}")
    # You can also send notifications or perform other actions based on the exception type

# Main function to test utility functions
def main():
    """
    Main function to test utility functions.
    """
    # Set up logging
    setup_logging()

    # Example file path validation
    file_path = "/path/to/data/file.csv"
    validate_file_exists(file_path)

    # Example directory validation
    directory_path = "/path/to/data/directory"
    validate_directory_exists(directory_path)

if __name__ == "__main__":
    main()

