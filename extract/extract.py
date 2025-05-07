import os
from kaggle.api.kaggle_api_extended import KaggleApi

# Define the data source
dataset_name = "vivovinco/2023-mlb-player-stats"

# Function to download the data
def download_data():
    # Set Kaggle API credentials
    os.environ['KAGGLE_CONFIG_DIR'] = '/path/to/.kaggle'  # Adjust path to your kaggle.json

    # Initialize the Kaggle API client
    api = KaggleApi()
    api.authenticate()
    
    # Download dataset to the 'data' folder
    api.dataset_download_files(dataset_name, path='data', unzip=True)
    print("Data downloaded successfully.")

# Call the function to download data
if __name__ == "__main__":
    download_data()
