import os
import sqlite3
import pandas as pd
from prefect import flow, task, get_run_logger
from kaggle.api.kaggle_api_extended import KaggleApi
from prefect.blocks.notifications import SlackWebhook

# Set the base directory for relative paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Task for extracting data
@task(retries=3, retry_delay_seconds=10, log_prints=True, name="Extract Data")
def extract_data():
    logger = get_run_logger()
    try:
        os.environ['KAGGLE_CONFIG_DIR'] = os.path.join(BASE_DIR, '.kaggle')
        api = KaggleApi()
        api.authenticate()
        api.dataset_download_files("seanlahman/the-history-of-baseball", path=os.path.join(BASE_DIR, 'data'), unzip=True)
        logger.info("Data downloaded successfully.")
    except Exception as e:
        logger.error(f"An error occurred while extracting data: {e}")
        raise

# Task for transforming data
@task(retries=3, retry_delay_seconds=10, log_prints=True, name="Transform Data")
def transform_data():
    logger = get_run_logger()
    try:
        # Define the relative file paths
        batting_file_path = os.path.join(BASE_DIR, 'data', 'batting.csv')  
        salary_file_path = os.path.join(BASE_DIR, 'data', 'salary.csv')

        # Read the datasets
        batting = pd.read_csv(batting_file_path)
        salary = pd.read_csv(salary_file_path)

        # Merge datasets
        data = pd.merge(batting, salary, on=["player_id", "year", "team_id", "league_id"])

        # Impute missing values for batting stats (numeric columns)
        batting_stats = ['ab', 'r', 'h', 'double', 'triple', 'hr', 'rbi', 'sb', 
                         'cs', 'bb', 'so', 'ibb', 'hbp', 'sh', 'sf', 'g_idp']
        data[batting_stats] = data[batting_stats].fillna(0)

        # Impute missing values for categorical columns
        categorical_cols = data.select_dtypes(include='object').columns.tolist()
        for col in categorical_cols:
            data[col] = data[col].fillna(data[col].mode()[0])

        # Check for missing values after transformation
        logger.info(f"Missing values after transformation: {data.isnull().sum()}")

        # Create the 'data' directory if it doesn't exist
        output_dir = os.path.join(BASE_DIR, 'data')
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Save the transformed data to the new CSV file
        output_file_path = os.path.join(output_dir, 'transformed_data.csv')
        data.to_csv(output_file_path, index=False)

        print(data.head())
        print(data.describe())

        logger.info(f"Transformation complete. Data saved to {output_file_path}")
        return output_file_path
    except Exception as e:
        logger.error(f"An error occurred during data transformation: {e}")
        raise

# Task for loading data into SQLite
@task(retries=3, retry_delay_seconds=10, log_prints=True, name="Load Data")
def load_data(output_file_path: str):
    logger = get_run_logger()
    try:
        # Load the transformed data
        data = pd.read_csv(output_file_path)
        db_path = os.path.join(BASE_DIR, 'baseball_data.db')

        # Create SQLite connection
        conn = sqlite3.connect('baseball_data.db')
        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS batting_data (
            player_id TEXT,
            year INTEGER,
            stint INTEGER,
            team_id TEXT,
            league_id TEXT,
            g INTEGER,
            ab INTEGER,
            r INTEGER,
            h INTEGER,
            double INTEGER,
            triple INTEGER,
            hr INTEGER,
            rbi INTEGER,
            sb INTEGER,
            cs INTEGER,
            bb INTEGER,
            so INTEGER,
            ibb INTEGER,
            hbp INTEGER,
            sh INTEGER,
            sf INTEGER,
            g_idp INTEGER,
            salary INTEGER
        )
        ''')

        # Load the transformed data into the table
        data.to_sql('batting_data', conn, if_exists='replace', index=False)

        # Verify by querying the table 
        cursor.execute("SELECT * FROM batting_data LIMIT 5;")
        logger.info(f"Sample data: {cursor.fetchall()}")

        # Query the average salary by team 
        cursor.execute("SELECT team_id, AVG(salary) FROM batting_data GROUP BY team_id")
        results = cursor.fetchall()

        for row in results:
            logger.info(f"Average salary by team: {row}")

        # Commit any changes and close the connection
        conn.commit()
        conn.close()

        logger.info(f"Data loaded successfully. Database saved at: {db_path}")
    except Exception as e:
        logger.error(f"An error occurred while loading data: {e}")
        raise

# Define your flow
@flow(name="Baseball ETL Flow")
def baseball_etl_flow():
    logger = get_run_logger()
    try:
        extract_data()
        output_file_path = transform_data()
        load_data(output_file_path)
        logger.info("Flow completed successfully!")
    except Exception as e:
        slack = SlackWebhook.load("my-slack-block")
        slack.notify(f"Flow failed: {str(e)}")
        raise

if __name__ == "__main__":
    baseball_etl_flow()
