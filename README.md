# Baseball ETL Pipeline

## Overview

This project implements an ETL (Extract, Transform, Load) pipeline for baseball statistics using data from Kaggle's ["History of Baseball" dataset](https://www.kaggle.com/datasets/seanlahman/the-history-of-baseball). The pipeline extracts data from the Kaggle API, cleans and transforms it with Pandas, loads the results into an SQLite database, and is orchestrated with Prefect. Automated scheduling, error handling, and Slack notifications are included.


## Project Structure
```
baseball_etl_flow.py       # Main ETL pipeline script
requirements.txt           # List of Python dependencies
data/                     # Folder to store raw and transformed data files
baseball_data.db          # SQLite database where transformed data is loaded
README.md                 # Documentation of the project
app.py                    # Streamlit App
```

## Project Requirements

- **Data source:** Kaggle API for data extraction.
- **Data transformation:** Pandas for merging, cleaning, and imputing missing values.
- **Data storage:** SQLite database for storing the transformed data.
- **Orchestration:** Prefect for workflow management and scheduling.
- **Error handling:** Prefect retries and Slack notifications for failure notifications.
- **Automated scheduling:** Scheduling handled via Prefect UI/CLI.


## Data Source

- **Dataset:** ["History of Baseball" dataset](https://www.kaggle.com/datasets/seanlahman/the-history-of-baseball)
- **Access:** Requires a Kaggle account and API key.


## Installation & Setup

### Prerequisites

- Python
- `prefect`
- `pandas`
- `sqlite3` 
- `kaggle`
- `slack_sdk` 
- `streamlit`

### 1. Clone the Repository

```git clone https://github.com/eo118/baseball-etl-pipeline.git```

```cd baseball-etl-pipeline```

### 2. Install Dependencies

```pip install -r requirements.txt```


### 3. Kaggle API Setup

1. Go to your Kaggle account settings and click **Create New API Token**.
2. Download the `kaggle.json` file.
3. Place `kaggle.json` in a `.kaggle` directory at the project root:
```<project-root>/.kaggle/kaggle.json```
Or set the `KAGGLE_CONFIG_DIR` environment variable to the folder containing `kaggle.json`.

### 4. (Optional) Slack Notifications

1. In the Prefect UI, go to **Blocks > Add Block > Slack Webhook**.
2. Name the block `my-slack-block`.
3. Paste your Slack webhook URL and save.
4. If you do not want Slack notifications, comment out or remove the notification code in `baseball_etl_flow.py`.


## Running the Pipeline

### Option 1: Run Locally

```python baseball_etl_flow.py```

This will execute the pipeline and perform the ETL steps (Extract, Transform, Load).

### Option 2: Run with Prefect CLI

1. **Create and apply a deployment:**
```prefect deployment build baseball_etl_flow.py:baseball_etl_flow -n daily-baseball-etl```
```prefect deployment apply baseball_etl_flow-deployment.yaml```


2. **Run the deployment manually:**
```prefect deployment run 'Baseball ETL Flow/daily-baseball-etl'```


### Option 3: Schedule with Prefect

To schedule the flow to run daily at 9 AM:

```prefect deployment schedule set daily-baseball-etl --cron "0 9 * * *"```


## Prefect Server and Ephemeral Mode

- **Ephemeral (Temporary) Server:**  
  If you run the pipeline script directly and have not started a Prefect server or set an API URL, Prefect starts a temporary local server for the duration of the flow run and shuts it down automatically.
- **Persistent Prefect Server:**  
  For ongoing orchestration and monitoring, start a persistent server:
```prefect server start```

This starts the Prefect UI at [http://127.0.0.1:4200](http://127.0.0.1:4200), where you can monitor and manage your flow.


## Pipeline Steps

1. **Extract:**  
 The `extract_data` task downloads the dataset from Kaggle using the Kaggle API and stores the files in the `data/` directory.
2. **Transform:**  
 The `transform_data` task processes the raw data:
 - Merges `batting.csv` and `salary.csv` files.
 - Imputes missing values in numeric and categorical columns.
 - Logs missing value counts and record counts.
 - Saves cleaned data as `data/transformed_data.csv`.
3. **Load:**  
 - Loads cleaned data into a local SQLite database (`baseball_data.db`).
 - Logs a sample of the data and the average salary by team.
4. **Error Handling, Retries, and Notifications:**  
 - Each task uses try/except blocks and Prefect retries (3 times, 10-second delay).
 - Slack notifications are sent on failure if configured.
5. **Scheduling:**  
 - Scheduling is handled through Prefect UI or CLI, not in code.
 - You can schedule the flow using Prefect's built-in cron or interval schedules.


## File Paths

- Relative paths are used for the file locations to ensure the pipeline runs smoothly across different environments.
- This ensures portability, and the file paths should be relative to the root directory of the project.


## Performance and Scalability

- **Database:** The pipeline currently uses SQLite, which is suitable for small to medium-sized datasets.
- **Scalability:** For larger datasets or production scenarios, consider switching to more robust databases like PostgreSQL or AWS RDS. Batch loading and chunked operations with Pandas can also be used for scaling the pipeline.


## Project Improvements and Future Work

- Use a more robust database for production (PostgreSQL, AWS RDS).
- Implement batch loading for large datasets.
- Add more sophisticated error handling and logging.

## Streamlit App: Interactive Data Exploration & Salary Prediction

I included a Streamlit web app for interactive exploration and analysis of the baseball data produced by the ETL pipeline.

### Features

- Filter players by salary range
- View top 5 home run hitters
- Summary statistics of the dataset
- Train a linear regression model to predict player salary based on HR and RBI
- Make salary predictions using the trained model

### How to Run the App

1. **Ensure the ETL pipeline has been run** and both `data/transformed_data.csv` and `baseball_data.db` exist.
2. **Install Streamlit** (if not already installed):

    ```
    pip install streamlit
    ```

3. **Start the Streamlit app** from project directory:

    ```
    streamlit run app.py
    ```

4. **Interact with the app** in your browser at [http://localhost:8501](http://localhost:8501).

### Requirements

- The ETL pipeline must have already run to generate the necessary data files.
- All dependencies in `requirements.txt` (add `streamlit` if not already present).
This app demonstrates how to use the pipeline’s output for real data analysis and machine learning, and provides an interactive interface for users to explore baseball statistics.


## Conclusion

This project demonstrates how to automate an ETL pipeline using Prefect, including scheduling, error handling, and notifications. The pipeline extracts, transforms, and loads baseball statistics data into an SQLite database, making it ready for analysis or further processing.

