import os
import pandas as pd
import numpy as np
import streamlit as st
import sqlite3
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import OneHotEncoder

# Configuration
csv_file_path = 'data/transformed_data.csv'
db_file_path = 'baseball_data.db'

# Data loading and Preprocessing

@st.cache_data
def load_data():
    if not os.path.exists(csv_file_path):
        st.error(f"File not found: {csv_file_path}. Please run the ETL pipeline first.")
        return None
    df = pd.read_csv(csv_file_path)
    return df

def preprocess_data(df):
    encoder = OneHotEncoder(sparse_output=False, drop='first')
    encoded_features = encoder.fit_transform(df[['team_id', 'league_id']])
    encoded_df = pd.DataFrame(encoded_features, columns=encoder.get_feature_names_out(['team_id', 'league_id']))
    df = pd.concat([df.reset_index(drop=True), encoded_df], axis=1).drop(columns=['team_id', 'league_id'])
    return df, encoder

# Model Training

def train_model(df):
    X = df[['hr', 'rbi']]
    y = df['salary']
    model = LinearRegression()
    model.fit(X, y)
    return model

# Database Query

def query_data(query):
    if not os.path.exists(db_file_path):
        st.error(f"Database not found: {db_file_path}. Please run the ETL pipeline first.")
        return []
    conn = sqlite3.connect(db_file_path)
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()
    return results

# Streamlit App

st.title('⚾ Baseball Player Salary Prediction and Analysis')

# Section 1: Filter by Salary Range
st.subheader("Filter by Salary Range")
min_salary, max_salary = st.slider("Salary Range", 0, 30000000, (500000, 10000000), step=50000)
if st.button("Show Players Within Salary Range"):
    query = f"""
        SELECT player_id, year, team_id, salary 
        FROM batting_data 
        WHERE salary BETWEEN {min_salary} AND {max_salary}
    """
    results = query_data(query)
    if results:
        df_players = pd.DataFrame(results, columns=["Player ID", "Year", "Team ID", "Salary"])
        st.dataframe(df_players)
    else:
        st.info("No players found in this salary range.")

# Section 2: Top 5 Players by Home Runs
st.subheader("Top 5 Players by Home Runs")
if st.button("Show Top 5 Home Run Hitters"):
    query = """
        SELECT player_id, year, hr
        FROM batting_data 
        ORDER BY hr DESC LIMIT 5
    """
    results = query_data(query)
    if results:
        df_home_runs = pd.DataFrame(results, columns=["Player ID", "Year", "Home Runs"])
        st.dataframe(df_home_runs)
    else:
        st.info("No data available.")

# Section 3: Load Dataset and Train Model
st.subheader("Train Salary Prediction Model")
if 'model_trained' not in st.session_state:
    st.session_state.model_trained = False
if 'model' not in st.session_state:
    st.session_state.model = None

if st.button('Load Dataset and Train Model'):
    df = load_data()
    if df is not None:
        st.write(df.head())
        df_processed, encoder = preprocess_data(df)
        model = train_model(df_processed)
        st.session_state.model = model
        st.session_state.model_trained = True
        st.success("Model trained successfully!")
        # Show summary stats
        summary = {
            'Total players': len(df),
            'Average salary': f"${df['salary'].mean():,.2f}",
            'Min salary': f"${df['salary'].min():,.2f}",
            'Max salary': f"${df['salary'].max():,.2f}",
            'Average home runs': f"{df['hr'].mean():.2f}",
            'Average RBI': f"{df['rbi'].mean():.2f}"
        }
        st.write("**Summary Statistics:**")
        st.json(summary)

# Section 4: Predict Player Salary
if st.session_state.model_trained and st.session_state.model is not None:
    st.subheader('Predict Player Salary')
    hr = st.number_input('Home Runs (HR)', min_value=0, max_value=100, value=10)
    rbi = st.number_input('Runs Batted In (RBI)', min_value=0, max_value=200, value=50)
    if st.button('Predict Salary'):
        input_data = np.array([[hr, rbi]])
        predicted_salary = st.session_state.model.predict(input_data)[0]
        st.success(f"Predicted Salary: ${predicted_salary:,.2f}")
else:
    st.info("Click 'Load Dataset and Train Model' to train the model first.")

# Footer
st.markdown("---")
st.caption("Data source: Kaggle Lahman Baseball Dataset | ETL by Prefect | App by Streamlit")
