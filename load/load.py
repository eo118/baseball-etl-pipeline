from sqlalchemy import create_engine
import pandas as pd

def load_data():
    # Ensure your database connection string is correct
    # Example: 'postgresql://username:password@localhost:5432/mydatabase'
    engine = create_engine('postgresql://username:password@localhost:5432/mydatabase')

    # Assuming df_combined is the dataframe you're working with
    df_combined = pd.read_csv('path_to_your_data.csv')  # Ensure you load your data properly
    
    # Save DataFrame to SQL
    df_combined.to_sql('player_stats', engine, if_exists='replace', index=False)

load_data()
