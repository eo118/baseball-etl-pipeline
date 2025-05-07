import os
import pandas as pd

# Function to clean and process the data
def clean_data():
    # Load the extracted data from CSV files
    batting_file_path = os.path.join('data', '2023 MLB Player Stats - Batting.csv')
    pitching_file_path = os.path.join('data', '2023 MLB Player Stats - Pitching.csv')
    
    df_batting = pd.read_csv(batting_file_path, encoding='ISO-8859-1', sep=';')
    df_pitching = pd.read_csv(pitching_file_path, encoding='ISO-8859-1', sep=';')

    # Handle missing values
    df_batting = df_batting.fillna(0)
    df_pitching = df_pitching.fillna(0)

    # Renaming columns for clarity
    df_batting.rename(columns={'Rk': 'Rk_batting', 'Age': 'Age_batting', 'Tm': 'Tm_batting'}, inplace=True)
    df_pitching.rename(columns={'Rk': 'Rk_pitching', 'Age': 'Age_pitching', 'Tm': 'Tm_pitching'}, inplace=True)

    # Merge the batting and pitching data on 'Name'
    df_combined = pd.merge(df_batting, df_pitching, on='Name', how='inner')

    # Save the cleaned data into a new CSV file
    df_combined.to_csv('data/cleaned_mlb_data.csv', index=False)

    print("Data cleaned and saved successfully.")

# Call the clean_data function
if __name__ == "__main__":
    clean_data()
