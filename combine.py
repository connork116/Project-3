import os
import pandas as pd
import pyarrow.parquet as pq

# Function to combine Parquet files for each team into a single Parquet file for MLB
def combine_team_parquet_files(input_folder, output_file):
    # List to store data frames for each team
    team_dfs = []

    # Iterate through each Parquet file in the folder
    for filename in os.listdir(input_folder):
        if filename.endswith('.parquet'):
            parquet_file_path = os.path.join(input_folder, filename)

            # Read Parquet file into a DataFrame
            team_df = pd.read_parquet(parquet_file_path)

            # Add team name as a column
            team_name = filename.split('.')[0]  # Extract team name from filename
            team_df['Team'] = team_name

            # Append DataFrame to the list
            team_dfs.append(team_df)

    # Concatenate all team DataFrames
    mlb_df = pd.concat(team_dfs, ignore_index=True)

    # Write combined DataFrame to a new Parquet file
    mlb_df.to_parquet(output_file, index=False)

# Example usage


input_folder = r'Data\Parquet\General\NHL'
output_file = 'nhl_data.parquet'
combine_team_parquet_files(input_folder, output_file)

input_folder = r'Data\Parquet\General\MLB'
output_file = 'mlb_data.parquet'
combine_team_parquet_files(input_folder, output_file)

input_folder = r'Data\Parquet\General\MLS'
output_file = 'mls_data.parquet'
combine_team_parquet_files(input_folder, output_file)

input_folder = r'Data\Parquet\General\NBA'
output_file = 'nba_data.parquet'
combine_team_parquet_files(input_folder, output_file)

input_folder = r'Data\Parquet\General\NFL'
output_file = 'nfl_data.parquet'
combine_team_parquet_files(input_folder, output_file)
