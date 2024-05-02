import os
import pandas as pd


def json_to_parquet(json_folder, parquet_folder):
    # Create Parquet folder if it doesn't exist
    if not os.path.exists(parquet_folder):
        os.makedirs(parquet_folder)

    # Iterate through each JSON file in the folder
    for filename in os.listdir(json_folder):
        if filename.endswith('.json'):
            json_file_path = os.path.join(json_folder, filename)
            parquet_file_path = os.path.join(parquet_folder, filename.replace('.json', '.parquet'))

            # Read JSON file into a DataFrame
            df = pd.read_json(json_file_path)

            # Write DataFrame to Parquet
            df.to_parquet(parquet_file_path)

# Conversions
json_folder = r'Data\JSON\General\MLB'
parquet_folder = r'Data\Parquet\General\MLB'
json_to_parquet(json_folder, parquet_folder)

json_folder = r'Data\JSON\General\MLS'
parquet_folder = r'Data\Parquet\General\MLS'
json_to_parquet(json_folder, parquet_folder)

json_folder = r'Data\JSON\General\NBA'
parquet_folder = r'Data\Parquet\General\NBA'
json_to_parquet(json_folder, parquet_folder)

json_folder = r'Data\JSON\General\NFL'
parquet_folder = r'Data\Parquet\General\NFL'
json_to_parquet(json_folder, parquet_folder)

json_folder = r'Data\JSON\General\NHL'
parquet_folder = r'Data\Parquet\General\NHL'
json_to_parquet(json_folder, parquet_folder)