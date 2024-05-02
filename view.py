import pyarrow.parquet as pq

# Path to the Parquet file
parquet_file = 'Data\Parquet\Category\MLS\CF_Montral_restaurants.parquet'

# Read the Parquet file
table = pq.read_table(parquet_file)

# Get the schema of the Parquet file
schema = table.schema

# Extract column names from the schema
column_names = schema.names

# Print the column names
print("Columns in the Parquet file:")
for column_name in column_names:
    print(column_name)
