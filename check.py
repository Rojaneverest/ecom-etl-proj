import os
import pandas as pd

# Define the directory containing CSV files
csv_dir = 'data/extracted_data'

# List all CSV files in the directory
csv_files = [f for f in os.listdir(csv_dir) if f.endswith('.csv')]

# Loop through each file and print columns and one row
for file in csv_files:
    file_path = os.path.join(csv_dir, file)
    try:
        df = pd.read_csv(file_path)
        print(f"\n--- {file} ---")
        print("Columns:", list(df.columns))
        print("First Row:\n", df.iloc[0] if not df.empty else "Empty CSV")
    except Exception as e:
        print(f"\n--- {file} ---")
        print("Error reading file:", e)
