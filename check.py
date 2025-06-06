import pandas as pd

# Load the CSV file
file_path = 'data/extracted_data/olist_orders_dataset.csv'
df = pd.read_csv(file_path)

# Check for null values
null_counts = df.isnull().sum()

# Print the null counts for each column
print("Null values count per column:")
print(null_counts)

# Optionally: print percentage of nulls per column
total_rows = len(df)
print("\nPercentage of nulls per column:")
print((null_counts / total_rows * 100).round(2))
