import pandas as pd

df = pd.read_csv('wfp_food_prices_ken.csv')

null_counts = df.isnull().sum()
print(null_counts)

total_nulls = df.isnull().sum().sum()
print(f"Total null values in the dataset: {total_nulls}")

# all rows that contain at least one null (very useful)
rows_with_nulls = df[df.isnull().any(axis=1)]
print(rows_with_nulls)

# 4. Optional: Save only the rows with nulls to a new CSV file
rows_with_nulls.to_csv('rows_with_nulls.csv', index=False)
print("Rows with nulls saved to 'rows_with_nulls.csv'")