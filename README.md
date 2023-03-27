# Assume 'df' is the original DataFrame with 433 columns
# Assume 'duplicate_cols' is a list of columns used for identifying duplicates
# Assume 'duplicate_row' is a row representing one of the duplicate records

from pyspark.sql.functions import col

# Create a filter condition for the duplicate record
filter_condition = col("col1") == duplicate_row[0]  # assuming 'col1' is the first column
for i in range(1, len(duplicate_cols)):
    filter_condition &= col(duplicate_cols[i]) == duplicate_row[i]

# Filter the original DataFrame for the duplicate record
df_filtered = df.filter(filter_condition)

# Show the result
df_filtered.show()
