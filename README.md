from pyspark.sql.functions import col, array

# Select the columns that contain policy details
policy_cols = [col("col1"), col("col2"), ..., col("col19")]

# Select the columns that contain amount details
amount_cols = [col("col20"), col("col21"), ..., col("col36")]

# Create an array of the policy columns
policy_array = array(*policy_cols)

# Group the data by the policy columns and get the sum of the amount columns
df_grouped = df.groupBy(policy_array).agg(*[sum(c).alias(c.name+"_sum") for c in amount_cols])

# Filter out the groups where all the amount columns are the same
df_filtered = df_grouped.filter(*[(col(c.name+"_sum") != c * len(amount_cols)) for c in amount_cols])

# Show the result
df_filtered.show()
