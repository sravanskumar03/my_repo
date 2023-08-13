from pyspark.sql.functions import when, col

# Create a list of columns to compare
to_compare = ['col1', 'col2', 'col3']

# Select the id column and use when to compare the columns
df_result = df1.select('id', *[when(col(c1) != col(c2), [c1, c2]).alias(c1) for c1, c2 in zip(to_compare, df2.columns)])

# For those with a mismatch, build an array of structs with 3 fields: (Actual_value, Expected_value, Field)
df_result = df_result.select('id', *[when(col(c).isNotNull(), col(c)).alias(c) for c in to_compare])
