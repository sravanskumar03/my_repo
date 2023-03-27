from pyspark.sql import Row

# Assume 'df' is a DataFrame that we want to convert to row objects
rows = df.collect()

# Convert each row to a row object
row_objects = [Row(*r) for r in rows]

# Now 'row_objects' is a list of row objects, where each row object represents a row in the DataFrame 'df'
