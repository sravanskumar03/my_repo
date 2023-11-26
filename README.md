Certainly! You can use the following PySpark function to retrieve all column names from a given table and return them as a Python list:

```python
from pyspark.sql import SparkSession

def get_table_columns(table_name):
    # Create a Spark session
    spark = SparkSession.builder.appName("GetTableColumns").getOrCreate()

    # Read a small sample of the table to get the schema
    sample_df = spark.read.table(table_name).limit(1)

    # Extract column names from the schema
    columns_list = sample_df.columns

    return columns_list

# Example usage:
table_name = "your_database.your_table"
columns_result = get_table_columns(table_name)
print(columns_result)
```

Replace `"your_database.your_table"` with the actual table name in the format `database_name.table_name`. This function reads a small sample of the table to get the schema, and then extracts the column names from the schema, returning them as a Python list.
