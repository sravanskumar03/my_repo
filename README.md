Certainly! You can achieve this by iterating over the list of table names, reading each table, finding the maximum `import_dt` value, and counting the records with that maximum value. Here's a sample PySpark function to accomplish this:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max

def max_import_dt_per_table(table_names):
    spark = SparkSession.builder.appName("MaxImportDtPerTable").getOrCreate()
    result_data = []

    for table_name in table_names:
        try:
            # Read the table
            df = spark.read.table(table_name)

            # Check if import_dt column exists in the DataFrame
            if 'import_dt' in df.columns:
                # Find the max import_dt and count of records with that value
                max_import_dt_row = df.groupBy().agg(max("import_dt").alias("max_import_dt"), col("import_dt").isNotNull().alias("count_max_import_dt"))
                max_import_dt_value = max_import_dt_row.select("max_import_dt").collect()[0][0]
                count_max_import_dt = max_import_dt_row.select("count_max_import_dt").collect()[0][0]

                # Append result to the result_data list
                result_data.append((table_name, max_import_dt_value, count_max_import_dt))
            else:
                # If import_dt column doesn't exist, append None values
                result_data.append((table_name, None, None))
        except Exception as e:
            # Handle any potential exceptions during table reading
            result_data.append((table_name, None, None))
            print(f"Error processing table {table_name}: {str(e)}")

    # Create a DataFrame from the result_data list
    result_df = spark.createDataFrame(result_data, ["table_name", "max_import_dt", "count_max_import_dt"])
    
    return result_df

# Example usage
table_names = ["table1", "table2", "table3"]  # Replace with your actual table names
result_df = max_import_dt_per_table(table_names)
result_df.show()
```

This function takes a list of table names, iterates through each table, and creates a DataFrame containing the table name, maximum `import_dt` value, and count of records with that maximum value. Please replace the `table_names` list with your actual table names.
