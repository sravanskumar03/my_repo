It seems there's an issue with the column name `import_dt` in the error message. It looks like there might be a space or typo in the column name. I'll adjust the code to handle potential column name variations. Try the updated code below:

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
            import_dt_column = next((col_name for col_name in df.columns if col_name.lower() == 'import_dt'), None)

            if import_dt_column:
                # Find the max import_dt and count of records with that value
                max_import_dt_row = df.groupBy().agg(max(import_dt_column).alias("max_import_dt"), col(import_dt_column).isNotNull().alias("count_max_import_dt"))
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
table_names = ["glu_bi_preissu.webpd.t_cobusinessinfo"]  # Replace with your actual table names
result_df = max_import_dt_per_table(table_names)
result_df.show()
```

This code attempts to find the correct `import_dt` column case-insensitively and should handle variations in column names more gracefully. Adjust the `table_names` list with your actual table names. If the issue persists, please provide more details about the error for further assistance.
