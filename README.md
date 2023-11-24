Certainly! You can use the following PySpark code to achieve this:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def find_duplicate_import_dates(input_table):
    # Create a Spark session
    spark = SparkSession.builder.appName("DuplicateImportDates").getOrCreate()

    # Read the parquet file into a DataFrame
    input_df = spark.read.parquet(input_table)

    # Group by import date and count the number of occurrences
    import_dates_count = input_df.groupBy("import_date").count()

    # Filter only the rows where count is greater than 1 (indicating duplicates)
    duplicates_df = import_dates_count.filter(col("count") > 1)

    return duplicates_df

# Example usage:
table_path = "path/to/your/parquet/table"
result_df = find_duplicate_import_dates(table_path)
result_df.show()
```

Replace `"path/to/your/parquet/table"` with the actual path to your parquet table. This code reads the parquet file, groups by import date, counts the occurrences, and filters out the rows where the count is greater than 1, giving you a DataFrame with import dates having row-level duplicates along with the count of duplicates for each import date.
