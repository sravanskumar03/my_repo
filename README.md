Certainly! Based on your sample scenario, here's a PySpark function that takes an S3 path as input, checks for row-level duplicates in each parquet file stored in S3, and returns a DataFrame containing import_dt partition files with row-level duplicates:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def find_duplicates_in_partitions(s3_path):
    # Create a Spark session
    spark = SparkSession.builder.appName("S3PartitionedDuplicates").getOrCreate()

    # Read the parquet files from the S3 path
    input_df = spark.read.parquet(s3_path)

    # Identify row-level duplicates within each import_dt partition
    partitioned_duplicates_df = (
        input_df
        .groupBy("import_dt", input_df.columns)
        .count()
        .filter(col("count") > 1)
        .groupBy("import_dt")
        .agg({"count": "count"})
        .withColumnRenamed("count", "no_of_row_level_duplicates")
    )

    return partitioned_duplicates_df

# Example usage:
s3_table_path = "s3://your-bucket/your-path"
result_df = find_duplicates_in_partitions(s3_table_path)
result_df.show()
```

Replace `"s3://your-bucket/your-path"` with the actual S3 path to your parquet table. This code efficiently identifies row-level duplicates within each import_dt partition, providing a DataFrame containing import_dt and the count of row-level duplicates for each partition file.
