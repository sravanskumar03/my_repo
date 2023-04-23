Here's an example of how you could do this using PySpark:

```python
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import multiprocessing as mp

def your_function(s3_path):
    # Your function that returns a JSON object containing s3path name, size of the s3 path and file count in the s3path
    pass

def process_s3_path(s3_path):
    result = your_function(s3_path)
    return (result['s3path'], result['size'], result['file_count'])

if __name__ == '__main__':
    spark = SparkSession.builder.appName("S3PathProcessing").getOrCreate()

    schema = StructType([
        StructField("s3path", StringType(), True),
        StructField("size", IntegerType(), True),
        StructField("file_count", IntegerType(), True)
    ])

    s3_paths = ["s3://path1", "s3://path2", "s3://path3"]
    pool = mp.Pool(processes=4)
    results = pool.map(process_s3_path, s3_paths)

    df = spark.createDataFrame(data=results, schema=schema)
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    df = df.withColumn("date", yesterday)

    df.show()
```

This code creates a Spark DataFrame with the schema specified and uses the `multiprocessing` module to call your function with multiple `s3_path` parameters. The results are then stored in the DataFrame and a new column with yesterday's date is added. You can adjust the number of processes used by the `Pool` to match your needs.

Is this what you were looking for?
