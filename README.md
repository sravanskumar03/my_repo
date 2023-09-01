from pyspark.sql.functions import trim

df = spark.read.format("csv").option("header", "true").load("path/to/csv/file")
df = df.select([trim(column).alias(column) for column in df.columns])
