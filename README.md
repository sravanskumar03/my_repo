from pyspark.sql.functions import col
df = df.select([col(c).cast("string") for c in df.columns])
