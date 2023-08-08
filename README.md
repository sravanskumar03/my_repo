from pyspark.sql.functions import regexp_replace

for col in df.columns:
    df = df.withColumn(col, regexp_replace(df[col], '[^\x00-\x7F]+', ''))
