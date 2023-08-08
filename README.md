import pyspark.sql.functions as F

df = df.select([F.regexp_replace(F.col(c), '[^a-zA-Z0-9]', '').alias(c) for c in df.columns])
