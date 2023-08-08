from pyspark.sql.functions import udf

def ascii_ignore(x):
    return x.encode('ascii', 'ignore').decode('ascii')

ascii_udf = udf(ascii_ignore)

for col in df.columns:
    df = df.withColumn(col, ascii_udf(col))
