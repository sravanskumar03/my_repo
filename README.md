from pyspark.sql.functions import udf

def ascii_ignore(x):
    return x.encode('ascii', 'ignore').decode('ascii')

ascii_udf = udf(ascii_ignore)

df = df.withColumn("new_column", ascii_udf("old_column"))
