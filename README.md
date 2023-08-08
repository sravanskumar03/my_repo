from pyspark.sql.functions import udf

def ascii_ignore(x):
    return x.encode('ascii', 'ignore').decode('ascii')

ascii_udf = udf(ascii_ignore)

df = df.select([ascii_udf(column).alias(column) for column in df.columns])
