from pyspark.sql.functions import expr, sequence, to_date

df = spark.range(0, (date.today() - date(2021, 1, 1)).days + 1, 1)
df = df.withColumn('date', expr("date_add('2021-01-01', id)"))

df.show()
