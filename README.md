from pyspark.sql.functions import split, concat_ws

def reverse_name(name):
    return concat_ws(", ", split(name, " ")[1], split(name, " ")[0])

df = spark.createDataFrame([(1, "angel de Maria"), (2, "lionel messi")], ["id", "name"])

df = df.withColumn("name", reverse_name(df.name))

df.show()
