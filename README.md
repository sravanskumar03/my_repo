from pyspark.sql.functions import size

df = spark.createDataFrame([(1, ["foo", "bar"]), (2, ["baz"]), (3, [])], ["id", "value"])
df.show()

+---+--------+
| id|   value|
+---+--------+
|  1|[foo,bar]|
|  2|    [baz]|
|  3|      []|
+---+--------+

df = df.withColumn("size", size(df.value))
df.show()

+---+--------+----+
| id|   value|size|
+---+--------+----+
|  1|[foo,bar]|   2|
|  2|    [baz]|   1|
|  3|      []|   0|
+---+--------+----+
