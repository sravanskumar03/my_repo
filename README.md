schema = StructType([
    StructField('Folder', StringType(), True),
    StructField('Size', LongType(), True)
])
df = spark.createDataFrame(folder_sizes, schema)

# show the result
df.show()
