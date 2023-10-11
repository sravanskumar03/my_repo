from pyspark.sql.functions import input_file_name

df = spark.read.json("s3://your-bucket-name/your-folder-name/")
df.withColumn("filename", input_file_name()).filter(col("filename").contains("00567")).select("filename").show()
