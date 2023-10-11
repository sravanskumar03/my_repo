from pyspark.sql.functions import input_file_name

df = spark.read.json("s3://your-bucket-name/your-folder-name/")
df.withColumn("filename", input_file_name()).filter(col("filename").contains("00567")).select("filename").show()

from pyspark.sql.functions import col

df = df.withColumn("eventbody", col("eventbody").cast("string"))
df.withColumn("filename", input_file_name()).filter(col("eventbody").contains("00567")).select("filename").show()
