from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType

# Initialize Spark session
spark = SparkSession.builder.appName("SearchTeaInXML").getOrCreate()

# Specify the S3 path containing XML files
s3_path = "s3://your_bucket/your_path/*.xml"

# Read XML files into a DataFrame
df = spark.read.format("xml").option("rowTag", "root").load(s3_path)

# Flatten the XML structure and convert to a single column of StringType
flat_df = df.selectExpr("explode_outer(to_json(struct(*))) as xml_content").select("xml_content").withColumn("xml_content", col("xml_content").cast(StringType()))

# Filter rows containing the word 'tea'
result_df = flat_df.filter(col("xml_content").contains("tea"))

# Show the results
result_df.show(truncate=False)
