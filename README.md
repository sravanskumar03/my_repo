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
.

Subject: Request for RAM Upgrade on Virtual Machine

Description:
I am submitting a request to the IT team for an increase in RAM memory allocation on my virtual machine. The business reason behind this request is crucial for our effective collaboration with business partners. We are currently engaged in data validation tasks involving both on-premises and cloud-based resources. To meet business partners' expectations, we need to document detailed comparisons in an Excel sheet for their ease of use. The additional RAM will enhance the virtual machine's performance, ensuring a smoother and more efficient execution of these data validation tasks. Your prompt attention to this matter is highly appreciated.
