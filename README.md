from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("CustomDelimitedTextFile").getOrCreate()

# Read the text file and create a DataFrame
df = spark.read.format("text").option("delimiter", "<custom_delimiter>").option("header", "true").option("inferSchema", "true").load("<path_to_text_file>")

# Show the DataFrame
df.show()
