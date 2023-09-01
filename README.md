from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("CustomDelimitedTextFile").getOrCreate()

# Define the schema of the DataFrame
schema = "column1 STRING, column2 INT, column3 DOUBLE"

# Read the text file and create a DataFrame
df = spark.read.format("csv").option("delimiter", "<custom_delimiter>").schema(schema).load("<path_to_text_file>")

# Show the DataFrame
df.show()
