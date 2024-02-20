from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create a Spark session
spark = SparkSession.builder.appName("ReadExcel").getOrCreate()

# Specify the path to your Excel file in Databricks File System (DBFS)
excel_file_path = "/dbfs/path/to/your/file.xlsx"

# Define the schema of your Excel file (modify as per your actual schema)
custom_schema = StructType([
    StructField("column1", StringType(), True),
    StructField("column2", IntegerType(), True),
    # Add more fields as needed
])

# Read Excel file as binary data
binary_data = spark.read.format("binaryFile").load(excel_file_path).select("content")

# Convert binary data to string
excel_content = binary_data.selectExpr("cast(content as string) as excel_content").collect()[0]["excel_content"]

# Split lines
lines = excel_content.split("\n")

# Parse lines and create a DataFrame
data = [tuple(line.split("\t")) for line in lines]
df = spark.createDataFrame(data, schema=custom_schema)

# Show the DataFrame
df.show()
