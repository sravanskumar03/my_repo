from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Create a SparkSession
spark = SparkSession.builder.appName("PythonListToDataFrame").getOrCreate()

# Define the list
data = [('John', 'Doe', 25), ('Jane', 'Doe', 30), ('Bob', 'Smith', 35)]

# Define the schema of the DataFrame
schema = StructType([StructField("first_name", StringType(), True),
                     StructField("last_name", StringType(), True),
                     StructField("age", StringType(), True)])

# Create a DataFrame from the list and schema
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df.show()
