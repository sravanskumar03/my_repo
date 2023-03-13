from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Create a SparkSession
spark = SparkSession.builder.appName("PythonListToDataFrame").getOrCreate()

# Define the list
My_list = ['2020-01-01', '2020-01-02', '2020-01-03']

# Create a RDD from the list
rdd = spark.sparkContext.parallelize(My_list)

# Convert the RDD to a DataFrame
df = rdd.map(lambda x: (x, )).toDF(['date'])

# Show the DataFrame
df.show()
