# Import necessary libraries
import pandas as pd
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("ReadExcel").getOrCreate()

# Specify the path to your Excel file in Databricks File System (DBFS)
excel_file_path = "/dbfs/path/to/your/file.xlsx"

# Read Excel file using pandas
pandas_df = pd.read_excel(excel_file_path)

# Convert pandas DataFrame to PySpark DataFrame
spark_df = spark.createDataFrame(pandas_df)

# Show the DataFrame
spark_df.show()
