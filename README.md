from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("CreateColumnStringFromTable").getOrCreate()

# Set the database and table names
database_name = "my_database"
table_name = "my_table"

# Run the DESCRIBE TABLE query
query = f"DESCRIBE TABLE {database_name}.{table_name}"
result = spark.sql(query)

# Get the column names and data types
columns = result.rdd.map(lambda row: (row["col_name"], row["data_type"])).collect()

# Create the column string
column_string = ", ".join([f"{name} as {name} {datatype}" for name, datatype in columns])
print(column_string)
