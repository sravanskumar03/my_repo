# Define the two lists
mondays = ["2021-01-04", "2021-01-11", "2021-01-18", "2021-01-25", ...]
month_ends = ["2021-01-31", "2021-02-28", "2021-03-31", "2021-04-30", ...]

# Merge the two lists using the + operator
merged_list = mondays + month_ends

# Sort the merged list in ascending order
sorted_list = sorted(merged_list)

# Create a PySpark DataFrame with two columns Freq and import date
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import lit
df_schema = StructType([StructField("Freq", StringType(), True), StructField("import date", StringType(), True)])
df_data = [(date, "M") for date in month_ends] + [(date, "W") for date in mondays]
df = spark.createDataFrame(df_data, df_schema)

# Show the resulting DataFrame
df.show()
