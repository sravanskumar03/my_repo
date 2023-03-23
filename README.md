from datetime import datetime
from pyspark.sql.functions import lit

# get the current date and time
now = datetime.now()

# format the date and time as a string
formatted_date_time = now.strftime("%Y-%m-%d %H:%M:%S")

# create a PySpark dataframe with the formatted date and time
df = spark.createDataFrame([(formatted_date_time,)], ["current_time"])

# print the current time and date
df.select(lit("Current Time: ").alias(""), "current_time").show(truncate=False)
